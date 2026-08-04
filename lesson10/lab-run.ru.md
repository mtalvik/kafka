# Урок 10 — реальный прогон лабы: команды, объяснения, схемы

Это журнал того, что мы **реально** сделали, по порядку. `LAB.md` — план;
здесь фактический ход со всеми отклонениями (ресайз инстанса, `terraform
import`, флаги console-consumer 4.x, фикс Ex8). Каждый блок: команда →
что она делает → что увидели.

Все клиентские конфиги на брокере: `~/kafka/clients/{admin,alice,bob}.properties`.
Пароль alice: `alice-pass`.

```
Общий маршрут:
  Шаг 1  настройки брокера (RF=1)          ──┐ фундамент
  Шаг 2  ресайз t3.micro → t3.small        ──┘ (иначе Gradle убивает micro)
  Шаг 3  Terraform: топики + ACL
  Шаг 4  client.properties
  Шаг 5  Ex3  конфиг идемпотентности
  Шаг 6  Ex5  атомарная запись + маркеры
  Шаг 7  Ex6  read-process-write (EOS)
  Шаг 8  Ex7  isolation.level + LSO
  Шаг 9  Ex8  fencing (зомби)
  Шаг 10 остановить инстансы
```

---

## Шаг 1 — настройки брокера для транзакций

Транзакционному producer'у нужен внутренний топик `__transaction_state`.
Его дефолты (RF=3, min.isr=2) невыполнимы на одной ноде → `initTransactions()`
завис бы. Нужны три строки `=1`.

```bash
# зайти в брокер
cd ~/otus-kafka
./aws-lab.sh start kafka
./aws-lab.sh ssh kafka

# проверить настройки
grep -E 'transaction.state.log|offsets.topic.replication' ~/kafka/config/server.properties
```

Увидели — всё уже стоит:
```
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

Проверили, что топик ещё не создан со старым RF:
```bash
~/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --command-config ~/kafka/clients/admin.properties \
  --describe --topic __transaction_state 2>&1 | head -5
# → "Topic '__transaction_state' does not exist"  ← хорошо, создастся с RF=1
```

**Почему это критично:** без `=1` первый же `initTransactions()` виснет на
60 секунд и падает по таймауту. Это единственный жёсткий блокер лабы.

```
  __transaction_state с дефолтом RF=3 на 1 брокере:
     initTransactions() → coordinator ждёт 3 реплики → их нет → ⏳ hang → timeout

  с RF=1:
     initTransactions() → coordinator: 1 реплика есть → ✓ поехали
```

---

## Шаг 2 — ресайз t3.micro → t3.small

Первый Gradle-билд (скачивание + компиляция) на `t3.micro` (1 GB RAM, мало
CPU-кредитов) выжирает кредиты, инстанс перестаёт отвечать — даже SSH не
пускает. Лечение: постоянный ресайз до `t3.small` (2 GB RAM).

Ресайз возможен **только на остановленном** инстансе, делается **из
CloudShell**:

```bash
# CloudShell
cd ~/otus-kafka
./aws-lab.sh stop kafka

# дождаться ПОЛНОГО stopped (не stopping!)
aws ec2 describe-instances --instance-ids i-05b43d908b4fa70ab \
  --query 'Reservations[0].Instances[0].State.Name' --output text
# повторять, пока не выведет: stopped

# сменить тип
aws ec2 modify-instance-attribute \
  --instance-id i-05b43d908b4fa70ab \
  --instance-type t3.small
# (молча = успех)

# поднять обратно и зайти
./aws-lab.sh start kafka
./aws-lab.sh ssh kafka
```

Проверили в брокере:
```bash
free -h    # Mem: 1.9Gi  ← стало small (было 0.9)
sudo systemctl is-active kafka   # active
```

**Важно:** ресайз не трогает EBS-диск — данные Kafka, топики, Terraform-state
целы. `modify-instance-attribute` требует именно `stopped`; на `stopping`
даёт `IncorrectInstanceState`.

**Дешевле, но медленнее:** можно было остаться на micro и приручить Gradle
(`-Xmx512m`, `--max-workers=1`). Мы выбрали small — надёжнее.

```
  t3.micro:  1 GB RAM, копит мало CPU-кредитов
             первый Gradle-билд → кредиты в ноль → инстанс глохнет ✗
  t3.small:  2 GB RAM, копит вдвое больше кредитов
             тот же билд → переживает спокойно ✓
```

---

## Шаг 3 — Terraform: топики и ACL

Инфраструктура для транзакций живёт в `lesson7/gitops` (кумулятивный
GitOps). Terraform гоняется **на брокере** (CloudShell не достаёт до
приватного IP).

```bash
cd ~/kafka-repo && git pull
cd lesson7/gitops

terraform init -upgrade      # mongey/kafka v0.13.1
terraform plan               # → Plan: 17 to add, 0 to change, 0 to destroy
```

В плане: 4 топика (`tx-a`, `tx-b`, `tx-inbound`, `tx-outbound`), ACL alice
(`TransactionalID`/group/topic `tx-`) и bob (`tx-` read), плюс `producer-lab`
из lesson8 (наконец заезжает под GitOps).

```bash
terraform apply              # yes
```

**Затык:** упало на `producer-lab` — «Topic with this name already exists»
(создавался руками в lesson8). Лечение — импорт существующего в state:

```bash
terraform import kafka_topic.producer_lab producer-lab   # Import successful
terraform plan               # → 4 to add, 1 to change, 0 to destroy
```

`1 to change` — проверили diff: меняется только `config` (добавляются
`cleanup.policy`, `retention.ms`), **`partitions` не трогается** → безопасно.

```bash
terraform apply              # yes → Apply complete! 4 added, 1 changed, 0 destroyed
```

Проверили топики на брокере:
```bash
~/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --command-config ~/kafka/clients/admin.properties --list | grep '^tx-'
# tx-a  tx-b  tx-inbound  tx-outbound
```

**Что дал каждый ACL** (без них транзакции падают):
```
  alice  Write+Describe на TransactionalID tx-   → initTransactions() разрешён
  alice  Read на Group tx-                        → sendOffsetsToTransaction (Ex6)
  alice  Write+Read+Describe на Topic tx-         → produce/consume в tx- топики
  bob    Read+Describe на Topic tx-               → проверка результата
```

---

## Шаг 4 — client.properties

```bash
cd ~/kafka-repo/lesson10/transactions-java
cp client.properties.example client.properties

# пароль alice
grep -i password ~/kafka/clients/alice.properties   # → password="alice-pass"

# вписать
sed -i 's/<PLACEHOLDER>/alice-pass/' client.properties
```

Итог `client.properties`:
```
bootstrap.servers=localhost:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=...PlainLoginModule required username="alice" password="alice-pass";
```

`client.properties` в `.gitignore` — пароль не уходит в git.

---

## Паттерн запуска Gradle

Все примеры гоняем так:
```bash
gradle exN --no-daemon --max-workers=1
```
- `--no-daemon` — не держать демон между запусками.
- `--max-workers=1` — не распараллеливать агрессивно, чтобы не жечь
  CPU-кредиты даже на small.
- Для чистого вывода без логов: `... 2>/dev/null | grep -E 'нужное'`.

---

## Шаг 5 — Ex3: конфиг идемпотентности

```bash
gradle ex3 --no-daemon --max-workers=1
```
```
ConfigException as expected: To use the idempotent producer,
max.in.flight.requests.per.connection must be set to at most 5. Current value is 6.
```

**Что доказали:** невалидный конфиг идемпотентности (`max.in.flight=6`)
отвергается **при создании** продюсера, а не позже при `send()`. Правило §2
лекции. Заодно подтвердили: клиент собрался, SASL-коннект работает.

---

## Шаг 6 — Ex5: атомарная запись + маркеры коммита

```bash
gradle ex5 --no-daemon --max-workers=1
```
В логе ключевое:
```
Invoking InitProducerId for the first time         ← initTransactions() прошёл
Discovered transaction coordinator 172.31.29.117   ← координатор найден
ProducerId set to 0 with epoch 0                    ← PID=0, epoch=0 (первый!)
committed transaction 0..3
```

Проверили запись напрямую:
```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --command-config ~/kafka/clients/admin.properties \
  --topic tx-a --from-beginning --timeout-ms 5000 \
  --formatter-property print.offset=true 2>/dev/null
```
```
Offset:0   a-0
Offset:2   a-1      ← offset 1 пропущен
Offset:4   a-2      ← offset 3 пропущен
Offset:6   a-3      ← offset 5 пропущен
```

**Схема — почему дырки в offset (§5 вживую):**
```
  партиция tx-a:
  ┌─────┬────────┬─────┬────────┬─────┬────────┬─────┐
  │ a-0 │ COMMIT │ a-1 │ COMMIT │ a-2 │ COMMIT │ a-3 │ ...
  │  0  │   1    │  2  │   3    │  4  │   5    │  6  │
  └─────┴────────┴─────┴────────┴─────┴────────┴─────┘
           маркер          маркер        маркер

  4 транзакции = 4 записи + 4 маркера коммита.
  Маркер занимает offset в логе, но консьюмеру НЕ отдаётся.
  Поэтому read_committed видит 0,2,4,6 — с дырками.
```
Маркер коммита — то, что координатор пишет в партицию на фазе D. На
слайдах это теория, здесь — на реальном брокере.

---

## Шаг 7 — Ex6: read-process-write (exactly once)

Это «банан»: прочитал заказ → записал результат → закоммитил offset одной
транзакцией.

```bash
gradle ex6 --no-daemon --max-workers=1 2>/dev/null | grep 'transaction committed'
# → transaction committed for 2 record(s)
```

Проверили `tx-outbound`:
```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --command-config ~/kafka/clients/admin.properties \
  --topic tx-outbound --from-beginning --timeout-ms 5000 \
  --formatter-property print.key=true --formatter-property print.offset=true 2>/dev/null
```

**Тест на exactly-once — запустили ДВАЖДЫ:**
```
после 1-го прогона:          после 2-го прогона:
  Offset:0 k1 first-processed   Offset:0 k1 first-processed
  Offset:1 k2 second-processed  Offset:1 k2 second-processed
                                Offset:3 k1 first-processed   ← +2 новых
                                Offset:4 k2 second-processed
```

**Схема — почему +2, а не удвоение:**
```
  Прогон 1:  seed кладёт 2 → трансформер обрабатывает → outbound: 2
             offset входа закоммичен ТРАНЗАКЦИОННО ✓
  Прогон 2:  seed кладёт 2 НОВЫХ → трансформер видит только их
             (старые НЕ перечитаны — их offset уже закоммичен) → outbound: 4

  Если бы offset коммитился НЕ транзакционно:
     прогон 2 перечитал бы всё с нуля → 2→6→12 взрыв дублей ✗
  А растёт линейно +2 → прогресс входа держится намертво → EXACTLY ONCE ✓
```
(Одинаковые k1/first-processed — это новые сообщения от seed'а, не
переобработка старых.)

---

## Шаг 8 — Ex7: isolation.level + LSO

Два консьюмера читают один топик: `read_committed` и `read_uncommitted`.

```bash
gradle ex7 --no-daemon --max-workers=1 2>/dev/null | grep -E 'read_committed|read_uncommitted|>>>'
```

Результат по существу:
```
  read_uncommitted : ... 4 ...    ← видит 4 (из абортнутой транзакции)
  read_committed   : 0,1,2,3,END  ← 4 НЕТ
```

**Схема — что видит кто (§7):**
```
  producer шлёт:  "0"(вне tx) → begin → "1"(tx) → "2"(вне tx) → "3"(tx) → COMMIT
                  → begin → "4"(tx) → ABORT → "END"(вне tx)

  read_uncommitted:  0 1 2 3 4 END      ← всё подряд, включая абортнутое 4
  read_committed:    0 . . . . .        ← сначала только 0
                     ↓ после COMMIT
                     0 1 2 3 . END      ← 1,2,3 разом; 2 вышло из-за LSO;
                                          4 (ABORT) — НИКОГДА
```
`read_committed` не видит абортнутое `4` — ядро §7. ✅
(Вывод «замусорен» a-0..a-3 из Ex5 и порядком строк — это косметика
верификатора, суть верна.)

---

## Шаг 9 — Ex8: fencing (защита от зомби)

Два продюсера с одним `transactional.id=tx-ex8`. Второй поднимает эпоху,
первый становится зомби.

**Затык:** старый код ловил только `ProducerFencedException`, а в Kafka 4.x
наружу летит `InvalidProducerEpochException` → падал с exit 1. Пофиксили
`catch` (ловим оба + `KafkaException`).

```bash
# Mac: запушить фикс
cd ~/REPOS/teaching/kafka
git add lesson10/ && git commit -m "lesson10: Ex8 catch InvalidProducerEpochException" && git push

# брокер: подтянуть и прогнать
cd ~/kafka-repo && git pull
cd lesson10/transactions-java
gradle ex8 --no-daemon --max-workers=1 2>/dev/null | grep -E 'committed|FENCED'
```
```
producer1 committed
producer2 committed (epoch bumped, producer1 now fenced)
FENCED as expected (InvalidProducerEpochException): producer1 holds a stale epoch, must be recreated
```

**Схема — fencing по эпохе (§6):**
```
  producer1  initTransactions(tx-ex8)  → coordinator: эпоха = 0
  producer1  commit "from-1"           → ✓ (эпоха 0 актуальна)

  producer2  initTransactions(tx-ex8)  → coordinator: эпоха = 1 (поднята!)
  producer2  commit "from-2"           → ✓ (эпоха 1 актуальна)

  producer1  commit "from-1-again"     → coordinator: у тебя эпоха 0 < 1
                                         → InvalidProducerEpochException ✗
                                         "old epoch" — зомби заборонён
```
Только один продюсер на `transactional.id` жив. Деталь для лекции: в 4.x
зомби ловит `InvalidProducerEpochException` (в main), а
`ProducerFencedException` уходит в network-thread — оба про одно.

---

## Шаг 10 — остановить инстансы

```bash
# CloudShell
cd ~/otus-kafka
./aws-lab.sh stop         # гасит kafka + elastic + clients
```
Между сессиями инстансы должны быть stopped (кредиты/деньги). Данные и
Terraform-state сохраняются. Тип `t3.small` держится до следующего полного
пересоздания с нуля.

---

## Итог

| Ex | Что доказали | Как проверили |
|---|---|---|
| Ex3 | невалидный конфиг идемпотентности отвергается при создании | `ConfigException` |
| Ex5 | атомарная запись в 2 топика + маркеры коммита | offset 0,2,4,6 в tx-a |
| Ex6 | read-process-write ровно один раз | +2 за прогон, без переобработки |
| Ex7 | read_committed не видит абортнутое, LSO держит | 4 виден только у read_uncommitted |
| Ex8 | fencing по эпохе, зомби заборонён | InvalidProducerEpochException |

**Полная цепочка exactly-once на реальном брокере:**
```
  идемпотентность (PID+Seq)  +  транзакции (begin…commit + offset)  +  fencing (эпоха)
  ────────────────────────────────────────────────────────────────────────────────────
              = сообщение обработано ровно один раз (Kafka → Kafka)
```

### Ключевые команды-шпаргалка

```bash
# зайти / выйти
./aws-lab.sh start kafka && ./aws-lab.sh ssh kafka      # CloudShell
./aws-lab.sh stop                                        # CloudShell

# запуск примера
cd ~/kafka-repo/lesson10/transactions-java
gradle exN --no-daemon --max-workers=1 2>/dev/null | grep -E 'нужное'

# посмотреть топик (флаги Kafka 4.x!)
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --command-config ~/kafka/clients/admin.properties \
  --topic ИМЯ --from-beginning --timeout-ms 5000 \
  --formatter-property print.key=true --formatter-property print.offset=true 2>/dev/null

# список tx- топиков
~/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --command-config ~/kafka/clients/admin.properties --list | grep '^tx-'

# Terraform (на брокере, в lesson7/gitops)
terraform plan
terraform apply
```
