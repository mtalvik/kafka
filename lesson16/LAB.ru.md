# Лаб 16 — Kafka Streams: транзакции, Processor API, Interactive Queries

Все команды выполняются на EC2 `kafka`, если не сказано иначе. Однонодовый
брокер работает по SASL/PLAIN; все клиенты аутентифицируются как `bob` — тот же
принципал, что в уроках 14 и 15. Отдельный Streams-процесс разворачивать не
надо: приложение живёт внутри `gradle exN`.

Отличие от урока 15: в упражнении 5 работают **два** инстанса приложения
одновременно, и это самое требовательное к памяти место всего курса. Читай
раздел 6 целиком до того, как запускать.

## 0. Подготовка

```bash
ssh-add ~/.ssh/id_ed25519_mtalvik          # на Mac, до scp/ssh
# на EC2 kafka (~/kafka-repo — клон репозитория; ~/kafka — установка брокера):
git -C ~/kafka-repo pull
cd ~/kafka-repo/lesson16/streams-papi-java
cp client.properties.example client.properties
# подставить пароль bob:
sed -i "s|REPLACE_ME|$(grep user_bob ~/kafka/config/kafka_server_jaas.conf | cut -d'"' -f2)|" client.properties
```

Консольным утилитам нужны те же учётные данные:

```bash
cat > /tmp/admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="bob" password="$(grep user_bob ~/kafka/config/kafka_server_jaas.conf | cut -d'"' -f2)";
EOF
```

Gradle установлен в системе (8.8, JDK 17, с урока 8), wrapper'а в репозитории
нет. t3.small тесноват по памяти, поэтому ограничиваем JVM перед сборкой:

```bash
export GRADLE_OPTS="-Xmx256m"
gradle build --no-daemon
```

> **Про логи.** Kafka на старте печатает полный конфиг каждого клиента на
> уровне INFO — сотни строк, в которых тонет вывод самих упражнений.
> Поэтому в `build.gradle` уровень логгера понижен до `warn`.
>
> Если надо заглянуть внутрь (например, увидеть `transactional.id` в блоке
> `ProducerConfig values:`), верни INFO на один запуск:
> ```bash
> gradle ex1 --no-daemon -Dorg.slf4j.simpleLogger.defaultLogLevel=info
> ```

Переменные, используемые дальше во всех разделах:

```bash
BS=localhost:9092
CFG=/tmp/admin.properties
```

## 1. Топики и ACL

Управляются Terraform в `lesson7/gitops` — тот же GitOps-поток, что с урока 7.
Здесь это не опция: топологии работают под `bob`, у которого нет Create на
кластере, так что создать эти топики руками под этим принципалом не выйдет.

```bash
cd ~/kafka-repo/lesson7/gitops
terraform apply
cd ~/kafka-repo/lesson16/streams-papi-java
```

Семь топиков, все с `replication_factor = 1`:

| топик | партиций | для чего |
|---|---|---|
| `eos-input` / `eos-output` | 2 | Ex1, транзакции |
| `papi-input` / `papi-output` | 1 | Ex2, топология руками |
| `sensor-readings` / `sensor-alerts` | 2 / 1 | Ex3, Punctuator |
| `iq-events` | 3 | Ex5, Interactive Queries |

Ex4 переиспользует `purchases` из урока 15 — нового топика не заводим.

> Streams создаёт свои внутренние топики (changelog и repartition) на старте,
> с именами `lesson16-exN-...`. В Terraform их нет: их создаёт приложение, и
> именно поэтому у `bob` есть префиксная ACL на `lesson16-` с Create. Их
> replication factor берётся из `StreamsConfig.REPLICATION_FACTOR_CONFIG`,
> который `Utils.streamProps` фиксирует в 1. Если приложение виснет на старте
> с ошибкой создания топика — эта настройка потерялась.
>
> **Новое в этом уроке:** `bob` получил ACL на ресурс `TransactionalID` с
> префиксом `lesson16-` (Write + Describe) и `IdempotentWrite` на кластер. Без
> них Ex1 с включённым EOS падает с `TransactionalIdAuthorizationException`.
> Проверить:
> ```bash
> ~/kafka/bin/kafka-acls.sh --bootstrap-server $BS --command-config $CFG \
>   --list --principal User:bob --transactional-id lesson16-
> ```

Настройки брокера, от которых зависят транзакции (не Terraform — статические
свойства брокера, менялись в уроке 10):

```bash
grep -E 'transaction.state.log' ~/kafka/config/server.properties
```

Оба должны быть `1`. Если нет — правим и перезапускаем брокер до Ex1, иначе
`initTransactions()` зависает и падает по таймауту.

## 2. Ex1 — транзакции (`exactly_once_v2`)

Топология простая: читает `eos-input`, преобразует значение, пишет
`eos-output`. Интерес не в топологии, а в том, что меняется от одного
параметра.

### Шаг 2.1 — запуск без транзакций

Терминал A:

```bash
gradle ex1 --no-daemon
```

Терминал B — отправить пару записей и прочитать результат:

```bash
printf '%s\n' "k1:10" "k2:20" | ~/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS --command-config $CFG --topic eos-input \
  --property parse.key=true --property key.separator=:

~/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --command-config $CFG \
  --topic eos-output --from-beginning --max-messages 2
```

Записи появляются практически сразу. Оффсеты коммитятся раз в 30 секунд —
дефолт `commit.interval.ms` без EOS.

Останови приложение (`Ctrl-C`).

### Шаг 2.2 — тот же прогон с EOS

```bash
gradle ex1 --no-daemon -Peos=true
```

Свойство `-Peos=true` выставляет
`processing.guarantee = exactly_once_v2` и больше ничего.

Отправь ещё две записи тем же способом. Теперь посмотри, что появилось на
брокере:

```bash
~/kafka/bin/kafka-transactions.sh --bootstrap-server $BS \
  --command-config $CFG list
```

Появится transactional id, начинающийся с `lesson16-ex1-`. Это и есть тот id,
который Streams вывел из `application.id` — руками его никто не задавал, и
именно поэтому префиксная ACL работает.

Подробности по конкретному id:

```bash
~/kafka/bin/kafka-transactions.sh --bootstrap-server $BS --command-config $CFG \
  describe --transactional-id <id из вывода выше>
```

Смотри поля `State` и `TransactionStartTimeMs`. Состояние
`CompleteCommit`/`Ongoing` меняется в такт `commit.interval.ms`, который под
EOS равен 100 мс.

Заодно посмотри на `TransactionTimeoutMs` — там **10000**, то есть 10 секунд.
Слайды OTUS называют 60 секунд — это дефолт обычного producer, а Streams
ставит своё значение. Значит, брошенная транзакция держит
`read_committed`-читателей десять секунд, а не минуту.

`ProducerEpoch` — счётчик поколений. При каждом перезапуске он растёт, и по
нему брокер отсекает зомби — зависший старый экземпляр, который вдруг ожил и
пытается писать.

`TopicPartitions` пустое, пока транзакция закрыта. Если поймать её в
состоянии `Ongoing`, там будут перечислены `eos-output-*` и
`__consumer_offsets-*` — то самое «выход плюс оффсеты в одной транзакции» из
лекции, видное глазами.

### Шаг 2.3 — read_committed против read_uncommitted

Отличие видно на потребителе. Запусти два консьюмера параллельно:

```bash
# терминал C — обычный, видит всё
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --command-config $CFG \
  --topic eos-output --from-beginning

# терминал D — только закоммиченное
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --command-config $CFG \
  --topic eos-output --from-beginning \
  --isolation-level read_committed
```

На нормальном потоке разницы почти нет — транзакции коммитятся каждые 100 мс.
Разница проявляется, когда транзакция зависает: терминал D не уедет дальше
last stable offset, пока координатор её не отменит.

### Проверь себя

Прежде чем идти дальше, ответь:

1. Почему в выводе `kafka-transactions.sh list` появился id, которого нет ни в
   одном конфиге проекта?
2. Что сломается, если `application.id` сделать
   `"lesson16-ex1-" + UUID.randomUUID()`? На каком именно этапе?
3. У топологии две подтопологии. Сколько транзакций открывается на коммит — по
   одной на подтопологию или одна на весь поток?

Ответ на третий вопрос — в разделе «v1 vs v2» лекции: под v2 producer один на
`StreamThread`, а не на task.

## 3. Ex2 — топология руками (Processor API)

Здесь нет ни `StreamsBuilder`, ни `.stream()`, ни `.mapValues()` — только
`new Topology()` и явно названные узлы. Граф: source → upper → (sink и logger).

### Шаг 3.1 — сначала посмотри на граф

```bash
gradle ex2 --no-daemon
```

На старте приложение печатает `topology.describe()`. Примерно так:

```
Topologies:
   Sub-topology: 0
    Source: source (topics: [papi-input])
      --> upper
    Processor: upper (stores: [])
      --> logger, sink
      <-- source
    Processor: logger (stores: [])
      --> none
      <-- upper
    Sink: sink (topic: papi-output)
      <-- upper
```

Читай это внимательно: `-->` — потомки, `<--` — родители. Узел `upper`
разветвляется на два потомка, а `logger` — терминальный (`--> none`), потому
что ничего не форвардит. Одна подтопология — репартиционирования нет.

### Шаг 3.2 — прогнать запись

```bash
echo "hello papi" | ~/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS --command-config $CFG --topic papi-input

~/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --command-config $CFG \
  --topic papi-output --from-beginning --max-messages 1
```

Ожидаем: `HELLO PAPI` на `papi-output` и строка от `logger` в терминале A.
Оба потомка получают одну и ту же запись — это и есть разветвление DAG.

### Шаг 3.3 — адресный forward

В `Ex2PapiTopology` найди вызов `context.forward(record)` и замени его на
адресный вариант:

```java
context.forward(record, "sink");
```

Пересобери и повтори шаг 3.2. Запись доедет до `papi-output`, но строки от
`logger` больше не будет. В DSL так сделать нечем — вот ради чего сюда
спускаются. Верни как было перед следующим упражнением.

## 4. Ex3 — stateful-процессор и Punctuator

Сценарий: датчики шлют показания в `sensor-readings` (ключ — id датчика,
значение — число). Процессор копит последнее значение по каждому датчику в
хранилище, а раз в 10 секунд wall-clock пунктуатор обходит хранилище и
выбрасывает в `sensor-alerts` всё, что превышает порог. Запись — на каждом
событии, чтение — по таймеру.

### Шаг 4.1 — запуск

```bash
gradle ex3 --no-daemon
```

Сразу видно главное отличие от всех предыдущих уроков: приложение печатает
`punctuate` каждые 10 секунд, **хотя ни одной записи ещё не пришло**. Ни один
оператор DSL так себя не ведёт.

Строк будет две на каждый тик — у `sensor-readings` две партиции, значит две
задачи, а расписание живёт внутри задачи, а не приложения. Запомни это:
«раз в 10 секунд» всегда значит «раз в 10 секунд на каждую задачу».

### Шаг 4.2 — подать показания

```bash
printf '%s\n' "s1:20" "s2:95" "s1:30" | ~/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS --command-config $CFG --topic sensor-readings \
  --property parse.key=true --property key.separator=:
```

Порог — 80. Ждём ближайший тик пунктуатора и смотрим алерты:

```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --command-config $CFG \
  --topic sensor-alerts --from-beginning --property print.key=true
```

Ожидаем алерт только по `s2`. Обрати внимание: алерт пришёл **не в момент
записи**, а на следующем тике — до десяти секунд задержки. Это осознанный
размен: пачка вместо потока уведомлений на каждое событие.

Проверь, что состояние действительно уехало в changelog:

```bash
~/kafka/bin/kafka-topics.sh --bootstrap-server $BS --command-config $CFG \
  --list | grep lesson16-ex3
```

Должен быть `lesson16-ex3-sensor-store-changelog`. Его создало приложение, под
префиксной ACL с Create.

### Шаг 4.3 — ловушка STREAM_TIME

Останови приложение. В `Ex3SensorPunctuate` поменяй тип часов:

```java
PunctuationType.WALL_CLOCK_TIME   →   PunctuationType.STREAM_TIME
```

Пересобери и запусти снова, но **ничего не отправляй**. Пунктуатор молчит:
стримовое время двигают только входящие записи, а их нет. Отправь три
записи — тики пойдут. Перестань отправлять — тики сразу же встанут.

Это та же ловушка, что `suppress(untilWindowCloses)` в уроке 15. Верни
`WALL_CLOCK_TIME` перед следующим упражнением.

## 5. Ex4 — `process()` внутри DSL-цепочки

Нормальная продакшн-форма: DSL делает обвязку, процессор стоит там, где DSL
кончился. Цепочка читает `purchases` из урока 15, фильтрует через `filter()`,
пропускает через `processValues()` с `FixedKeyProcessor` и пишет через `to()`.

```bash
gradle ex4 --no-daemon
```

Формат CSV тот же, что в уроке 15:
`customerId,employeeId,department,amount,card`.

```bash
printf '%s\n' \
  "c1,e7,cafe,8.50,4111111111111234" \
  "c2,e9,electronics,900.00,5555444433332222" \
  | ~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS \
    --command-config $CFG --topic purchases
```

Смотри вывод приложения. Процессор печатает `partition` и `offset` каждой
записи через `context.recordMetadata()` — информация, которой в DSL просто нет.

### Проверь себя

Почему здесь `processValues()`, а не `process()`? Что появится в `describe()`,
если поменять одно на другое? (Подсказка: `purchases` — две партиции, а
`process()` может менять ключ.)

## 6. Ex5 — Interactive Queries на двух инстансах

Самое тяжёлое упражнение курса по памяти. Прочти шаг 6.0 до того, как
что-либо запускать.

Приложение считает события по ключу в хранилище `count-store` и поднимает
HTTP-эндпоинт `GET /count?key=<ключ>`. HTTP — голый
`com.sun.net.httpserver` из JDK, без Spring: два Spring Boot на t3.small рядом
с брокером не живут.

### Шаг 6.0 — память

На этой машине одновременно будут жить брокер плюс два JVM приложения.
Проверь, что свободной памяти хватает:

```bash
free -m
```

Нужно хотя бы ~600 MB в колонке `available`. Если меньше — закрой всё
лишнее и убей gradle-демоны:

```bash
pkill -f GradleDaemon
```

Каждый инстанс запускается с `-Xmx192m` (прописано в `build.gradle` через `jvmArgs`
в блоке задач). Не убирай это — без ограничения JVM возьмёт
четверть памяти хоста под heap, и второй инстанс либо не поднимется, либо
его убьёт OOM killer в середине ребаланса.

Собери один раз заранее, чтобы Gradle не компилировал параллельно с
работающими инстансами:

```bash
gradle build --no-daemon
```

### Шаг 6.1 — первый инстанс

Терминал A:

```bash
gradle ex5 --no-daemon -Pport=8080
```

Ждём строку `state=RUNNING`. До неё эндпоинт отвечает 503 — это не баг, а
обработанный `InvalidStateStoreException` из лекции.

Терминал B — подать события по нескольким ключам (ключи разные нарочно —
они должны разойтись по трём партициям):

```bash
printf '%s\n' "energy:1" "finance:1" "retail:1" "energy:1" "finance:1" \
  | ~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS \
    --command-config $CFG --topic iq-events \
    --property parse.key=true --property key.separator=:
```

Запроси все три ключа:

```bash
for k in energy finance retail; do
  echo -n "$k -> "; curl -s "http://localhost:8080/count?key=$k"; echo
done
```

Пока инстанс один, он владеет всеми тремя партициями, и все ответы
локальные. В логе каждого запроса видно `local`.

### Шаг 6.2 — второй инстанс

Терминал C:

```bash
gradle ex5 --no-daemon -Pport=8081
```

Пойдёт ребаланс — ждём, пока **оба** инстанса напишут `state=RUNNING`.
Три партиции на два инстанса делятся как 2 + 1.

Теперь спроси те же три ключа у **обоих** портов:

```bash
for p in 8080 8081; do
  echo "== инстанс $p =="
  for k in energy finance retail; do
    echo -n "$k -> "; curl -s "http://localhost:$p/count?key=$k"; echo
  done
done
```

Главный результат всего урока: **оба порта дают одинаковые ответы**, хотя
ни у одного из них нет всех данных. Смотри логи: часть запросов
обслужена `local`, часть — `remote -> localhost:80xx`. Это работает
`queryMetadataForKey`.

Проверь распределение со стороны брокера:

```bash
~/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $BS \
  --command-config $CFG --describe --group lesson16-ex5
```

В колонке `CONSUMER-ID` будут два разных члена группы, и видно, какие
партиции кому достались.

### Шаг 6.3 — убить инстанс

`Ctrl-C` в терминале C (порт 8081). Сразу же, не ждав, долби оставшийся
инстанс в цикле:

```bash
while true; do
  echo -n "$(date +%T) finance -> "
  curl -s "http://localhost:8080/count?key=finance"; echo
  sleep 1
done
```

Наблюдаем три фазы подряд:

1. несколько секунд ошибки соединения — инстанс ещё считает, что ключ
   удалённый, и стучится на мёртвый 8081;
2. затем 503 — ребаланс прошёл, партиция наша, но хранилище ещё
   восстанавливается из changelog;
3. вновь корректный ответ, теперь `local`.

Фаза 2 — то самое окно восстановления из лекции. Счётчик после
восстановления тот же, что был, — состояние не потерялось вместе с
умершим инстансом, потому что оно лежит в changelog-топике в Kafka.

### Шаг 6.4 — неизвестный ключ

```bash
curl -i "http://localhost:8080/count?key=nosuchkey"
```

Ждём `404`, а не 500 и не пустой 200. `store.get()` вернул `null`, и код это
обработал. Если видишь 500 с NPE — это ровно тот баг, про который
предупреждает лекция.

Останови оставшийся инстанс.

## 7. Сброс и очистка

Чтобы прогнать упражнение с нуля, останови его и сбрось состояние вместе с
внутренними топиками:

```bash
~/kafka/bin/kafka-streams-application-reset.sh --bootstrap-server $BS \
  --config-file $CFG --application-id lesson16-ex5 --input-topics iq-events
rm -rf state/lesson16-ex5
```

Для Ex1 есть нюанс: если приложение убито в середине транзакции,
`read_committed`-консьюмеры будут висеть, пока координатор не отменит её по
таймауту. Посмотреть, что висит:

```bash
~/kafka/bin/kafka-transactions.sh --bootstrap-server $BS --command-config $CFG \
  list
```

Не добивай транзакцию вручную через `forceTerminate` — подожди таймаут и
посмотри, как оно разрешается само. Это часть упражнения.

Когда закончил на сегодня:

```bash
rm -f /tmp/admin.properties client.properties
pkill -f GradleDaemon
```

Топики лабораторные, но управляются Terraform — **не удаляй их**
через `kafka-topics.sh`, иначе следующий `terraform plan` захочет их
пересоздать. Чтобы снести лабу, убери блоки урока 16 из
`lesson7/gitops/topics.tf` и `acls.tf` и сделай apply.

Внутренние топики `lesson16-*` — другое дело: их создало приложение, в
состоянии Terraform их нет, и `kafka-streams-application-reset.sh` убирает
их штатно.

## Что сдать

1. Вывод `kafka-transactions.sh list` с видным `lesson16-ex1-...` (шаг 2.2).
2. Вывод `topology.describe()` до и после адресного `forward` (шаг 3.3).
3. Два скрина или вывода шага 4.3: тики идут на `WALL_CLOCK_TIME` без
   трафика и молчат на `STREAM_TIME`.
4. Вывод цикла из шага 6.3, где видны все три фазы — ошибка соединения,
   503, корректный ответ.
5. Ответы на три вопроса из раздела 2 и вопрос из раздела 5.
