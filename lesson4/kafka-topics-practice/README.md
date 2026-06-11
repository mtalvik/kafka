# Kafka Topics Practice в Docker на Windows

Практика к теме: **топики, records, offsets, partitions и основные операции `kafka-topics.sh`**.

Проект запускает один Kafka-брокер в Docker **без ZooKeeper**, в режиме **KRaft**. Этого достаточно для учебной практики на Windows через Docker Desktop.

---

## 1. Что понадобится

Установите:

1. **Docker Desktop for Windows**
2. Включённый Docker Engine
3. PowerShell или Windows Terminal

Проверка:

```powershell
docker --version
docker compose version
```

Если команды работают, можно запускать практику.

---

## 2. Файл `docker-compose.yml`

В корне проекта должен быть такой `docker-compose.yml`:

```yaml
services:
  kafka:
    image: apache/kafka:3.7.0
    container_name: kafka
    hostname: kafka
    ports:
      - "9095:9095"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller

      KAFKA_LISTENERS: PLAINTEXT://:9092,EXTERNAL://:9095,CONTROLLER://:9096
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9095
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9096

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 3

    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 10
```

Важно:

```text
container_name: kafka
```

Значит во всех командах `docker exec` имя контейнера будет **kafka**, а не `kafka-practice`.

Также запомните порты:

```text
9092 — broker внутри контейнера. Его используем в docker exec.
9095 — broker с Windows-хоста. Его используют внешние клиенты на Windows.
9096 — controller. Руками к нему через kafka-topics.sh не подключаемся.
```

Если подключиться к `9096`, будет ошибка вида:

```text
The remote node is not a BROKER that supports the METADATA api
```

---

## 3. Запуск Kafka

Откройте PowerShell в папке проекта и выполните:

```powershell
docker compose down -v
docker compose up -d
```

Проверьте, что контейнер запущен:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Вы должны увидеть контейнер:

```text
kafka
```

Посмотреть логи Kafka:

```powershell
docker logs -f kafka
```

Когда в логах нет постоянных ошибок и контейнер не перезапускается, Kafka готова.

---

## 4. Как выполнять Kafka-команды

Все команды ниже запускаются **внутри контейнера** через `docker exec`.

Общий шаблон:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 <операция>
```

Здесь:

```text
kafka                         имя контейнера
/opt/kafka/bin/kafka-topics.sh полный путь до утилиты
localhost:9092                broker-порт внутри контейнера
```

Не используйте в этих командах:

```text
kafka-practice
localhost:9095
localhost:9096
```

`kafka-practice` — неправильное имя контейнера для текущего compose-файла.  
`9095` нужен для подключения с Windows-хоста, а не изнутри контейнера.  
`9096` — это controller-порт, а не broker-порт.

---

## 5. Практика 1: список топиков

Команда:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Сначала список может быть пустым. Это нормально: мы ещё не создавали свои топики.

Смысл команды:

```text
--list — вывести список существующих топиков
```

---

## 6. Практика 2: создать топик

Создадим топик `kinaction_helloworld` с тремя партициями:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic kinaction_helloworld --partitions 3 --replication-factor 1
```

Ожидаемый результат:

```text
Created topic kinaction_helloworld.
```

Что здесь важно:

```text
--create                       создать топик
--topic kinaction_helloworld   имя топика
--partitions 3                 количество партиций
--replication-factor 1         одна копия каждой партиции
```

Для локальной практики `replication-factor 1` подходит, потому что у нас один брокер. В реальном production-кластере обычно используют больше брокеров и replication factor, например `3`.

---

## 7. Практика 3: снова вывести список топиков

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Теперь должен появиться топик:

```text
kinaction_helloworld
```

Так мы проверяем, что создание прошло успешно.

---

## 8. Практика 4: описание топика

Посмотрим устройство топика:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic kinaction_helloworld
```

Важно: `--describe` работает только для уже созданного топика. Если сначала не выполнить `--create`, будет ошибка:

```text
Topic 'kinaction_helloworld' does not exist as expected
```

Пример вывода:

```text
Topic: kinaction_helloworld TopicId: ... PartitionCount: 3 ReplicationFactor: 1 Configs: ...
Topic: kinaction_helloworld Partition: 0 Leader: 1 Replicas: 1 Isr: 1
Topic: kinaction_helloworld Partition: 1 Leader: 1 Replicas: 1 Isr: 1
Topic: kinaction_helloworld Partition: 2 Leader: 1 Replicas: 1 Isr: 1
```

Расшифровка:

```text
PartitionCount: 3    у топика три партиции
ReplicationFactor: 1 каждая партиция хранится в одной копии
Leader: 1            брокер-лидер для партиции
Replicas: 1          где лежат копии партиции
Isr: 1               in-sync replicas, синхронные реплики
```

Главная идея: топик не является одной физической очередью. Он состоит из партиций.

---

## 9. Практика 5: записать сообщения в топик

Теперь отправим несколько сообщений. Запустите producer:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic kinaction_helloworld
```

После запуска вводите сообщения по одному и нажимайте Enter:

```text
message-1
message-2
message-3
```

Чтобы выйти из producer:

```text
Ctrl + C
```

Что произошло: producer опубликовал records в topic `kinaction_helloworld`.

---

## 10. Практика 6: прочитать сообщения из начала топика

Запустите consumer:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kinaction_helloworld --from-beginning
```

Вы должны увидеть сообщения:

```text
message-1
message-2
message-3
```

Чтобы остановить consumer:

```text
Ctrl + C
```

`--from-beginning` означает: читать существующие сообщения с начала доступного лога.

---

## 11. Практика 7: посмотреть offsets и partitions

Запустим consumer так, чтобы он показывал служебную информацию: topic, partition и offset.

В PowerShell выполните:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kinaction_helloworld --from-beginning --property print.partition=true --property print.offset=true --property print.key=true --property print.value=true
```

Пример вывода может быть таким:

```text
Partition:0 Offset:0 null message-1
Partition:1 Offset:0 null message-2
Partition:2 Offset:0 null message-3
```

Или сообщения могут оказаться в других партициях. Это нормально.

Важно:

```text
offset считается внутри конкретной partition
partition 0 имеет свои offsets
partition 1 имеет свои offsets
partition 2 имеет свои offsets
```

---

## 12. Практика 8: создать сообщения с ключами

Ключ помогает Kafka выбирать партицию. Сообщения с одинаковым ключом обычно попадают в одну и ту же партицию.

Запустите producer с парсингом ключа:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic kinaction_helloworld --property parse.key=true --property key.separator=:
```

Вводите сообщения в формате `key:value`:

```text
order-777:created
order-777:paid
order-777:shipped
order-888:created
order-888:paid
```

Выйдите через `Ctrl + C`.

Теперь прочитайте с partition и offset:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kinaction_helloworld --from-beginning --property print.partition=true --property print.offset=true --property print.key=true --property print.value=true
```

Обратите внимание: события с одинаковым ключом должны идти в одной партиции. Так Kafka сохраняет порядок событий для одной сущности, например для одного заказа.

---

## 13. Практика 9: увеличить количество партиций

Сейчас у топика 3 партиции. Увеличим до 6:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic kinaction_helloworld --partitions 6
```

Проверим:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic kinaction_helloworld
```

Теперь должно быть:

```text
PartitionCount: 6
```

Важно: количество партиций можно увеличить, но нельзя просто так уменьшить.

Также увеличение количества партиций может изменить распределение новых сообщений по ключам. Поэтому в реальных системах это делают осторожно.

---

## 14. Практика 10: удалить топик

Удалим учебный топик:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic kinaction_helloworld
```

Проверим список:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Топика `kinaction_helloworld` больше не должно быть.

Удаление топика удаляет и данные внутри него.

---

## 15. Дополнительная практика: второй топик

Создайте топик `orders_created`:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders_created --partitions 3 --replication-factor 1
```

Проверьте список топиков:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Посмотрите описание:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders_created
```

Отправьте сообщения:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders_created
```

Примеры сообщений:

```text
order-1 created
order-2 created
order-3 created
```

Прочитайте сообщения:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders_created --from-beginning
```

---

## 16. Быстрый справочник команд

```powershell
# Запустить Kafka
docker compose up -d

# Остановить Kafka
docker compose down

# Остановить Kafka и удалить данные
docker compose down -v

# Посмотреть контейнеры
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Посмотреть логи
docker logs -f kafka

# Список топиков
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Создать топик
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic kinaction_helloworld --partitions 3 --replication-factor 1

# Описать топик
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic kinaction_helloworld

# Увеличить число партиций
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic kinaction_helloworld --partitions 6

# Удалить топик
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic kinaction_helloworld

# Producer без ключей
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic kinaction_helloworld

# Producer с ключами
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic kinaction_helloworld --property parse.key=true --property key.separator=:

# Consumer с начала
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kinaction_helloworld --from-beginning

# Consumer с partition, offset, key, value
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kinaction_helloworld --from-beginning --property print.partition=true --property print.offset=true --property print.key=true --property print.value=true
```

---

## 17. Задания для самостоятельной проверки

1. Создайте топик `payments_completed` с двумя партициями.
2. Выведите список топиков.
3. Посмотрите описание `payments_completed`.
4. Отправьте 5 сообщений без ключей.
5. Прочитайте сообщения с `--from-beginning`.
6. Отправьте 5 сообщений с ключами `payment-1` и `payment-2`.
7. Проверьте, в какие партиции попали сообщения.
8. Увеличьте количество партиций до 4.
9. Проверьте описание топика.
10. Удалите учебные топики.

---

## 18. Частые проблемы на Windows

### `No such container: kafka-practice`

Вы используете старое имя контейнера.

Неправильно:

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Правильно:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Проверить реальные имена контейнеров:

```powershell
docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"
```

### `The node does not support METADATA`

Скорее всего, вы подключились к `9096`.

Неправильно:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9096 --list
```

Правильно:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

`9096` — это controller-порт. Для обычных команд Kafka нужен broker-порт `9092`.

### `Topic 'kinaction_helloworld' does not exist as expected`

Вы пытаетесь описать топик до его создания.

Сначала:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic kinaction_helloworld --partitions 3 --replication-factor 1
```

Потом:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic kinaction_helloworld
```

### Контейнер не запускается

Проверьте Docker Desktop. Он должен быть запущен.

```powershell
docker ps
```

Посмотрите логи:

```powershell
docker logs kafka
```

### Порт 9095 занят

Если порт занят, остановите старые Kafka-контейнеры:

```powershell
docker ps
```

Затем остановите ненужный контейнер:

```powershell
docker stop <container_name>
```

### Хочу начать практику заново

Остановите контейнеры и удалите volume с данными:

```powershell
docker compose down -v
```

Потом снова:

```powershell
docker compose up -d
```

### PowerShell странно обрабатывает переносы строк

В README команды даны одной строкой. Просто копируйте их целиком.

---

## 19. Короткая лекционная формула

```text
Topic — имя потока данных.
Record — одно сообщение в топике.
Offset — позиция сообщения внутри партиции.
Partition — упорядоченный раздел топика.
Порядок гарантируется только внутри partition.
kafka-topics.sh управляет топиками: list, describe, create, delete, alter.
```

## параметры


Проверьте, что контейнер работает:

```bash
docker ps
```

Удобно сразу зайти внутрь контейнера:

```bash
docker exec -it kafka bash
```

Дальше команды в README предполагают, что вы находитесь внутри контейнера `kafka`.

---

## 2. Базовые переменные внутри контейнера

Для удобства задайте переменную:

```bash
BS=localhost:9092
```

Проверка подключения:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --list
```

Если команда отработала без ошибки, Kafka готова.

---

## 3. Демонстрация: создание топика с партициями

Создадим топик с тремя партициями. Так как у нас один брокер, replication factor должен быть `1`.

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BS \
  --create \
  --topic demo_orders \
  --partitions 3 \
  --replication-factor 1
```

Посмотреть список топиков:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --list
```

Посмотреть описание топика:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BS \
  --describe \
  --topic demo_orders
```

Что объяснять на этом шаге:

- `Topic` — имя потока данных.
- `PartitionCount: 3` — топик разделён на три партиции.
- `ReplicationFactor: 1` — каждая партиция имеет одну копию, потому что брокер один.
- `Leader` — брокер, который принимает чтение и запись для партиции.
- `Replicas` — список брокеров, где лежат копии партиции.
- `Isr` — синхронизированные реплики.

---

## 4. Демонстрация: запись сообщений в топик

Запустите producer:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS \
  --topic demo_orders
```

Введите несколько сообщений вручную:

```text
order-1 created
order-2 created
order-3 created
order-1 paid
order-2 shipped
```

Завершите producer через `Ctrl+C`.

Теперь прочитайте сообщения:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server $BS \
  --topic demo_orders \
  --from-beginning \
  --timeout-ms 5000
```

Что объяснять:

- Producer пишет сообщения в topic.
- Kafka кладёт каждое сообщение в одну из партиций.
- Consumer читает сообщения из topic.
- Сообщения не удаляются сразу после чтения.

---

## 5. Демонстрация: key-value сообщения и распределение по партициям

Создадим отдельный топик:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BS \
  --create \
  --topic demo_keyed_orders \
  --partitions 3 \
  --replication-factor 1
```

Запустите producer с ключами:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS \
  --topic demo_keyed_orders \
  --property parse.key=true \
  --property key.separator=:
```

Введите сообщения:

```text
order-1:created
order-2:created
order-1:paid
order-3:created
order-1:shipped
order-2:paid
```

Завершите через `Ctrl+C`.

Прочитайте сообщения с печатью ключей, partition и offset:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server $BS \
  --topic demo_keyed_orders \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --timeout-ms 5000
```

Что объяснять:

- Kafka использует key, чтобы выбрать партицию.
- Сообщения с одинаковым key обычно попадают в одну и ту же партицию.
- Порядок гарантируется только внутри одной партиции.
- Offset считается отдельно внутри каждой партиции.

---

## 6. Демонстрация: offset’ы

Посмотреть offset’ы по топику:

```bash
/opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server $BS \
  --topic demo_keyed_orders
```

Пример вывода:

```text
demo_keyed_orders:0:2
demo_keyed_orders:1:0
demo_keyed_orders:2:4
```

Как читать:

```text
topic:partition:latest-offset
```

Что объяснять:

- Offset — позиция сообщения внутри партиции.
- У каждой партиции своя последовательность offset’ов.
- Latest offset показывает позицию, до которой дошла запись.

---

## 7. Демонстрация: файлы журналов на диске

Kafka внутри контейнера хранит данные в директории логов. Обычно для образа Apache Kafka это:

```bash
ls -la /tmp/kafka-logs
```

Посмотрите директории партиций:

```bash
ls -la /tmp/kafka-logs | grep demo
```

Пример:

```text
demo_orders-0
demo_orders-1
demo_orders-2
demo_keyed_orders-0
demo_keyed_orders-1
demo_keyed_orders-2
```

Зайдите в одну партицию:

```bash
ls -la /tmp/kafka-logs/demo_keyed_orders-0
```

Вы увидите файлы вроде:

```text
00000000000000000000.log
00000000000000000000.index
00000000000000000000.timeindex
```

Что объяснять:

- Партиция хранится на диске как каталог.
- `.log` содержит сами сообщения.
- `.index` помогает искать сообщения по offset.
- `.timeindex` помогает искать сообщения по времени.
- Имя сегмента соответствует начальному offset’у сегмента.

---

## 8. Демонстрация: просмотр содержимого log-файла

Найдите `.log` файл:

```bash
ls /tmp/kafka-logs/demo_keyed_orders-0/*.log
```

Просмотрите его через `kafka-dump-log.sh`:

```bash
/opt/kafka/bin/kafka-dump-log.sh \
  --files /tmp/kafka-logs/demo_keyed_orders-0/00000000000000000000.log \
  --print-data-log
```

Если в партиции `0` нет сообщений, попробуйте партицию `1` или `2`:

```bash
/opt/kafka/bin/kafka-dump-log.sh \
  --files /tmp/kafka-logs/demo_keyed_orders-1/00000000000000000000.log \
  --print-data-log
```

Что объяснять:

- Это низкоуровневый просмотр сегмента Kafka.
- Обычно разработчик читает данные через consumer, а не через файлы.
- Но для понимания устройства Kafka полезно увидеть, что сообщения реально лежат в log-сегментах.

---

## 9. Демонстрация: retention по времени

Создадим топик, который быстро удаляет старые данные.

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BS \
  --create \
  --topic demo_retention \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=10000 \
  --config segment.ms=5000 \
  --config segment.bytes=1024
```

Запишите сообщения:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS \
  --topic demo_retention
```

Введите:

```text
message-1
message-2
message-3
```

Завершите через `Ctrl+C`.

Сразу прочитайте:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server $BS \
  --topic demo_retention \
  --from-beginning \
  --timeout-ms 5000
```

Подождите 20–40 секунд:

```bash
sleep 30
```

Попробуйте прочитать снова:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server $BS \
  --topic demo_retention \
  --from-beginning \
  --timeout-ms 5000
```

Что объяснять:

- `retention.ms=10000` означает хранить данные около 10 секунд.
- `segment.ms=5000` помогает быстрее закрывать активный сегмент.
- Kafka удаляет не отдельные сообщения, а старые закрытые сегменты.
- Поэтому удаление может произойти не мгновенно ровно через 10 секунд.

---

## 10. Демонстрация: изменение параметров существующего топика

Посмотреть настройки топика:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server $BS \
  --entity-type topics \
  --entity-name demo_retention \
  --describe
```

Изменить retention:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server $BS \
  --entity-type topics \
  --entity-name demo_retention \
  --alter \
  --add-config retention.ms=60000
```

Снова посмотреть настройки:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server $BS \
  --entity-type topics \
  --entity-name demo_retention \
  --describe
```

Удалить пользовательскую настройку:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server $BS \
  --entity-type topics \
  --entity-name demo_retention \
  --alter \
  --delete-config retention.ms
```

Что объяснять:

- `kafka-topics.sh --config` задаёт параметры при создании топика.
- `kafka-configs.sh --alter --add-config` меняет параметры существующего топика.
- `--delete-config` удаляет настройку с уровня топика, после чего применяется значение по умолчанию брокера.

---

## 11. Демонстрация: compacted topic

Создадим compacted topic:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BS \
  --create \
  --topic demo_compact \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config segment.ms=5000 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config delete.retention.ms=10000
```

Запустите producer с ключами:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS \
  --topic demo_compact \
  --property parse.key=true \
  --property key.separator=:
```

Введите несколько версий одних и тех же ключей:

```text
customer-0:Basic
customer-1:Gold
customer-0:Gold
customer-2:Basic
customer-1:Basic
```

Завершите через `Ctrl+C`.

Прочитайте до compaction:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server $BS \
  --topic demo_compact \
  --from-beginning \
  --property print.key=true \
  --property print.offset=true \
  --timeout-ms 5000
```

Подождите, чтобы активный сегмент закрылся и cleaner получил шанс поработать:

```bash
sleep 30
```

Добавьте ещё одно сообщение, чтобы Kafka активнее создала новый сегмент:

```bash
printf 'customer-3:Silver\n' | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS \
  --topic demo_compact \
  --property parse.key=true \
  --property key.separator=:
```

Снова подождите:

```bash
sleep 30
```

Прочитайте снова:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server $BS \
  --topic demo_compact \
  --from-beginning \
  --property print.key=true \
  --property print.offset=true \
  --timeout-ms 5000
```

Что объяслять:

- `cleanup.policy=compact` включает log compaction.
- Kafka старается оставить последнее значение для каждого ключа.
- Compaction не происходит мгновенно.
- Активный сегмент не сжимается, сжимаются закрытые сегменты.
- Offset’ы не перенумеровываются, поэтому после compaction возможны пропуски.

Важно: на маленьком локальном стенде compaction может быть не очень зрелищной и не всегда происходит сразу. Это нормально: cleaner работает фоново.

---

## 12. Демонстрация: tombstone, то есть удаление ключа

В compacted topic удаление ключа делается сообщением с тем же key и `null` value.

Для console producer проще передать пустое значение после разделителя:

```bash
printf 'customer-0:\n' | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS \
  --topic demo_compact \
  --property parse.key=true \
  --property key.separator=:
```

Прочитайте топик:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server $BS \
  --topic demo_compact \
  --from-beginning \
  --property print.key=true \
  --property print.offset=true \
  --timeout-ms 5000
```

Что объяснять:

- Tombstone — это запись `key=X, value=null`.
- Для compacted topic это означает удаление ключа из итогового состояния.
- Tombstone не удаляется мгновенно, он хранится некоторое время.
- Это нужно, чтобы отставшие consumers увидели факт удаления.

Примечание: в `kafka-console-producer.sh` пустое значение может отображаться как пустая строка. В приложениях на Java/Go/Python tombstone обычно отправляют именно как `null` value.

---

## 13. Демонстрация: удаление записей до offset’а

Создадим топик:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BS \
  --create \
  --topic demo_delete_records \
  --partitions 1 \
  --replication-factor 1
```

Запишем сообщения:

```bash
for i in 0 1 2 3 4 5; do
  echo "message-$i"
done | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS \
  --topic demo_delete_records
```

Проверим чтение:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server $BS \
  --topic demo_delete_records \
  --from-beginning \
  --property print.offset=true \
  --timeout-ms 5000
```

Создадим JSON-файл для удаления записей до offset `3`:

```bash
cat > /tmp/delete-records.json <<'JSON'
{
  "partitions": [
    {
      "topic": "demo_delete_records",
      "partition": 0,
      "offset": 3
    }
  ],
  "version": 1
}
JSON
```

Выполним удаление:

```bash
/opt/kafka/bin/kafka-delete-records.sh \
  --bootstrap-server $BS \
  --offset-json-file /tmp/delete-records.json
```

Прочитаем снова:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server $BS \
  --topic demo_delete_records \
  --from-beginning \
  --property print.offset=true \
  --timeout-ms 5000
```

Что объяснять:

- Kafka не удаляет произвольную одну запись из середины.
- `kafka-delete-records.sh` удаляет данные до указанного offset’а.
- После удаления первые доступные offset’ы меняются.

---

## 14. Демонстрация: `max.message.bytes`

Создадим топик с маленьким лимитом размера сообщения:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BS \
  --create \
  --topic demo_max_message \
  --partitions 1 \
  --replication-factor 1 \
  --config max.message.bytes=100
```

Отправим короткое сообщение:

```bash
echo "small" | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS \
  --topic demo_max_message
```

Попробуем отправить большое сообщение:

```bash
python3 - <<'PY' | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic demo_max_message
print('x' * 1000)
PY
```

Ожидаемо producer может получить ошибку, потому что сообщение больше лимита топика.

Что объяснять:

- `max.message.bytes` ограничивает размер одного сообщения в топике.
- Большие сообщения требуют согласования настроек producer, broker/topic и consumer.
- Kafka обычно не используют для очень больших payload’ов; часто в Kafka кладут ссылку на объект, а сам объект хранят в S3/MinIO/БД.

---

## 15. Команды для очистки демо

Удалить созданные топики:

```bash
for topic in \
  demo_orders \
  demo_keyed_orders \
  demo_retention \
  demo_compact \
  demo_delete_records \
  demo_max_message
  do
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server $BS \
      --delete \
      --topic $topic || true
  done
```

Остановить контейнер:

```bash
docker compose down
```

Если хотите удалить все локальные данные контейнера и начать заново:

```bash
docker compose down -v
```

---

## 16. Короткий сценарий для лекции

Рекомендуемый порядок демонстрации:

1. Запустить Kafka через Docker Compose.
2. Создать `demo_orders` и показать `--list`, `--describe`.
3. Записать и прочитать простые сообщения.
4. Создать `demo_keyed_orders`, отправить key-value сообщения.
5. Показать partition и offset при чтении.
6. Показать директории `/tmp/kafka-logs` и файлы `.log`, `.index`, `.timeindex`.
7. Показать `kafka-dump-log.sh`.
8. Создать `demo_retention`, показать удаление по времени.
9. Создать `demo_compact`, показать идею сохранения последнего значения по ключу.
10. Показать tombstone для удаления ключа.
11. Показать `kafka-delete-records.sh`.
12. Показать динамическое изменение параметров через `kafka-configs.sh`.

Главная мысль всей демонстрации:

```text
Topic — логический поток.
Partition — физический журнал внутри топика.
Record — сообщение.
Offset — позиция сообщения в partition.
Segment — файл-кусок журнала.
Retention удаляет старые segment’ы.
Compaction сохраняет последнее значение по ключу.
```
