# HW3 — Транзакции

**Задание.** Создать `topic1` и `topic2`. Написать продюсер: транзакция —
5 сообщений в каждый топик → commit; вторая транзакция — 2 сообщения в
каждый → abort. Написать консьюмер, который показывает только
подтверждённые сообщения (аборт не виден).

**Решение.** Продюсер с `transactional.id`, консьюмер с
`isolation.level=read_committed`. Запуск на брокере `kafka` EC2
(SASL/PLAIN, principal alice).

```
transactions-java/
├── build.gradle              # ./gradlew producer | consumer
├── client.properties.example
└── src/main/java/demo/
    ├── Utils.java            # SASL-конфиг, topic1/topic2
    ├── TxProducer.java       # tx1: 5+5 commit;  tx2: 2+2 abort
    └── TxConsumer.java       # read_committed
```

## Запуск

**1. Топики**
```bash
cd ~/kafka
for t in topic1 topic2; do
  bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --command-config clients/admin.properties \
    --create --topic "$t" --partitions 1 --replication-factor 1
done
```

**2. ACL для alice** (producer + consumer)
```bash
bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --command-config clients/admin.properties --add \
  --producer --topic topic1 --topic topic2 \
  --transactional-id hw3-producer --allow-principal User:alice

bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --command-config clients/admin.properties --add \
  --consumer --topic topic1 --topic topic2 \
  --group hw3-consumer --allow-principal User:alice
```

**3. client.properties** (пароль alice)
```bash
cd ~/kafka-repo/hw3/transactions-java
cp client.properties.example client.properties
sed -i 's/<PLACEHOLDER>/alice-pass/' client.properties
```

**4. Продюсер**
```bash
gradle producer --no-daemon --max-workers=1 2>/dev/null | grep -E 'COMMITTED|ABORTED'
```
```
Transaction 1 COMMITTED: 5 messages to topic1 and 5 to topic2
Transaction 2 ABORTED: 2 messages to each topic discarded
```

**5. Консьюмер**
```bash
gradle consumer --no-daemon --max-workers=1 2>/dev/null | grep -E 'committed-|topic[12]:|Expected'
```
```
topic1 [p0 off0] key1 = committed-1
...
topic2 [p0 off4] key5 = committed-5
----
topic1: 5 messages (read_committed)
topic2: 5 messages (read_committed)
Expected 5 per topic. Aborted messages are NOT shown.
```

**Результат:** 5 сообщений на топик — подтверждённая транзакция видна,
отменённая (2+2) не видна. Задание выполнено.

## Как работает

- `commitTransaction()` пишет commit-маркеры → `read_committed` отдаёт эти
  записи.
- `abortTransaction()` пишет abort-маркеры → записи остаются в логе, но
  `read_committed` их пропускает (видит только `read_uncommitted`).
- `transactional.id` включает транзакции и идемпотентность;
  `initTransactions()` вызывается один раз перед первой транзакцией.

## Проверка (опционально)

`read_committed` видит 5, `read_uncommitted` — 7 (5 + 2 аборт): аборт
физически в логе, просто скрыт isolation level.
```bash
cd ~/kafka
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --command-config clients/admin.properties \
  --topic topic1 --from-beginning --timeout-ms 5000 2>/dev/null | grep -c committed
# 5
```

## Требования к брокеру

На одиночной ноде нужны (иначе `initTransactions()` виснет):
```
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```
