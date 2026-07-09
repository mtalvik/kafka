# HW3 — Transactions and Exactly-Once

Homework: build and run applications that use Kafka transactions.

**Task.** Create two topics `topic1` and `topic2`. Write a producer that
runs two transactions — the first commits 5 messages to each topic, the
second sends 2 messages to each topic and aborts. Write a consumer that
reads both topics and shows only the messages from the committed
transaction; the aborted ones must not appear.

The whole task reduces to one idea: a `read_committed` consumer never sees
records from an aborted transaction. The producer proves it by aborting a
transaction on purpose.

## What is in this folder

```
hw3/
├── LAB.md                         # this document
└── transactions-java/             # Gradle project, kafka-clients 4.0.0
    ├── build.gradle               # tasks: ./gradlew producer | consumer
    ├── settings.gradle
    ├── .gitignore                 # excludes client.properties, build/
    ├── client.properties.example
    └── src/main/java/demo/
        ├── Utils.java             # SASL config loader; topic1/topic2 names
        ├── TxProducer.java        # tx1: 5+5 -> commit; tx2: 2+2 -> abort
        └── TxConsumer.java        # isolation.level=read_committed
```

The apps run against the existing `kafka` EC2 broker (Apache Kafka 4.3.0,
KRaft single-node, SASL/PLAIN) as principal `alice`.

## Prerequisites

- Broker running with the single-node transaction settings (from lesson 10):
  `transaction.state.log.replication.factor=1` and
  `transaction.state.log.min.isr=1`. Without them `initTransactions()` hangs.
- Gradle 8.8 on the broker; `alice` SASL principal.

## Step 1: start the broker

```bash
cd ~/otus-kafka
./aws-lab.sh start kafka
./aws-lab.sh ssh kafka
```

Confirm the transaction settings are in place:

```bash
grep -E 'transaction.state.log' ~/kafka/config/server.properties
# transaction.state.log.replication.factor=1
# transaction.state.log.min.isr=1
```

## Step 2: create topic1 and topic2

```bash
cd ~/kafka
for t in topic1 topic2; do
  bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --command-config clients/admin.properties \
    --create --topic "$t" --partitions 1 --replication-factor 1
done

bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --command-config clients/admin.properties --list | grep -E '^topic[12]$'
```

## Step 3: grant alice the ACLs

`alice` acts as both the transactional producer and the consumer, so she
needs write/read/describe on both topics, write/describe on the
transactional id, and read on the consumer group.

```bash
cd ~/kafka

# producer: write+describe on both topics + the transactional id
bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --command-config clients/admin.properties --add \
  --producer --topic topic1 --topic topic2 \
  --transactional-id hw3-producer \
  --allow-principal User:alice

# consumer: read+describe on both topics + read on the group
bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --command-config clients/admin.properties --add \
  --consumer --topic topic1 --topic topic2 \
  --group hw3-consumer \
  --allow-principal User:alice
```

## Step 4: configure client.properties

```bash
cd ~/kafka-repo/hw3/transactions-java
cp client.properties.example client.properties
sed -i "s/<PLACEHOLDER>/$(grep user_alice ~/kafka/config/kafka_server_jaas.conf | cut -d'"' -f2)/" client.properties
```

`client.properties` is gitignored — the password never reaches git.

## Step 5: run the producer

```bash
gradle producer --no-daemon --max-workers=1 2>/dev/null \
  | grep -E 'COMMITTED|ABORTED'
```

Expected:

```
Transaction 1 COMMITTED: 5 messages to topic1 and 5 to topic2
Transaction 2 ABORTED: 2 messages to each topic discarded
```

The producer opened two transactions. The first committed 5+5 records; the
second sent 2+2 records and aborted them. The aborted records are physically
written to the log but marked aborted, so a `read_committed` consumer skips
them.

## Step 6: run the consumer

```bash
gradle consumer --no-daemon --max-workers=1 2>/dev/null \
  | grep -E 'committed-|topic[12]:|Expected'
```

Expected — only the 5 committed messages per topic, none of the aborted:

```
topic1 [p0 off0] key1 = committed-1
topic1 [p0 off1] key2 = committed-2
...
topic2 [p0 off4] key5 = committed-5
----
topic1: 5 messages (read_committed)
topic2: 5 messages (read_committed)
Expected 5 per topic. Aborted messages are NOT shown.
```

`5 messages per topic` is the pass condition: the committed transaction is
visible, the aborted one is not.

## Verification

Confirm from the CLI as well. A `read_committed` console consumer sees 5 per
topic; a `read_uncommitted` one sees 7 (5 committed + 2 aborted), which
proves the aborted records are on disk and only hidden by isolation level.

```bash
cd ~/kafka

# read_committed -> 5 per topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --command-config clients/admin.properties \
  --topic topic1 --from-beginning --timeout-ms 5000 2>/dev/null | grep -c committed
# -> 5

# read_uncommitted -> 7 (includes the 2 aborted)
printf 'isolation.level=read_uncommitted\n' >> /tmp/ru.properties
cat clients/admin.properties >> /tmp/ru.properties
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --command-config /tmp/ru.properties \
  --topic topic1 --from-beginning --timeout-ms 5000 2>/dev/null | grep -cE 'committed|aborted'
# -> 7
rm /tmp/ru.properties
```

Screenshots for submission: producer output (`COMMITTED`/`ABORTED`),
consumer output (5 per topic), and the read_committed vs read_uncommitted
CLI counts.

## How it works

```
  Producer (transactional.id = hw3-producer)

  beginTransaction()
    send x5 -> topic1        ┐
    send x5 -> topic2        │  transaction 1
  commitTransaction()        ┘  -> commit markers written, records visible

  beginTransaction()
    send x2 -> topic1        ┐
    send x2 -> topic2        │  transaction 2
  abortTransaction()         ┘  -> abort markers written, records hidden

  Consumer (isolation.level = read_committed)
    reads up to the Last Stable Offset, skips aborted records
    -> sees 5 per topic, never the 2 aborted
```

- **Commit** writes commit markers into each partition; `read_committed`
  delivers those records.
- **Abort** writes abort markers; the records stay in the log (they consume
  offsets) but `read_committed` skips them. Only `read_uncommitted` sees them.
- `transactional.id` implies idempotence and enables the transactional API.
  `initTransactions()` must be called once before the first transaction.

## Cleanup

```bash
# optional: remove the homework topics and ACLs
cd ~/kafka
for t in topic1 topic2; do
  bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --command-config clients/admin.properties --delete --topic "$t"
done

# stop instances between sessions
cd ~/otus-kafka && ./aws-lab.sh stop
```

## References

- KafkaProducer transactions: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
- Consumer isolation.level: https://kafka.apache.org/documentation/#consumerconfigs_isolation.level
- KIP-98 (transactions): https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
