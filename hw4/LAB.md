# HW4 — Kafka Streams: session-window event count

Count events sharing a key within a 5-minute session. Input fed by hand with
the console producer.

Runs against the `kafka` EC2: Apache Kafka 4.3.0, KRaft single-node,
SASL/PLAIN, principal `admin`.

## Implementation

`streams-sessions-java/`, Gradle, `kafka-streams` 4.0.0.

- `Ex1SessionCount` — running count, emitted on every record.
- `Ex2FinalSessionCount` — one record per session via
  `suppress(untilWindowCloses)`.

Topology:

```java
builder.stream("events", Consumed.with(Serdes.String(), Serdes.String()))
       .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
       .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
       .count(Materialized.as("events-session-counts-store"))
       .toStream()
```

`groupByKey`, not `groupBy` — the console producer already sets the key, so no
repartition topic is created. `WithNoGrace` because grace on a hand-fed topic
only delays the result; the deprecated `SessionWindows.with(...)` defaults to
24 hours and makes a suppressed topology look broken.

Windowed keys are flattened to `key@start-end` before the output topic so a
plain console consumer can read them.

Config (`Utils.streamsProps`):

| Setting | Reason |
|---|---|
| `replication.factor=1` | Streams creates its changelog itself and otherwise inherits the broker default of 3 → `INVALID_REPLICATION_FACTOR` on one node |
| `statestore.cache.max.bytes=0` | every update visible; `cache.max.bytes.buffering` was removed in 4.0 |
| `commit.interval.ms=1000` | results appear within a second |
| `auto.offset.reset=earliest` | reprocess on restart |

Topics: `events` (3 partitions, Terraform in `lesson7/gitops`, shared with
lesson 15), `events-session-counts`, `events-session-counts-final`.

## Run

```bash
# CloudShell
cd ~/otus-kafka && ./aws-lab.sh start && ./aws-lab.sh ssh kafka
```

```bash
# EC2 — topics
cd ~/kafka-repo/lesson7/gitops && terraform apply
cd ~/kafka
for t in events-session-counts events-session-counts-final; do
  bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
    --command-config kafka-configs/clients/admin.properties \
    --create --if-not-exists --topic $t --partitions 1 --replication-factor 1
done
```

```bash
# EC2 — app
cd ~/kafka-repo/hw4/streams-sessions-java
cp client.properties.example client.properties
sed -i "s|<PLACEHOLDER>|$(grep '^   password=' ~/kafka/config/kafka_server_jaas.conf | cut -d'"' -f2)|" client.properties
export GRADLE_OPTS="-Xmx256m"
gradle ex1 --no-daemon
```

```bash
# EC2, second shell — producer
cd ~/kafka
bin/kafka-console-producer.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --topic events --property parse.key=true --property key.separator=:
```

```
a:1
a:2
a:3
b:1
```

`-Dgap.minutes=1` shortens the gap for interactive testing; the submitted run
uses the default 5 minutes.

## Result

```
in   key=a value=1
out  key=a window=18:41:51..18:41:51 count=1
in   key=a value=2
drop key=a window=18:41:51..18:41:51 (session merged away)
out  key=a window=18:41:51..18:42:01 count=2
in   key=a value=3
drop key=a window=18:41:51..18:42:01 (session merged away)
out  key=a window=18:41:51..18:42:04 count=3
```

Window start is fixed at the first record; the end extends with each new one;
the count accumulates. `drop` is a tombstone — the absorbed window key is
deleted, not updated, so the output branch filters nulls before `to()`.

Output topic:

```bash
bin/kafka-console-consumer.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --topic events-session-counts --from-beginning \
  --formatter-property print.key=true \
  --formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
  --timeout-ms 5000
```

```
a@1785868911392-1785868911392   1
a@1785868911392-1785868921897   2
a@1785868911392-1785868924521   3
```

### Ex2 — final counts only

`suppress` holds every intermediate update and releases one record per session
once stream time passes the deadline. Stream time comes from record timestamps,
not the wall clock: stop producing and the last session never closes. Send any
record with a later timestamp and the held result appears at once.

## Reset

```bash
bin/kafka-streams-application-reset.sh --bootstrap-server 172.31.29.117:9092 \
  --config-file kafka-configs/clients/admin.properties \
  --application-id hw4-session-count --input-topics events
rm -rf /tmp/kafka-streams/hw4-session-count
```

Internal topics created by the app — one changelog, no repartition:

```bash
bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties --list | grep hw4
```
