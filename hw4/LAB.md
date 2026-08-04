# HW4 — Kafka Streams: counting events per key in a session window

Assignment: start Kafka, create the topic `events`, and build a Kafka Streams
application that counts events sharing the same key within a 5-minute session.
Messages are fed in by hand with the console producer.

Everything runs against the existing broker on the `kafka` EC2 (Apache Kafka
4.3.0, KRaft single-node, SASL/PLAIN).

## Why a session window

A tumbling or hopping window is defined before any data arrives — fixed edges
on the time axis, every record falls into whichever bucket its timestamp lands
in. A session window has no edges until the data supplies them. It grows for as
long as records for that key keep arriving within the inactivity gap, and it
ends when the gap elapses with silence.

Two properties follow, and both are visible in this lab:

- **Sessions merge.** A record that lands between two existing sessions for the
  same key joins them into one. The two old windows cease to exist and the
  aggregate is recomputed for the merged range.
- **Windows are per key, not global.** Key `a` can be mid-session while key `b`
  has been quiet for ten minutes.

## What you will build

- A Gradle Java project (`streams-sessions-java/`) on `kafka-streams` 4.0.0.
- `Ex1SessionCount` — emits the running count on every record.
- `Ex2FinalSessionCount` — emits one record per session, after it closes.
- Topics: `events` (input), `events-session-counts` and
  `events-session-counts-final` (output).
- A `client.properties` with `admin`'s SASL/PLAIN credentials (gitignored).

## Prerequisites

- The `kafka` EC2 running Apache Kafka 4.3.0 via `systemctl`, with the `admin`
  super-user.
- Gradle 8.8 on the EC2 (`/opt/gradle-8.8`).
- Repo cloned on the EC2 at `~/kafka-repo/`.

## Architecture

```
  local Mac                          kafka EC2 (172.31.29.117)
  ─────────                          ─────────────────────────
  edit *.java                        ┌──────────────────────────────────┐
       │ git push                    │ broker localhost:9092            │
       ▼                             │                                  │
  github.com/mtalvik/kafka           │  events ──────┐                  │
       │ git pull (EC2)              │               │                  │
       ▼                             │   ┌───────────▼───────────────┐  │
  ~/kafka-repo/hw4/                  │   │ Streams app               │  │
       │ gradle ex1 --no-daemon      │   │  groupByKey               │  │
       └────────────────────────────►│   │  SessionWindows(5 min)    │  │
                                     │   │  count()                  │  │
                                     │   └───────┬───────────┬───────┘  │
                                     │           │           │          │
                                     │  ...-changelog   events-session- │
                                     │  (internal)          counts      │
                                     └──────────────────────────────────┘
```

The changelog topic is created by Streams itself, not by you. That is where the
single-node trap lives — see Step 4.

---

## Step 1: Broker up

```bash
cd ~/otus-kafka
./aws-lab.sh start
./aws-lab.sh ssh kafka
```

```bash
sudo systemctl status kafka | head -3
```

Expected: `active (running)`.

## Step 2: Create the topics

`events` is already managed by Terraform in `lesson7/gitops` (3 partitions,
RF 1) — it is shared with lesson 15. Apply that first if you have not:

```bash
cd ~/kafka-repo/lesson7/gitops && terraform apply
```

The two output topics belong to this homework alone, so create them by hand:

```bash
cd ~/kafka
bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --create --if-not-exists --topic events-session-counts \
  --partitions 1 --replication-factor 1

bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --create --if-not-exists --topic events-session-counts-final \
  --partitions 1 --replication-factor 1
```

Three partitions on `events` is deliberate: keys hash to different partitions,
so the app is doing real per-key work rather than reading one ordered log.

## Step 3: Pull and configure

```bash
cd ~/kafka-repo && git pull
cd hw4/streams-sessions-java
cp client.properties.example client.properties
nano client.properties     # replace <PLACEHOLDER> with admin's password
```

Find admin's password:

```bash
grep '^   password=' ~/kafka/config/kafka_server_jaas.conf
```

```bash
export GRADLE_OPTS="-Xmx256m"
```

## Step 4: The replication factor trap

Streams keeps its session store fault-tolerant by mirroring it into a changelog
topic, `hw4-session-count-events-session-counts-store-changelog`. It creates
that topic on startup, and unless told otherwise it asks the broker for
`default.replication.factor` — which is 3. On a one-broker cluster the request
comes back `INVALID_REPLICATION_FACTOR` and the app dies before processing a
single record.

The fix is one line in the config, already in `Utils.streamsProps`:

```java
props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
```

Same failure mode as `__transaction_state` and `_schemas` in the earlier
lessons: any topic Kafka creates on your behalf inherits the broker default,
and the broker default assumes a real cluster.

## Step 5: Run the app

```bash
gradle ex1 --no-daemon
```

The topology description prints first. Confirm there is exactly one state store
and no repartition node — `groupByKey` on already-keyed records avoids the
repartition topic:

```
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [events])
      --> KSTREAM-PEEK-0000000001
    ...
    Processor: KSTREAM-AGGREGATE-0000000003 (stores: [events-session-counts-store])
```

Leave it running.

## Step 6: Feed events by hand

In a second SSH session:

```bash
cd ~/kafka
bin/kafka-console-producer.sh --bootstrap-server 172.31.29.117:9092 \
  --producer.config kafka-configs/clients/admin.properties \
  --topic events \
  --property parse.key=true --property key.separator=:
```

Type key-value pairs:

```
a:login
a:click
b:login
a:logout
b:click
```

The app prints a running count per key:

```
in   key=a value=login
out  key=a window=14:02:11..14:02:11 count=1
in   key=a value=click
out  key=a window=14:02:11..14:02:14 count=2
in   key=b value=login
out  key=b window=14:02:19..14:02:19 count=1
in   key=a value=logout
out  key=a window=14:02:11..14:02:25 count=3
```

Note the window end moving with each record: the session is growing. The start
stays put — that is the timestamp of the first record in the session.

## Step 7: Watch a merge

Sessions merge when a record bridges two of them. With a 5-minute gap that is
hard to stage by hand, so restart with a short gap:

```bash
gradle ex1 --no-daemon -Dgap.minutes=1
```

Then, in the producer: send `a:one`, wait ~90 seconds, send `a:two`. Two
separate sessions exist now. The merge itself needs a record whose timestamp
falls inside both gaps, which the console producer cannot backdate — so instead
observe the tombstone path directly by sending records close together and
watching the `drop` lines:

```
out  key=a window=14:10:02..14:10:02 count=1
drop key=a window=14:10:02..14:10:02 (session merged away)
out  key=a window=14:10:02..14:10:09 count=2
```

The `drop` line is a `null` value on the old windowed key. Any consumer of a
windowed aggregate has to handle it — the old window key is genuinely deleted,
not updated. This is why the output branch filters nulls before writing to
`events-session-counts`.

## Step 8: Read the result topic

Third session:

```bash
cd ~/kafka
bin/kafka-console-consumer.sh --bootstrap-server 172.31.29.117:9092 \
  --consumer.config kafka-configs/clients/admin.properties \
  --topic events-session-counts --from-beginning \
  --property print.key=true --property key.separator=' -> ' \
  --value-deserializer org.apache.kafka.common.serialization.LongDeserializer
```

```
a@1754308931000-1754308931000 -> 1
a@1754308931000-1754308934000 -> 2
b@1754308939000-1754308939000 -> 1
```

The windowed key is flattened to `key@start-end` so a plain console consumer
can read it. Keeping the native `Windowed<String>` key would require the
session-windowed serde on the consumer side.

## Step 9: Final counts only

```bash
gradle ex2 --no-daemon -Dgap.minutes=1
```

`Ex2FinalSessionCount` adds `suppress(untilWindowCloses(...))`. Nothing is
emitted until a session closes, and merges never reach the output at all.

The catch is worth internalising: stream time advances from record timestamps,
not from the wall clock. Send three records for key `a`, then stop. Nothing
prints — not after one minute, not after ten. The session closes only when a
record arrives with a timestamp past the deadline. Send `z:tick` two minutes
later and `a`'s final count appears immediately.

```
in   key=a value=one
in   key=a value=two
    (silence — suppress is holding the result)
in   key=z value=tick
final key=a window=14:20:03..14:20:07 count=2
```

This is the standard trap with suppression on a low-traffic topic. In
production the usual answers are a heartbeat producer or a custom
`Punctuator` on wall-clock time.

## Step 10: Reset between runs

Streams stores its progress in the consumer group and the changelog. To rerun
from a clean slate:

```bash
# stop the app first (Ctrl-C)
cd ~/kafka
bin/kafka-streams-application-reset.sh --bootstrap-server 172.31.29.117:9092 \
  --config-file kafka-configs/clients/admin.properties \
  --application-id hw4-session-count \
  --input-topics events
```

Then delete the local state directory:

```bash
rm -rf /tmp/kafka-streams/hw4-session-count
```

Confirm which internal topics the app created:

```bash
bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --list | grep hw4
```

Expected: one changelog topic per app, no repartition topic.

---

## Results

| Check | Expected |
|---|---|
| `events` created, 3 partitions, RF 1 | Step 2 |
| App starts without `INVALID_REPLICATION_FACTOR` | Step 4/5 |
| Count rises per key as records arrive | Step 6 |
| Window end extends, start stays fixed | Step 6 |
| Tombstone on session merge | Step 7 |
| Result topic readable with `LongDeserializer` | Step 8 |
| Suppressed variant emits only on window close | Step 9 |
| Exactly one internal changelog topic, no repartition | Step 10 |

## Notes

- The gap is 5 minutes by default, as the assignment specifies. `-Dgap.minutes=1`
  only shortens it for interactive testing; leave it off for the submitted run.
- `SessionWindows.ofInactivityGapWithNoGrace` replaces the deprecated
  `SessionWindows.with(...)`. The grace period is a separate concern from the
  gap: grace covers late-arriving records after the window would otherwise
  close, and defaults to 24 hours on the deprecated API — long enough to make a
  suppressed topology look broken.
- `cache.max.bytes.buffering` was removed in 4.0. The replacement is
  `statestore.cache.max.bytes`, set to 0 here so every update is visible.
- Running as `admin` sidesteps ACLs. A non-super-user principal would need
  Describe/Create on the internal topics plus Read on the consumer group, which
  is a separate exercise.
