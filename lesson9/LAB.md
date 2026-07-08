# Lesson 9 Lab — Consumer API hands-on

This lab demonstrates the Java Consumer client against the existing
broker on the `kafka` EC2 (Apache Kafka 4.3.0, KRaft single-node,
SASL/PLAIN) provisioned in hw2 and managed through Terraform in
lesson 7. **No new infrastructure is created** — the consumer reads
the `producer-lab` topic created in lesson 8, as the `bob` principal
who already has READ/DESCRIBE on it and READ on all groups.

Five small Java programs (`Ex1Consumer` through `Ex5Consumer`) each
illustrate one concept from `lecture.md`:

| Program | Concept |
|---|---|
| `Ex1Consumer` | Minimal consumer: `subscribe`, poll loop, auto-commit, own group |
| `Ex2Consumer` | Consumer group scaling — run several instances, watch assignment + rebalance |
| `Ex3Consumer` | Manual `commitSync()` with `enable.auto.commit=false` |
| `Ex4Consumer` | Hybrid commit: `commitAsync()` in the loop + `commitSync()` in `finally` |
| `Ex5Consumer` | `assign()` + `seekToBeginning()` — manual offset control / replay |

## What you will build

- A Gradle Java project (`consumer-java/`) using `kafka-clients` 3.7.0,
  mirroring the lesson 8 `producer-java` layout
- Five runnable consumer examples, each exposed as a Gradle task
  (`gradle ex1`, `ex2`, …)
- A `client.properties` file with bob's SASL/PLAIN credentials, kept
  out of git via `.gitignore`

No topic and no ACLs are created — everything reuses lesson 8's
`producer-lab` and bob's existing grants.

## Prerequisites

- A working hw2 / lesson 7 / lesson 8 setup: the `kafka` EC2 running
  Apache Kafka 4.3.0, with alice/bob/charlie SASL users in
  `kafka_server_jaas.conf`, and the `producer-lab` topic (3 partitions)
  present (managed by Terraform in `lesson7/gitops/`).
- Gradle 8.8 already installed on the EC2 from lesson 8
  (`/opt/gradle-8.8`). If a fresh shell does not find it:
  `source ~/.bashrc`.
- The lesson 8 `producer-java` project still configured (its
  `client.properties` with alice's password), so we can feed fresh
  messages into `producer-lab`.
- AWS CloudShell access with `./aws-lab.sh` and the EC2 SSH key.

## Architecture

```
  local Mac                           kafka EC2 (172.31.29.117)
  ─────────                           ──────────────────────────
  edit *.java, *.gradle               ┌──────────────────────┐
       │                              │ broker localhost:9092│
       │ git commit / push            │   SASL/PLAIN         │
       ▼                              │   StandardAuthorizer │
  github.com/mtalvik/kafka            │   topic producer-lab │
       │                              │   __consumer_offsets │
       │ git pull (on EC2)            └──────────▲───────────┘
       ▼                                         │
  ~/kafka-repo/lesson9/                          │ Consumer API
       │                              ┌──────────┴────────────┐
       │ gradle exN --no-daemon       │ java demo.ExNConsumer │
       └─────────────────────────────►│ kafka-clients 3.7.0   │
                                      │ principal: bob        │
                                      └───────────────────────┘
```

Client code runs on the `kafka` EC2 itself, connecting to its own
broker on `localhost:9092`. Committed offsets are stored in the
broker's internal `__consumer_offsets` topic.

## Configuration files explained

All files live under `lesson9/consumer-java/`. Same Gradle
`application` layout as lesson 8, with `Consumer` classes instead of
`Producer`.

### `consumer-java/settings.gradle`

```groovy
rootProject.name = 'consumer-java'
```

### `consumer-java/build.gradle`

Identical to lesson 8 except the task loop points at `ExNConsumer`:

```groovy
application {
    mainClass = 'demo.Ex1Consumer'
}

['ex1', 'ex2', 'ex3', 'ex4', 'ex5'].each { name ->
    def cls = 'demo.' + name.capitalize() + 'Consumer'
    tasks.register(name, JavaExec) {
        group = 'application'
        description = "Run ${cls}"
        classpath = sourceSets.main.runtimeClasspath
        mainClass = cls
        systemProperty 'client.properties.path', file('client.properties').absolutePath
    }
}
```

### `consumer-java/client.properties.example`

```properties
bootstrap.servers=localhost:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="bob" password="<PLACEHOLDER>";

topic=producer-lab
group.id=lesson9
```

Template for the real `client.properties`. Note the principal is now
**bob** (READ), not alice. The `group.id=lesson9` here is used by
`Ex2Consumer` (the scaling demo). `Ex1/Ex3/Ex4` set their own group id
in code so each reads `producer-lab` from the beginning independently;
`Ex5` assigns partitions manually and needs no group.

### `consumer-java/src/main/java/demo/Utils.java`

Mirror of the lesson 8 helper, with deserializers instead of
serializers and `auto.offset.reset=earliest`:

```java
final class Utils {

    static Properties baseConsumerConfig() {
        Properties props = new Properties();
        String path = System.getProperty("client.properties.path", "client.properties");
        try (FileInputStream in = new FileInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("failed to load " + path, e);
        }
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static Properties consumerConfig(Consumer<Properties> overrides) {
        Properties props = baseConsumerConfig();
        overrides.accept(props);
        return props;
    }

    static String topic() {
        return baseConsumerConfig().getProperty("topic", "producer-lab");
    }
}
```

`group.id` is read straight from `client.properties` (Kafka's own
property name), so it flows through automatically unless an example
overrides it.

### `Ex1Consumer.java` — minimal consumer

```java
var config = Utils.consumerConfig(p ->
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "lesson9-ex1"));

try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
    consumer.subscribe(List.of(topic));
    int emptyPolls = 0;
    while (emptyPolls < 5) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        if (records.isEmpty()) { emptyPolls++; continue; }
        emptyPolls = 0;
        for (ConsumerRecord<String, String> r : records) {
            System.out.printf("key=%s value=%s partition=%d offset=%d%n",
                    r.key(), r.value(), r.partition(), r.offset());
        }
    }
}
```

The simplest consumer: `subscribe`, poll, print, auto-commit (default
`true`). Uses its own group `lesson9-ex1` and `earliest`, so it always
reads the whole topic. Exits after five consecutive empty polls
(~2.5 s idle) instead of running forever — convenient for a lab step.

### `Ex2Consumer.java` — group scaling and rebalance

```java
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Utils.baseConsumerConfig());
Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("revoked: " + partitions);
    }
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("assigned: " + partitions);
    }
});

try {
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : records) {
            System.out.printf("key=%s value=%s partition=%d offset=%d%n",
                    r.key(), r.value(), r.partition(), r.offset());
        }
    }
} catch (WakeupException e) {
    // expected on Ctrl-C
} finally {
    consumer.close();
}
```

Uses the shared group `lesson9` from `client.properties`, so several
instances form one group. The `ConsumerRebalanceListener` prints the
partitions each instance owns. Runs forever; `wakeup()` from a
shutdown hook breaks `poll()` cleanly on Ctrl-C. This is the only
example meant to run as multiple processes at once.

### `Ex3Consumer.java` — manual `commitSync`

```java
var config = Utils.consumerConfig(p -> {
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "lesson9-ex3");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
});
// ... poll loop ...
consumer.commitSync();
System.out.println("committed batch (" + records.count() + " records)");
```

Auto-commit off, `commitSync()` after each processed batch. The commit
blocks until the broker acknowledges — "committed" always means
"already processed" → at-least-once.

### `Ex4Consumer.java` — hybrid commit

```java
// in the loop:
consumer.commitAsync((offsets, exception) -> {
    if (exception != null) System.out.println("async commit failed: " + exception.getMessage());
    else System.out.println("async committed: " + offsets);
});
// on the way out:
} finally {
    try { consumer.commitSync(); System.out.println("final sync commit done"); }
    finally { consumer.close(); }
}
```

`commitAsync()` on the hot path (fast, non-blocking, with a callback),
`commitSync()` in `finally` so the final offsets are guaranteed before
close. The production idiom from §6 of the lecture.

### `Ex5Consumer.java` — manual assignment and replay

```java
TopicPartition p0 = new TopicPartition(topic, 0);
consumer.assign(List.of(p0));
consumer.seekToBeginning(List.of(p0));
// ... poll loop prints partition=0 records from offset 0 ...
```

No `subscribe()`, no group coordination: `assign()` one partition
directly, then `seekToBeginning()` to replay it from offset 0 every
run, regardless of any committed offset. Shows that an offset is just a
position in a partition.

## Step 1: Verify the broker is running

```bash
cd ~/otus-kafka
./aws-lab.sh start
./aws-lab.sh ssh kafka
```

On the `kafka` EC2:

```bash
sudo systemctl status kafka | head -3
```

Expected: `active (running)`. Confirm `producer-lab` still exists:

```bash
cd ~/kafka
bin/kafka-topics.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --describe --topic producer-lab | head -1
```

Expected: a line showing `Topic: producer-lab  ...  PartitionCount: 3`.
If it is missing, re-apply the lesson 7 Terraform:

```bash
cd ~/kafka-repo/lesson7/gitops
terraform apply
```

## Step 2: Pull the repo and check the project

```bash
cd ~/kafka-repo
git pull
cd lesson9/consumer-java
ls -R
```

Expected: `build.gradle`, `settings.gradle`, `.gitignore`,
`client.properties.example`, and `src/main/java/demo/` with `Utils`
and `Ex1..Ex5Consumer`. Gradle 8.8 is already on the PATH from
lesson 8 (`gradle --version` → `8.8`).

## Step 3: Put fresh messages into `producer-lab`

The consumers read what the lesson 8 producer wrote. To have known,
fresh data (including keyed records for the partitioning view), run
the lesson 8 producer as alice:

```bash
cd ~/kafka-repo/lesson8/producer-java
gradle ex1 --no-daemon    # 5 no-key messages
gradle ex4 --no-daemon    # 10 keyed messages (order-42, order-99)
```

If `producer-java/client.properties` is missing, recreate it from
lesson 8 Step 4 (alice's password). Leave this data in place for the
rest of the lab.

## Step 4: Configure `client.properties`

```bash
cd ~/kafka-repo/lesson9/consumer-java
cp client.properties.example client.properties
nano client.properties
```

Replace `<PLACEHOLDER>` with **bob's** password from the JAAS file:

```bash
grep user_bob ~/kafka/config/kafka_server_jaas.conf
```

`client.properties` is gitignored.

## Step 5: Run Ex1 — minimal consumer

```bash
cd ~/kafka-repo/lesson9/consumer-java
export GRADLE_OPTS="-Xmx256m"
gradle ex1 --no-daemon
```

Expected output (order across partitions may vary):

```
> Task :ex1
key=null value=ex1 message 0 partition=2 offset=0
key=null value=ex1 message 1 partition=2 offset=1
...
key=order-42 value=ex4 order-42 message 0 partition=1 offset=0
...
key=order-99 value=ex4 order-99 message 0 partition=2 offset=5
...

BUILD SUCCESSFUL
```

One consumer reads all three partitions, prints every record, then
exits after the topic is drained. Run it a second time: it resumes
from the committed offset (auto-commit saved it), so it reads
**nothing** and exits after the idle timeout. That is the
committed-offset mechanism working. To force a full re-read, use a new
group id or reset offsets (Step 10).

## Step 6: Run Ex2 — consumer group scaling and rebalance

This step needs **two or three SSH sessions** to the `kafka` EC2, each
running `gradle ex2`. They share `group.id=lesson9`, so the three
partitions get divided among them.

Session A:

```bash
cd ~/kafka-repo/lesson9/consumer-java
gradle ex2 --no-daemon
```

Expected in A (it gets all three partitions initially):

```
assigned: [producer-lab-0, producer-lab-1, producer-lab-2]
```

Now start session B (second SSH), same command. Watch both:

```
# session A
revoked: [producer-lab-0, producer-lab-1, producer-lab-2]
assigned: [producer-lab-0, producer-lab-1]
# session B
assigned: [producer-lab-2]
```

Start session C — the three partitions spread one per consumer:

```
# each session ends up with one of:
assigned: [producer-lab-0]
assigned: [producer-lab-1]
assigned: [producer-lab-2]
```

Now Ctrl-C session C. Its partition is reassigned to a survivor:

```
# a surviving session
revoked: [...]
assigned: [producer-lab-1, producer-lab-2]
```

That reassignment on join and on exit is the **rebalance**. To watch
live consumption during scaling, run the lesson 8 producer in a fourth
shell while the consumers are up:

```bash
cd ~/kafka-repo/lesson8/producer-java
gradle ex4 --no-daemon
```

The records appear in whichever session owns the target partition.
Stop all `ex2` sessions with Ctrl-C when done (each closes cleanly via
`wakeup()`).

## Step 7: Run Ex3 — manual commitSync

```bash
gradle ex3 --no-daemon
```

Expected:

```
key=... partition=0 offset=...
... (a batch)
committed batch (N records)
...
BUILD SUCCESSFUL
```

Group `lesson9-ex3`, auto-commit off. Each `committed batch` line
appears after `commitSync()` returns — a blocking, acknowledged
offset write.

## Step 8: Run Ex4 — commitAsync hybrid

```bash
gradle ex4 --no-daemon
```

Expected:

```
key=... partition=1 offset=...
async committed: {producer-lab-1=OffsetAndMetadata{offset=...}}
...
final sync commit done
BUILD SUCCESSFUL
```

Group `lesson9-ex4`. `async committed:` lines come from the callback
during the loop; `final sync commit done` is the blocking commit in
`finally`.

## Step 9: Run Ex5 — manual assignment and replay

```bash
gradle ex5 --no-daemon
```

Expected — only partition 0, always from offset 0:

```
partition=0 offset=0 value=ex1 message 0
partition=0 offset=1 value=...
...
BUILD SUCCESSFUL
```

Run it a second time — identical output. Because it uses `assign()` +
`seekToBeginning()` and never commits, it replays partition 0 from the
start every time, ignoring committed offsets.

## Step 10: Inspect consumer groups and offsets

List the groups the lab created:

```bash
~/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config ~/kafka/kafka-configs/clients/admin.properties \
  --list
```

Expected: `lesson9`, `lesson9-ex1`, `lesson9-ex3`, `lesson9-ex4`
(Ex5 uses no group).

Describe one to see committed offset, log-end offset, and lag per
partition:

```bash
~/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config ~/kafka/kafka-configs/clients/admin.properties \
  --describe --group lesson9-ex1
```

Expected (LAG 0 after a full drain):

```
GROUP       TOPIC        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
lesson9-ex1 producer-lab 0          5               5               0
lesson9-ex1 producer-lab 1          5               5               0
lesson9-ex1 producer-lab 2          5               5               0
```

`CURRENT-OFFSET` is the committed offset; `LOG-END-OFFSET − CURRENT-OFFSET`
is the lag. This is the standard consumer health check.

To force Ex1 to re-read on the next run, reset its committed offsets
(only works while no member of the group is active):

```bash
~/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config ~/kafka/kafka-configs/clients/admin.properties \
  --reset-offsets --to-earliest --group lesson9-ex1 \
  --topic producer-lab --execute
```

## What was demonstrated

| Concept | Ex | Observable behavior |
|---|---|---|
| One consumer reads all partitions | Ex1 | records from partitions 0, 1, 2 in one process |
| Committed offset resumes reading | Ex1 | a second run reads nothing (starts from committed) |
| Group divides partitions | Ex2 | 3 instances end with one partition each |
| Rebalance on join/leave | Ex2 | `assigned:` / `revoked:` lines when instances start/stop |
| Manual `commitSync` | Ex3 | `committed batch` after each blocking commit |
| Hybrid `commitAsync` + `commitSync` | Ex4 | callback lines in loop, `final sync commit done` at end |
| Offset is a position; replay | Ex5 | partition 0 re-read from offset 0 every run |
| Committed offset and lag | Step 10 | `kafka-consumer-groups.sh --describe` shows CURRENT/LOG-END/LAG |

## Repository layout

```
lesson9/
├── lecture.md             — Consumer API concepts and internals
├── LAB.md                 — this file
└── consumer-java/
    ├── build.gradle       — Gradle project with one task per Ex
    ├── settings.gradle    — Gradle project name
    ├── .gitignore         — excludes client.properties and build artifacts
    ├── client.properties.example  — template (bob), real file gitignored
    └── src/main/java/demo/
        ├── Utils.java           — shared config loader (deserializers, earliest)
        ├── Ex1Consumer.java     — minimal consumer, own group
        ├── Ex2Consumer.java     — group scaling + rebalance listener
        ├── Ex3Consumer.java     — manual commitSync
        ├── Ex4Consumer.java     — commitAsync + sync-in-finally
        └── Ex5Consumer.java     — assign + seekToBeginning replay
```

## Cleanup

No topic or ACLs to remove — `producer-lab` is shared with lesson 8.
Delete only the consumer groups this lab created:

```bash
for g in lesson9 lesson9-ex1 lesson9-ex3 lesson9-ex4; do
  ~/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server 172.31.29.117:9092 \
    --command-config ~/kafka/kafka-configs/clients/admin.properties \
    --delete --group "$g"
done
```

(A group can only be deleted when it has no active members — stop all
`exN` processes first.)

Stop the EC2 instances when not actively working:

```bash
cd ~/otus-kafka
./aws-lab.sh stop
```

## Reference questions

After running the lab, you should be able to answer:

1. In Ex2 with 3 partitions, you start a **fourth** `ex2` instance.
   What does its `assigned:` line show, and why? What happens when you
   then stop one of the other three?
2. Why does a second run of Ex1 read nothing, while a second run of
   Ex5 reads everything again? Name the mechanism that differs.
3. Ex3 crashes (kill -9) after `poll()` but before `commitSync()`.
   When restarted, which records does it reprocess? Which delivery
   guarantee is this?
4. In Ex4, why is `commitSync()` in `finally` needed if `commitAsync()`
   already ran in the loop? What is lost if you drop it?
5. Change `Ex1Consumer`'s group id to match Ex3 (`lesson9-ex3`) and
   run Ex1 then Ex3. Does Ex3 still see records? Explain using the
   committed offset.
6. If bob's READ ACL on `producer-lab` were removed, which exception
   surfaces and at which call — `subscribe()`, `poll()`, or `commitSync()`?
