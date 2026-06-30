# Lesson 8 Lab — Producer API hands-on

This lab demonstrates the Java Producer client against the existing
broker on the `kafka` EC2 (Apache Kafka 4.3.0, KRaft single-node,
SASL/PLAIN) provisioned in hw2 and managed through Terraform in
lesson 7. No new infrastructure is created.

Five small Java programs (`Ex1Producer` through `Ex5Producer`) each
illustrate one concept from `LECTURE.md`:

| Program | Concept |
|---|---|
| `Ex1Producer` | Minimal synchronous producer: `Properties`, `send().get()`, `close()` |
| `Ex2Producer` | Async send with callback and `linger.ms=500` — observe when callbacks fire |
| `Ex3Producer` | `flush()` mid-loop — see the batching boundary |
| `Ex4Producer` | Key-based partitioning — same key always lands in the same partition |
| `Ex5Producer` | `acks=0` vs `acks=1` vs `acks=all` timing on the single-node broker |

## What you will build

- A Gradle Java project (`producer-java/`) using `kafka-clients` 3.7.0
- A new topic `producer-lab` with three partitions and ACLs for alice
  (write/describe) and bob (read/describe)
- Five runnable producer examples, each exposed as a Gradle task
  (`./gradlew ex1`, `ex2`, …)
- A `client.properties` file with alice's SASL/PLAIN credentials,
  kept out of git via `.gitignore`

## Prerequisites

- A working hw2 / lesson 7 setup: the `kafka` EC2 instance running
  Apache Kafka 4.3.0 via `systemctl`, with alice/bob/charlie SASL
  users defined in `kafka_server_jaas.conf`.
- AWS CloudShell access with `./aws-lab.sh` and the EC2 SSH key.
- Git repository cloned on both Mac and `kafka` EC2 at
  `~/kafka-repo/` (the path used in lesson 7).

## Architecture

```
  local Mac                           kafka EC2 (172.31.29.117)
  ─────────                           ──────────────────────────
  edit *.java, *.gradle               ┌──────────────────────┐
       │                              │ broker localhost:9092│
       │ git commit / push            │   SASL/PLAIN         │
       ▼                              │   StandardAuthorizer │
  github.com/mtalvik/kafka            │   topic producer-lab │
       │                              └──────────▲───────────┘
       │ git pull (on EC2)                       │
       ▼                                         │ Producer API
  ~/kafka-repo/lesson8/                ┌─────────┴─────────────┐
       │                               │ java demo.ExNProducer │
       │ ./gradlew ex1 --no-daemon     │ kafka-clients 3.7.0   │
       └──────────────────────────────►│ principal: alice      │
                                       └───────────────────────┘
```

Client code runs on the `kafka` EC2 itself, connecting to its own
broker on `localhost:9092`. There is no cross-EC2 networking. Gradle
builds the project on the EC2 and runs each `ExNProducer` directly.

## Configuration files explained

All files live under `lesson8/producer-java/`. The structure is a
standard Gradle Java project with the `application` plugin.

### `producer-java/settings.gradle`

```groovy
rootProject.name = 'producer-java'
```

Declares the project name. Required for any Gradle project.

### `producer-java/build.gradle`

```groovy
plugins {
    id 'application'
    id 'java'
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories { mavenCentral() }

dependencies {
    implementation 'org.apache.kafka:kafka-clients:3.7.0'
    implementation 'org.slf4j:slf4j-simple:2.0.13'
}

application {
    mainClass = 'demo.Ex1Producer'
}

['ex1', 'ex2', 'ex3', 'ex4', 'ex5'].each { name ->
    def cls = 'demo.' + name.capitalize() + 'Producer'
    tasks.register(name, JavaExec) {
        group = 'application'
        description = "Run ${cls}"
        classpath = sourceSets.main.runtimeClasspath
        mainClass = cls
        systemProperty 'client.properties.path', file('client.properties').absolutePath
    }
}
```

Two things to note:

- `kafka-clients:3.7.0` is the client library. The broker on EC2 runs
  4.3.0. The wire protocol is compatible across these versions —
  the client speaks a slightly older dialect but the broker
  understands it.
- The loop at the bottom registers five Gradle tasks (`ex1` … `ex5`),
  one per `ExNProducer` class. Each passes the absolute path of
  `client.properties` to the JVM as a system property, so the program
  can find its config regardless of working directory.

### `producer-java/.gitignore`

```
.gradle/
build/
client.properties
*.iml
.idea/
```

`client.properties` (with the real password) must never be committed.
Only the `.example` template is in git. `.gradle/` and `build/` are
build artifacts.

### `producer-java/client.properties.example`

```properties
bootstrap.servers=localhost:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="<PLACEHOLDER>";

topic=producer-lab
```

Template for the real `client.properties`. After `git pull` on the
EC2, copy this file to `client.properties` and replace
`<PLACEHOLDER>` with alice's password from
`~/kafka/config/kafka_server_jaas.conf`.

The `topic=producer-lab` entry is read by `Utils.topic()` so all the
`ExNProducer` classes write to the same place without hardcoding.

### `producer-java/src/main/java/demo/Utils.java`

```java
final class Utils {

    static Properties baseProducerConfig() {
        Properties props = new Properties();
        String path = System.getProperty("client.properties.path", "client.properties");
        try (FileInputStream in = new FileInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("failed to load " + path, e);
        }
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    static Properties producerConfig(Consumer<Properties> overrides) {
        Properties props = baseProducerConfig();
        overrides.accept(props);
        return props;
    }

    static String topic() {
        return baseProducerConfig().getProperty("topic", "producer-lab");
    }
}
```

Shared helper used by every `ExNProducer`. Loads `client.properties`
(bootstrap servers, SASL config, topic name) and adds the
serializers. The `producerConfig(Consumer)` form lets each example
add or override specific settings without duplicating the connection
configuration.

The `client.properties.path` system property is set by Gradle, so the
file is found regardless of the JVM's working directory.

### `producer-java/src/main/java/demo/Ex1Producer.java`

```java
public class Ex1Producer {
    public static void main(String[] args) {
        String topic = Utils.topic();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.baseProducerConfig())) {
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, null, "ex1 message " + i);
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("sent partition=%d offset=%d%n",
                        metadata.partition(), metadata.offset());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
```

The simplest possible producer. Five records with no key, sent
synchronously (`.get()` blocks until the broker acks). Demonstrates
the minimum code path: build producer, send, close.

Because the key is `null`, the default partitioner (uniform sticky)
decides where each record lands.

### `producer-java/src/main/java/demo/Ex2Producer.java`

```java
public class Ex2Producer {
    public static void main(String[] args) {
        String topic = Utils.topic();
        var props = Utils.producerConfig(p -> p.put(ProducerConfig.LINGER_MS_CONFIG, "500"));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                final int idx = i;
                System.out.printf("%s send %d%n", LocalTime.now(), idx);
                producer.send(
                    new ProducerRecord<>(topic, null, "ex2 message " + idx),
                    (metadata, exception) -> {
                        if (exception != null) {
                            exception.printStackTrace();
                        } else {
                            System.out.printf("%s callback %d partition=%d offset=%d%n",
                                LocalTime.now(), idx,
                                metadata.partition(), metadata.offset());
                        }
                    });
            }
            System.out.printf("%s loop done, waiting on close()%n", LocalTime.now());
        }
        System.out.printf("%s producer closed%n", LocalTime.now());
    }
}
```

Async send with a callback, and `linger.ms=500`. The producer
deliberately waits up to 500 ms before shipping a batch.

Every line is timestamped with `LocalTime.now()`, so the output makes
the asynchronous behavior visible: the `send` lines run immediately,
the `callback` lines arrive in a burst about 500 ms later when the
Sender thread finally ships the batch.

### `producer-java/src/main/java/demo/Ex3Producer.java`

```java
public class Ex3Producer {
    public static void main(String[] args) {
        String topic = Utils.topic();
        var props = Utils.producerConfig(p -> p.put(ProducerConfig.LINGER_MS_CONFIG, "200"));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 200; i++) {
                final int idx = i;
                producer.send(
                    new ProducerRecord<>(topic, null, "ex3 message " + idx),
                    (metadata, exception) -> {
                        if (exception != null) exception.printStackTrace();
                        else System.out.printf("callback %d partition=%d offset=%d%n",
                                idx, metadata.partition(), metadata.offset());
                    });

                if ((i + 1) % 50 == 0) {
                    System.out.printf("===== FLUSH at i=%d =====%n", i);
                    producer.flush();
                    System.out.printf("===== FLUSH done at i=%d =====%n", i);
                }
            }
        }
    }
}
```

200 records sent asynchronously, with a `flush()` every 50. The
output shows the flush markers and demonstrates that immediately
after each `===== FLUSH done =====`, all preceding callbacks have
fired. Without `flush()`, callbacks would arrive in a single burst at
the end when `close()` runs.

### `producer-java/src/main/java/demo/Ex4Producer.java`

```java
public class Ex4Producer {
    public static void main(String[] args) {
        String topic = Utils.topic();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.baseProducerConfig())) {
            for (String key : new String[] {"order-42", "order-99"}) {
                for (int i = 0; i < 5; i++) {
                    ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, key, "ex4 " + key + " message " + i);
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("sent key=%s partition=%d offset=%d%n",
                            key, metadata.partition(), metadata.offset());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
```

Ten records: five with `key="order-42"`, five with `key="order-99"`.
The output shows that all `order-42` records land in the same
partition and all `order-99` records land in another (or possibly the
same — `murmur2("order-42") mod 3` and `murmur2("order-99") mod 3` can
collide). Run the program twice — partition numbers are stable across
runs.

### `producer-java/src/main/java/demo/Ex5Producer.java`

```java
public class Ex5Producer {
    private static final int N = 100_000;
    private static final String PAYLOAD;  // ~200 bytes of repeated text

    public static void main(String[] args) {
        timeAcks("0");
        timeAcks("1");
        timeAcks("all");
    }

    private static void timeAcks(String acks) {
        String topic = Utils.topic();
        var props = Utils.producerConfig(p -> {
            p.put(ProducerConfig.ACKS_CONFIG, acks);
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        });

        long start = System.currentTimeMillis();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < N; i++) {
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), PAYLOAD));
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("acks=%s sent=%d elapsed=%d ms throughput=%.0f msg/s%n",
                acks, N, elapsed, N * 1000.0 / elapsed);
    }
}
```

Sends 100,000 records three times with `acks=0`, `acks=1`, and
`acks=all`, printing elapsed time and throughput for each.

`enable.idempotence=false` is set explicitly because idempotence
requires `acks=all` and would prevent the comparison from running.

On the single-node broker (replication factor 1, ISR = {leader}),
`acks=all` collapses to `acks=1`, so the three timings end up within
~20% of each other. This is the trap described in §4 of the lecture:
to see real `acks=all` overhead, you would need a multi-broker cluster
with replication factor ≥ 2.

## Step 1: Verify the broker is running

If the EC2 instances were stopped (e.g. after `./aws-lab.sh stop`),
start them and SSH into the broker:

```bash
cd ~/otus-kafka
./aws-lab.sh start
./aws-lab.sh ssh kafka
```

On the `kafka` EC2:

```bash
sudo systemctl status kafka
```

Expected: `active (running)`. If the broker is stopped,
`sudo systemctl start kafka`.

Confirm authentication works using the admin client config from
lesson 7:

```bash
cd ~/kafka
bin/kafka-broker-api-versions.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties | head -1
```

Expected: `172.31.29.117:9092 (id: 1 rack: null isFenced: false) -> (`.

## Step 2: Pull the repo and install Gradle

```bash
cd ~/kafka-repo
git pull
```

Gradle is not installed on the EC2 by default. Install it once:

```bash
cd /tmp
curl -sL https://services.gradle.org/distributions/gradle-8.8-bin.zip -o gradle.zip
sudo unzip -q gradle.zip -d /opt
echo 'export PATH=/opt/gradle-8.8/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
gradle --version
```

Expected: `Gradle 8.8`.

## Step 3: Create the `producer-lab` topic and grant ACLs

The topic must have multiple partitions so the partitioner demos in
Ex1, Ex2, and Ex4 are visible. Three partitions is enough.

```bash
cd ~/kafka

bin/kafka-topics.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --create \
  --topic producer-lab \
  --partitions 3 \
  --replication-factor 1
```

Expected: `Created topic producer-lab.`

Grant alice WRITE and DESCRIBE on the topic so the producer can send
and discover metadata:

```bash
bin/kafka-acls.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --add \
  --allow-principal User:alice \
  --operation Write \
  --operation Describe \
  --topic producer-lab
```

Grant bob READ and DESCRIBE so step 10 can verify with the console
consumer:

```bash
bin/kafka-acls.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --add \
  --allow-principal User:bob \
  --operation Read \
  --operation Describe \
  --topic producer-lab
```

(Bob already has READ on Group `*` from lesson 7, so no additional
group ACL is needed.)

Alternative: add `producer-lab` and its ACLs to `lesson7/gitops/`
Terraform and run `terraform apply`. Either approach is valid.

## Step 4: Configure `client.properties`

```bash
cd ~/kafka-repo/lesson8/producer-java
cp client.properties.example client.properties
nano client.properties
```

Replace `<PLACEHOLDER>` with alice's password. Find it in the JAAS
file on the broker:

```bash
grep user_alice ~/kafka/config/kafka_server_jaas.conf
```

`client.properties` is gitignored, so the real password never reaches
git.

## Step 5: Run Ex1 — minimal send

```bash
cd ~/kafka-repo/lesson8/producer-java
export GRADLE_OPTS="-Xmx256m"
gradle ex1 --no-daemon
```

The first run downloads `kafka-clients` and dependencies (~20 MiB).
Subsequent runs reuse the local cache.

Expected output:

```
> Task :ex1
sent partition=2 offset=0
sent partition=2 offset=1
sent partition=2 offset=2
sent partition=2 offset=3
sent partition=2 offset=4

BUILD SUCCESSFUL
```

All five records typically land in the same partition because the
sticky partitioner picks one partition per batch and five records
easily fit in one batch.

## Step 6: Run Ex2 — async send with `linger.ms=500`

```bash
gradle ex2 --no-daemon
```

Expected output shape (timestamps will differ):

```
12:34:56.001 send 0
12:34:56.002 send 1
12:34:56.003 send 2
... (all 10 sends within a few ms)
12:34:56.012 send 9
12:34:56.013 loop done, waiting on close()
12:34:56.515 callback 0 partition=1 offset=0
12:34:56.515 callback 1 partition=1 offset=1
... (all 10 callbacks within the same millisecond)
12:34:56.520 producer closed
```

Observe:

- The `send` lines complete in ~10 ms, well before any callback fires.
- The `callback` lines all arrive in a burst about 500 ms later,
  exactly when `linger.ms` expires.
- `loop done` prints before any callback — this proves `send()` is
  non-blocking.

## Step 7: Run Ex3 — flush() inside a loop

```bash
gradle ex3 --no-daemon
```

The output is verbose (200 records × 1 callback each, plus 4 flush
markers). Pipe through `grep` to see the structure:

```bash
gradle ex3 --no-daemon 2>&1 | grep -E '(FLUSH|callback (0|49|50|99|100|149|150|199))'
```

Expected:

```
callback 0 ...
... (up to callback 49, all delivered)
===== FLUSH at i=49 =====
===== FLUSH done at i=49 =====
callback 50 ...
... (up to callback 99)
===== FLUSH at i=99 =====
===== FLUSH done at i=99 =====
...
```

Every `FLUSH done` appears only after all preceding callbacks have
fired. `flush()` is the synchronous "drain everything" barrier.

## Step 8: Run Ex4 — key-based partitioning

```bash
gradle ex4 --no-daemon
```

Expected output (exact partition numbers depend on `murmur2`):

```
sent key=order-42 partition=1 offset=0
sent key=order-42 partition=1 offset=1
sent key=order-42 partition=1 offset=2
sent key=order-42 partition=1 offset=3
sent key=order-42 partition=1 offset=4
sent key=order-99 partition=2 offset=0
sent key=order-99 partition=2 offset=1
sent key=order-99 partition=2 offset=2
sent key=order-99 partition=2 offset=3
sent key=order-99 partition=2 offset=4
```

All five `order-42` records land in the same partition. All five
`order-99` records land in the same partition. Run the program a
second time — the partitions assigned to each key are identical to
the first run. This is the property the lecture describes: same key
always lands in the same partition.

## Step 9: Run Ex5 — acks throughput comparison

```bash
gradle ex5 --no-daemon
```

Expected output (approximate numbers on a t3.micro):

```
acks=0   sent=100000 elapsed=2100 ms throughput=47619 msg/s
acks=1   sent=100000 elapsed=2400 ms throughput=41667 msg/s
acks=all sent=100000 elapsed=2500 ms throughput=40000 msg/s
```

On the single-node broker, `acks=all` and `acks=1` differ by only
~5%, because ISR contains just the leader. To see a real difference
the cluster would need `replication.factor=3` and
`min.insync.replicas=2`, which is out of scope for this lab.

`acks=0` is consistently the fastest because the producer never waits
for a response — it just streams records into the TCP socket as fast
as the OS will accept them.

## Step 10: Verify with the console consumer

In a second SSH session on the `kafka` EC2:

```bash
cat > /tmp/bob.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="bob" password="$(grep user_bob ~/kafka/config/kafka_server_jaas.conf | cut -d'"' -f2)";
EOF

~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --consumer.config /tmp/bob.properties \
  --topic producer-lab \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --max-messages 20
```

Expected: a stream of records from all four Ex runs (Ex1–Ex4),
showing key, partition, offset, and value. Verify that records with
key=`order-42` all share the same partition.

`rm /tmp/bob.properties` after verifying.

You can also view the topic in Kafbat UI on the `elastic` EC2 — the
hw2 setup includes it on port 8080.

## What was demonstrated

| Concept | Ex | Observable behavior |
|---|---|---|
| `send()` is asynchronous | Ex2 | `send` lines print immediately, callbacks fire ~500 ms later |
| `linger.ms` controls batching delay | Ex2 | the 500 ms gap is visible in timestamps |
| `flush()` is a synchronous barrier | Ex3 | every `FLUSH done` arrives only after all preceding callbacks |
| `close()` does an implicit `flush()` | Ex2 | the final callbacks arrive before "producer closed" |
| Same key → same partition | Ex4 | both runs of `Ex4` send `order-42` to the same partition |
| Default partitioner is sticky | Ex1 | five no-key records all land in one partition |
| `acks=all` is meaningless on RF=1 | Ex5 | `acks=all` and `acks=1` show ~5% difference |
| `acks=0` is fastest | Ex5 | no broker round-trip per request |

## Repository layout

```
lesson8/
├── LECTURE.md             — Producer API concepts and internals
├── LAB.md                 — this file
└── producer-java/
    ├── build.gradle       — Gradle project with one task per Ex
    ├── settings.gradle    — Gradle project name
    ├── .gitignore         — excludes client.properties and build artifacts
    ├── client.properties.example  — template, real file gitignored
    └── src/main/java/demo/
        ├── Utils.java           — shared config loader
        ├── Ex1Producer.java     — minimal sync send
        ├── Ex2Producer.java     — callback + linger.ms=500
        ├── Ex3Producer.java     — flush() mid-loop
        ├── Ex4Producer.java     — key-based partitioning
        └── Ex5Producer.java     — acks 0/1/all timing
```

## Cleanup

```bash
cd ~/kafka

bin/kafka-topics.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --delete --topic producer-lab

bin/kafka-acls.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --remove --allow-principal User:alice \
  --operation Write --operation Describe --topic producer-lab

bin/kafka-acls.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --remove --allow-principal User:bob \
  --operation Read --operation Describe --topic producer-lab
```

Stop the EC2 instances when not actively working to avoid charges:

```bash
cd ~/otus-kafka
./aws-lab.sh stop
```

## Reference questions

After running the lab, you should be able to answer:

1. In Ex4, if every key is changed to `null`, how does the partition
   distribution change? Predict the output, then test.
2. In Ex3, if you remove the four `flush()` calls, where do the 200
   callbacks fire? Why?
3. In Ex5, why is `acks=all` not noticeably slower than `acks=1` on
   this setup? What would the result look like with
   `replication.factor=3` and `min.insync.replicas=2`?
4. What does `enable.idempotence=true` (default in Ex1–Ex4) actually
   guarantee in this single-node setup with replication factor 1?
5. If alice's WRITE ACL on `producer-lab` is removed, which exception
   does Ex1 throw? Where does it surface — in `send()`, in `.get()`,
   in `close()`?
