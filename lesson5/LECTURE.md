# Lecture 5: Kafka Cluster Monitoring

**Course:** OTUS Apache Kafka
**Duration:** ~60 minutes of reading + the hands-on lab
**Level:** Intermediate — Kafka architecture (brokers, topics, partitions, ISR, replication) is assumed

---

## 🎯 Learning outcomes

After this lecture you will be able to:

- Explain how Kafka monitoring differs from monitoring a regular application
- Distinguish four independent observation angles: brokers, producers, consumers, ZooKeeper
- Choose metrics for **alerts** separately from metrics for **graphs**
- Set up the `JMX → JMX Exporter → Prometheus → Grafana` pipeline and explain what each link is for
- Read `OfflinePartitionsCount`, `UnderReplicatedPartitions`, `consumer lag` and know what to do when they are non-zero
- Compare specialized UI systems (AKHQ, Kafdrop, Kpow) and pick the right one for the job

---

## 1. Why monitoring Kafka isn't optional

Kafka rarely stands alone. It sits in the middle: producers from dozens of services write events, consumers read them to build analytics, update caches, send push notifications, or process payments.

A concrete example. Bolt coordinates thousands of drivers through events — order created, driver assigned, ride completed. If the Kafka cluster slows down for a minute, the driver doesn't get the order, the passenger opens Uber instead of Bolt. Money is moving in real time.

Or Wise: every transfer is a chain of events between services. If the `payments-processor` consumer falls half an hour behind, the customer sees "transfer in progress" and writes to support. Support says "all good, please wait". Two hours later the customer leaves for TransferGo.

Monitoring Kafka isn't a "nice to have". It's a tool that answers three questions:

1. **Is the cluster alive right now?** — alerts on critical metrics
2. **Is the trend good or bad?** — graphs you check once a day
3. **What happened yesterday at 03:42 when it broke?** — historical data for incident review

These three jobs are solved by different tools, and it's important not to mix them up.

---

## 2. Four independent observation angles

Kafka is a distributed system, and you have to watch it from all sides. Watching only the broker isn't enough. Watching only consumer lag isn't enough. You need all four metric sources at once:

**Broker** — the heart of the cluster. Topics live here, replication happens, partition leaders are elected. Broker metrics split into two groups: those characterizing **the Kafka process itself** (Kafka metrics via JMX), and those characterizing **the operating system underneath** (CPU, RAM, page cache, disk, network).

**Producer** — the client application writing to Kafka. Producer metrics live **inside the producer's JVM**, not on the broker. This is important to grasp: the broker doesn't know how slowly the producer assembles batches, only the result — a write request arrived.

**Consumer** — the client application reading from Kafka. Same story: metrics live in the consumer's JVM. The main one is `records-lag-max` — distance from the tail of the partition. The broker knows the offset the consumer has committed (through `__consumer_offsets`), but the lag metric is easier to read from the client.

**ZooKeeper** — the coordination layer in classical clusters. In newer KRaft-mode clusters ZooKeeper is replaced by built-in consensus, but in many production environments ZK will remain for a long time because migration is non-trivial.

In your stack in `lesson5/docker-compose.yml` three of the four are running: broker, ZooKeeper, and kafka-exporter (which imitates consumer metrics by reading offsets directly). Producer and consumer are launched by short-lived scripts `scripts/demo.sh` and `scripts/produce-load.sh` — specifically for load.

---

## 3. Broker metrics: graphs vs. alerts

This is the most important concept in the entire lecture. The Kafka broker has **a lot** of metrics — several hundred MBeans if you open JConsole. If you put alerts on all of them, ops will burn out from false positives. If you put alerts on none of them, you'll learn about an outage from the tech lead's phone call.

The right approach: split metrics into two buckets.

### 3.1 Metrics for alerts

This is a very short list — literally two metrics, and one only works in a multi-broker cluster:

`ActiveControllerCount` — how many brokers in the cluster currently consider themselves the controller. In a normal Kafka cluster **there must be exactly one** controller. Two = split brain, zero = no one manages the cluster.

Alert rule is simple: `sum(kafka_controller_kafkacontroller_activecontrollercount) != 1`. Page immediately.

`OfflinePartitionsCount` — number of partitions without a live leader. Non-zero means part of the data is **physically unavailable** for reading and writing. Not "running slow" — fully offline.

Alert rule: `kafka_controller_kafkacontroller_offlinepartitionscount > 0`. Page.

That's it. There are no other "wake a human up at night" metrics. Keep this list in your head separately.

### 3.2 Metrics for graphs

This is what hangs on the dashboard and what you look at in the morning over coffee:

`UnderReplicatedPartitions` — partitions where ISR (in-sync replicas) is less than the full replication factor. This is an early warning: data is still available (the leader is alive) but replication is falling behind. If it stays above zero for fifteen minutes, time to check what's wrong with the followers.

`IsrShrinksPerSec` and `IsrExpandsPerSec` — rate of ISR contraction and expansion. If these metrics "saw" with high frequency, followers can't keep up with the leader and keep dropping out of ISR. Possible causes: network, follower disk, GC pauses.

`LeaderElectionRateAndTimeMs` — frequency and duration of leader elections. Elections are a normal event during regular broker restarts. But if they happen on their own on a running cluster, something is unstable.

`BytesInPerSec` / `BytesOutPerSec` — broker throughput in bytes. What to look at: trend (is load growing month over month) and comparison between brokers (one broker shouldn't be taking 80% of traffic — that means partitions are distributed poorly).

`RequestsPerSec` — number of requests to the broker by type (Produce, Fetch, Metadata). Useful during debugging: if FetchConsumer traffic suddenly grew — someone added new consumers or they lost offsets and are reading from the beginning.

In your stack these metrics arrive via the `kafka-jmx-exporter` container at `http://localhost:7071/metrics`. Open it and search for `kafka_server_brokertopicmetrics_bytesinpersec` — already in Prometheus format.

### 3.3 Server-level metrics

Under Kafka runs a regular Linux machine (or container). Its metrics are not Kafka-specific but critical to its operation.

**Page cache hit ratio.** Kafka doesn't read data through JVM heap at all — it relies on the OS page cache. That's why almost all server RAM should go to page cache. If hit ratio drops (because another greedy process appeared on the server, for example), Kafka starts reading from disk and slows down dramatically.

Confluent recommends giving the Kafka server 6-8 GB JVM heap and leaving **all the rest of the memory** for page cache. No "let's give heap 32 GB just in case" — it will hurt performance by taking RAM from page cache.

**Disk usage.** Obvious, but worth restating. An alert at 85% is mandatory. Kafka doesn't stop itself; it writes until it crashes with a write error.

**CPU usage.** Kafka isn't very CPU-intensive in normal operation, but during replication, compression, and compaction the CPU can spike.

**Network bytes sent/received.** Should correlate with `BytesIn/OutPerSec`, plus inter-broker traffic (replication).

---

## 4. Producer and Consumer metrics

It's important to understand **where** the metrics originate. These are not broker metrics — they are client JVM metrics.

### 4.1 Producer

The producer is a library inside your application. When you call `producer.send(record)`, the library doesn't send the record to the broker immediately. It puts it in a local buffer, waits for a batch to fill up (or for `linger.ms` to elapse), and then sends the batch to the broker.

Producer metrics describe the health of this process:

`request-latency-avg` — average time from sending the batch to receiving an ack from the broker. If it's growing — either the network is slow or the broker is slow. This is the first thing you look at on the client side.

`record-error-rate` — error rate during sending. Normally near zero. If non-zero — check the producer logs for the actual error.

`batch-size-avg` — average batch size. If it's much less than `batch.size`, the producer can't accumulate data before sending (low load or `linger.ms` too short). If it's near the limit — at full load, on the contrary.

`compression-rate-avg` — how efficiently data compresses. Depends on message format and compression algorithm (snappy, lz4, gzip, zstd).

JMX path for the producer: `kafka.producer:type=producer-metrics,client-id=<client-id>`. Metrics are split by `client-id`, so if you have several producers in one application they go as separate MBeans.

### 4.2 Consumer

The consumer is also a client library. Its main job is to not fall behind the partition tail.

`records-lag-max` — **maximum** lag across all partitions the consumer reads. This metric answers the main question: "are we in real-time, or are we catching up?"

If the consumer reads 10 partitions and on nine of them the lag is zero but on the tenth it's 50,000 — `records-lag-max` will show 50,000. This is the right behavior: one lagging partition is still a lag for the data consumer.

`bytes-consumed-rate` and `records-consumed-rate` — read rate. Used for capacity planning: how much data flows through the consumer.

`fetch-rate` — frequency of fetch requests to the broker. If it's high while `records-consumed-rate` is low — the consumer is polling the broker frequently but getting nothing (or getting tiny batches).

JMX path for the consumer: `kafka.consumer:type=consumer-fetch-manager-metrics,client-id=<client-id>`.

### 4.3 Lag from the broker side: kafka-exporter

Reading lag through consumer JMX is correct but not always possible. Often consumers are written by other teams, JMX is not set up, and you can't reach into their JVM.

The solution is `kafka-exporter` (a.k.a. `danielqsj/kafka-exporter`). It's a separate application that **connects to Kafka as an ordinary client**, reads the `__consumer_offsets` topic, and publishes lag metrics in Prometheus format.

In your stack it sits at `http://localhost:9308/metrics`. Search for `kafka_consumergroup_lag` — that's the lag-by-group metric collected without the consumers' involvement.

This is also convenient because the metric appears automatically for any new consumer group — no application-side setup needed.

---

## 5. ZooKeeper metrics (and the path to KRaft)

ZooKeeper in classic Kafka stores cluster metadata: which topics exist, which partitions live on which brokers, who is currently controller. If ZooKeeper dies or becomes unreachable, Kafka gradually degrades — brokers cannot agree on changes.

Main metrics:

`outstanding_requests` — number of requests queued in ZooKeeper. If it keeps growing — ZK can't keep up with the load.

`avg_latency` — average response time for a client request. The norm depends on hardware, but usually it's single-digit milliseconds. Tens of ms means the disk under ZK is too slow.

`num_alive_connections` — number of active connections. Brokers + admin tools + kafka-exporter + everything else.

`followers` — number of ZK followers in the cluster. In a 3-node ZK cluster this should equal two (one leader + two followers).

`pending_syncs` — sync requests being processed. Should be near zero.

You can get these metrics three ways:

**JMX** — standard, via a JMX exporter, like Kafka. In your stack: `http://localhost:7072/metrics`.

**Four-letter words** — ancient ZK interface over TCP. E.g., `echo srvr | nc localhost 2181` returns status. Modern configurations often disable this interface for security.

**AdminServer** — HTTP interface on a separate port (default 8080). In your stack it's enabled via `ZOOKEEPER_ADMIN_ENABLE_SERVER: "true"` in `docker-compose.yml`. Open `http://localhost:8080/commands/monitor` — you'll see JSON with metrics.

### 5.1 A note about KRaft

As of Kafka 3.3 KRaft (Kafka Raft) is considered production-ready and replaces ZooKeeper with built-in consensus. In new clusters ZK is not used at all.

For monitoring this means: instead of a separate ZooKeeper process with separate metrics, controller metrics appear inside Kafka itself. JMX paths change: `kafka.server:type=KafkaServer,name=BrokerState` and `kafka.controller:type=KafkaController,name=ActiveControllerCount` stay, but new ones specific to the quorum appear: `kafka.server:type=raft-metrics`.

Your stack uses ZooKeeper (Confluent image `cp-zookeeper:7.6.1`), so everything above applies. But keep in mind: in production of new projects you'll more often see KRaft.

---

## 6. Why JMX, and how it works

JMX (Java Management Extensions) is a Java standard for exposing internal metrics and managing an application "from the inside". Any Java application, including Kafka and ZooKeeper, publishes a set of objects (MBeans) via JMX, and each MBean has attributes — those are the metrics.

When we say "enable JMX in Kafka", we do three things:

1. Enable remote JMX in the JVM (options `-Dcom.sun.management.jmxremote=true`)
2. Open the port on which JMX will listen (e.g., `KAFKA_JMX_PORT=10030`)
3. Specify on which hostname JMX will advertise itself (`-Djava.rmi.server.hostname=kafka`)

In your `docker-compose.yml` this is explicit:

```yaml
KAFKA_JMX_OPTS: >-
  -Dcom.sun.management.jmxremote=true
  -Dcom.sun.management.jmxremote.authenticate=false
  -Dcom.sun.management.jmxremote.ssl=false
  -Dcom.sun.management.jmxremote.port=10030
  -Dcom.sun.management.jmxremote.rmi.port=10030
  -Djava.rmi.server.hostname=kafka
```

After this you can connect a JMX client to Kafka — for example JConsole, which ships with the JDK. You open `jconsole`, pick Remote Process, point at `localhost:10030`, and see the MBean tree: `kafka.server`, `kafka.controller`, `kafka.network` and so on.

JConsole is a handy tool to **manually** verify that a needed metric exists and what attributes it has. But for production monitoring JConsole isn't suitable: it's interactive, doesn't keep history, and works with only one JVM at a time.

### 6.1 JMX port conflict in CLI utilities

This problem isn't obvious the first time, but it bites you once. The stack catches it explicitly.

When the broker is launched with `KAFKA_JMX_PORT=10030`, the variable is exported into the container's environment. If you then run `docker compose exec kafka kafka-topics --list ...`, the new `kafka-topics` process **inherits** the same variable and also tries to open JMX on 10030 — but the port is already held by the broker. You get `Port already in use: 10030` and the command fails.

The right fix is to unset the conflicting variables before running the CLI:

```bash
unset KAFKA_JMX_OPTS KAFKA_JMX_PORT JMX_PORT
kafka-topics --bootstrap-server kafka:9092 --list
```

This is already in `scripts/demo.sh`. Remember the pattern — in any container with JMX enabled, before a `kafka-*` CLI utility you must unset these variables.

### 6.2 macOS JConsole gotcha: the /etc/hosts hack

JConsole connects via RMI. RMI tells the client "I'm at `kafka:10030`", because that's what `-Djava.rmi.server.hostname=kafka` says. On macOS that hostname doesn't resolve — and JConsole shows "Connection failed".

The fix: add the hostname to `/etc/hosts`:

```bash
sudo sh -c 'echo "127.0.0.1 kafka zookeeper" >> /etc/hosts'
```

Now `kafka` resolves to localhost on the Mac, RMI succeeds, JConsole connects. Inside the docker network the same hostname already worked because containers share a DNS layer.

We can't simply set `java.rmi.server.hostname=localhost` instead — then the JMX exporter container, which connects from inside docker, would fail.

---

## 7. Production stack: JMX Exporter, Prometheus, Grafana

JMX is a powerful tool, but Prometheus doesn't talk to it directly. Prometheus works in pull mode over HTTP: every N seconds it hits a `/metrics` endpoint and parses the response in its own text format. JMX is an RMI protocol over TCP, plus a bunch of binary sub-protocols.

A bridge is needed. That bridge is the **JMX Exporter** from the Prometheus team.

### 7.1 Two ways to run JMX Exporter

**As a Java agent inside the Kafka JVM.** This is the way Confluent recommends. Launch like this:

```
KAFKA_OPTS="-javaagent:/path/to/jmx_prometheus_javaagent.jar=7071:/path/to/kafka.yml"
```

The agent starts inside the Kafka JVM, reads MBeans directly (no RMI), and publishes a Prometheus endpoint on port 7071. Advantages: fewer moving parts, direct access to metrics.

**Downside:** if the agent fails to start (e.g., can't open its HTTP port), it brings the broker down with it.

**As a separate process.** JMX Exporter connects to the Kafka JMX port as a remote RMI client, reads MBeans, and publishes its own HTTP endpoint. Advantages: if the exporter dies, the broker lives. It's less efficient (RMI is slower than direct access), but **much safer** for an educational stack and for many production setups.

Your stack picks **the second option**, and it's the right decision for an educational demo. The `demo-kafka-jmx-exporter` and `demo-zookeeper-jmx-exporter` containers stand on their own, and if something dies in them, Kafka keeps running.

### 7.2 JMX Exporter rules file

JMX hands out hundreds of MBeans with long names like:

```
kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=orders
```

Prometheus likes flat names with labels. The rules file `kafka.yml` describes how to convert the former into the latter. A simple rule example:

```yaml
- pattern: kafka.server<type=BrokerTopicMetrics, name=(.+)PerSec, topic=(.+)><>OneMinuteRate
  name: kafka_server_brokertopicmetrics_$1_per_sec
  labels:
    topic: "$2"
  type: GAUGE
```

It means: take all MBeans like `BrokerTopicMetrics`, extract the metric name from the first capture group, put the topic into the `topic` label, and publish as a Prometheus gauge.

A rules file for Kafka is usually copied from GitHub (`prometheus/jmx_exporter` repo, `example_configs/kafka-2_0_0.yml`) and modified to fit. Writing from scratch makes no sense.

### 7.3 What kafka-exporter does and why it's separate

`kafka-exporter` (by danielqsj) is not a JMX bridge. It's a separate application that **connects to Kafka as an ordinary client** and publishes metrics based on what it sees through the Kafka API.

The main thing it gives — **consumer lag** for all consumer groups, without the consumers themselves participating. That's critical: if different teams write their consumers, you can't force them all to set up JMX. But running one kafka-exporter is your job, and it covers all groups automatically.

In addition, kafka-exporter exposes topic- and partition-level metrics: partition size in messages, time of last message, and so on.

In the stack it sits at `http://localhost:9308/metrics`. Check `kafka_consumergroup_lag` — that's the very metric the exporter is here for.

### 7.4 Prometheus — pull, not push

In `prometheus.yml` Prometheus has a `scrape_configs` section listing the endpoints it will visit. In your stack:

```
kafka-jmx-exporter:7071
zookeeper-jmx-exporter:7072
kafka-exporter:9308
```

Every `scrape_interval` (15 seconds by default, 10 in your stack) Prometheus does an HTTP GET on each endpoint, parses the response, and stores the metrics in its TSDB.

**Important:** Prometheus reaches out to metrics itself. Applications **do not send** it anything. This is the opposite of Graphite/InfluxDB, where the application writes into the database.

The advantage of the pull model — Prometheus always knows whether a target responds. If a scrape failed, the `up{job="kafka-jmx"}` metric becomes zero, and you can put an alert on that. With the push model this doesn't work: if the app died, it won't send "I died".

### 7.5 Grafana — the final layer

Grafana collects nothing. It only renders graphs from data it pulls from Prometheus (via PromQL queries).

Dashboards in Grafana are JSON files describing panels, queries, and visual settings. The most famous ready-made dashboard for Kafka is `721-kafka` on grafana.com, importable with one command.

In your stack the dashboard lives at `grafana/dashboards/kafka-demo-dashboard.json` and is wired through provisioning (`grafana/provisioning/dashboards/dashboards.yml`). That means after `docker compose up` the dashboard appears in Grafana automatically, no manual import.

This is the right pattern: everything that should be in Grafana is described as code and lands there automatically. No "open the UI and click around".

---

## 8. Specialized UI systems

Prometheus + Grafana is for **metrics and trends**. But sometimes you need to look **at Kafka itself**: what topics exist, what messages are inside, what state consumer groups are in. That's what specialized UIs do.

**AKHQ** — open-source, Java/Micronaut. The most feature-rich free option. Browse messages, manage topics, view and reset offsets, Schema Registry, LDAP/OIDC auth. UI looks dated, deployment is heavy (JVM + YAML config). My first choice for open-source teams.

**Kafdrop** — open-source, simpler than AKHQ. View-only consumer groups, no auth. Suitable for dev stacks or internal teams without security requirements.

**UI for Apache Kafka (Kafbat fork)** — open-source, modern, actively developed. Features sit between Kafdrop and AKHQ. **This is what's running on port 8090 in your stack.**

**Kpow** — commercial (factorhouse.io), free tier for small clusters. Significantly better UX than open-source alternatives. Deep analytics, live tailing, Connect/ksqlDB/Schema Registry management, multi-cluster. The best Kafka UI on the market, if budget allows.

**Conduktor** — commercial, positioned similarly to Kpow. Desktop app + web. Growing fast, smaller open-source community.

**Xinfra Monitor (LinkedIn)** — separate category: **synthetic monitoring**. Writes test messages and reads them back, measures end-to-end latency, availability, lost/duplicated records. Answers "does the cluster work from the client's point of view?", which metrics alone can't tell you. Overkill for small clusters; valuable at thousands-of-brokers scale.

---

## 9. Tying it back to your stack

### 9.1 What runs

`docker compose up -d` brings up eight containers:

- `demo-zookeeper` — coordination
- `demo-kafka` — the single broker
- `demo-kafka-jmx-exporter` — JMX → Prometheus bridge for Kafka
- `demo-zookeeper-jmx-exporter` — JMX → Prometheus bridge for ZK
- `demo-kafka-exporter` — consumer lag and topic metrics
- `demo-prometheus` — TSDB + scraper
- `demo-grafana` — UI with the pre-loaded dashboard
- `demo-kafka-ui` — Kafbat UI for browsing topics/messages

### 9.2 What talks to what

```
Kafka (10030 JMX)  ──RMI──►  kafka-jmx-exporter (7071/metrics)  ──┐
ZK    (10020 JMX)  ──RMI──►  zk-jmx-exporter   (7072/metrics)  ──┼──►  Prometheus (9090)  ──►  Grafana (3000)
Kafka (9092)       ──API──►  kafka-exporter    (9308/metrics)  ──┘
Kafka (9092 + 10030) ──── ──►  kafka-ui (8090)
```

### 9.3 What to look at during the demo

In order from raw to pretty:

1. **Targets alive:** `http://localhost:9090/targets`. All three should be `UP`.
2. **Raw metrics:** `http://localhost:7071/metrics`. Search for `activecontrollercount=1`, `offlinepartitionscount=0`.
3. **ZooKeeper AdminServer:** `http://localhost:8080/commands/monitor`. JSON straight from ZK, no JMX bridge.
4. **Grafana:** `http://localhost:3000`, admin/admin. Dashboard preloaded via provisioning.
5. **Create lag:** `./scripts/demo.sh` — writes 100, reads 10, writes 200 → ~290 lag. Visible in Grafana within 15-30 seconds.

---

## 10. Quick cheat sheet

1. **Four observation angles are independent.** Broker / Producer / Consumer / ZooKeeper — each with its own metrics. Don't confuse producer metrics with broker metrics: the former live in the client JVM, the latter on the server.

2. **There are very few alerts.** In a standalone Kafka — only `ActiveControllerCount != 1` and `OfflinePartitionsCount > 0`. Everything else is graphs and trends.

3. **The main consumer metric is `records-lag-max`.** Zero or close — real-time. Growing — consumer can't keep up.

4. **Page cache matters more than JVM heap.** Don't give Kafka more than 6-8 GB of heap; leave the rest of RAM for the OS page cache.

5. **Prometheus pulls.** Applications publish `/metrics`, Prometheus scrapes every N seconds. The `up{job="..."}` metric tells you whether a target is reachable.

6. **JMX Exporter is a bridge.** Java agent (inside JVM) or separate process. Separate process is more reliable: exporter dies, Kafka lives.

7. **kafka-exporter ≠ JMX exporter.** kafka-exporter connects to Kafka as a client, reads `__consumer_offsets`, gives lag for all groups automatically.

8. **For browsing data you need a separate UI.** Grafana shows metrics, not messages. For messages and management — AKHQ, Kafbat/UI for Apache Kafka, or Kpow.

---

## Sources

### Official documentation

| Source | URL |
|--------|-----|
| Kafka Monitoring (Apache) | https://kafka.apache.org/documentation/#monitoring |
| Confluent Kafka Monitoring | https://docs.confluent.io/platform/current/kafka/monitoring.html |
| Prometheus docs | https://prometheus.io/docs/introduction/overview/ |
| JMX Exporter | https://github.com/prometheus/jmx_exporter |
| kafka-exporter (danielqsj) | https://github.com/danielqsj/kafka_exporter |
| ZooKeeper AdminServer | https://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_adminserver |

### Theory and context

| Source | URL |
|--------|-----|
| Monitoring Kafka with JMX (Datadog) | https://www.datadoghq.com/blog/monitoring-kafka-performance-metrics/ |
| Collecting Kafka performance metrics (Datadog) | https://www.datadoghq.com/blog/collecting-kafka-performance-metrics/ |
| KRaft mode overview | https://developer.confluent.io/learn/kraft/ |

### Practical

| Source | URL |
|--------|-----|
| Grafana Dashboard 721 (Kafka) | https://grafana.com/grafana/dashboards/721-kafka/ |
| AKHQ | https://akhq.io/ |
| Kpow | https://kpow.io/ |
| Xinfra Monitor | https://github.com/linkedin/kafka-monitor |
| Awesome Prometheus Alerts (Kafka) | https://samber.github.io/awesome-prometheus-alerts/rules.html#kafka |

**Stack versions (June 2026):**

- `confluentinc/cp-kafka:7.6.1`
- `confluentinc/cp-zookeeper:7.6.1`
- `ghcr.io/kafbat/kafka-ui:latest`
- `danielqsj/kafka-exporter:v1.7.0`
- `prom/prometheus:v2.52.0`
- `grafana/grafana:10.4.3`
- JMX Exporter: `1.4.0`

---

*Next lesson: security — authentication (SASL), authorization (ACL), encryption (TLS).*
