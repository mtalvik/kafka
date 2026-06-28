# Lab 5: Kafka Cluster Monitoring

**Course:** OTUS Apache Kafka
**Goal:** on a single Kafka broker, set up and walk through the full monitoring stack — JMX, Prometheus, Grafana, Kafka UI — and understand which tool answers which question.
**Duration:** ~90 minutes
**Theory:** see `LECTURE.md` in the same folder

---

## 🎯 What you'll get out of it

After the lab:
- Stack of 8 containers running locally (Kafka, ZK, 3 exporters, Prometheus, Grafana, Kafka UI)
- You've walked through **all 4 interfaces** to Kafka metrics: JConsole, raw Prometheus, Grafana, Kafka UI
- You've seen how graphs react to real load
- You know why `lag = 290` immediately after the demo is **not a bug — it's a simulation of a crashed consumer**

---

## Prerequisites

### macOS / Apple Silicon

```bash
# Docker runtime
brew install colima
brew install docker docker-compose

# JDK for JConsole
brew install --cask temurin

# (optional) jq for JSON viewing
brew install jq
```

### Start Colima before anything else

```bash
colima start
```

Verify docker responds:

```bash
docker ps
```

Empty table or list of containers — good. "Cannot connect to the Docker daemon" — Colima hasn't started.

---

## ⚠️ Gotchas you'll hit

| Symptom | Cause | Fix |
|---|---|---|
| `unknown shorthand flag: 'd' in -d` | No docker compose plugin installed | Use `docker-compose` (with the dash) |
| `Cannot connect to the Docker daemon at unix:///...colima/...sock` | Colima asleep | `colima start` |
| JConsole "Connection Failed" on 10030 | RMI hostname `kafka` doesn't resolve on macOS | `sudo sh -c 'echo "127.0.0.1 kafka zookeeper" >> /etc/hosts'` |
| Browser: `ERR_EMPTY_RESPONSE` on 10030 | JMX is RMI, not HTTP | Use jconsole, not the browser |
| `Port already in use 10030` from `kafka-topics` CLI | CLI inherits `KAFKA_JMX_PORT` from the env | Scripts already do `unset KAFKA_JMX_OPTS KAFKA_JMX_PORT JMX_PORT` |

---

## Step 0 — Start the stack

```bash
cd /Users/maria.talvik/REPOS/teaching/kafka/lesson5
docker-compose up -d --build
```

First run is **80-100 seconds** (pulls images, builds the jmx-exporter image).

Verify:

```bash
docker-compose ps
```

Should show **8 containers Up**:

```
demo-zookeeper
demo-kafka
demo-kafka-jmx-exporter
demo-zookeeper-jmx-exporter
demo-kafka-exporter
demo-prometheus
demo-grafana
demo-kafka-ui
```

If something is `Restarting` — check logs:

```bash
docker-compose logs <service_name>
```

---

## Step 1 — JMX directly

### 1.1 ZooKeeper AdminServer (HTTP)

ZK from version 3.5 can serve metrics over HTTP. We enabled it in `docker-compose.yml` via `ZOOKEEPER_ADMIN_ENABLE_SERVER: "true"`.

```bash
curl http://localhost:8080/commands
```

Should return JSON listing available commands. The most useful one:

```bash
curl http://localhost:8080/commands/monitor | jq .
```

What to look at:

| Field | Meaning | Norm |
|---|---|---|
| `server_state` | ZK role | `standalone` (single ZK in our setup) |
| `znode_count` | nodes Kafka created in ZK | ~28 (empty Kafka) |
| `num_alive_connections` | connected clients | 1 (Kafka broker) |
| `ephemerals_count` | ephemeral nodes | 2 (broker + controller). Disappear when broker dies |
| `outstanding_requests` | request queue | 0 (if growing, ZK can't cope) |
| `avg_latency` | average response | <10 ms |
| `watch_count` | watchers active | ~11 (broker tracks changes) |

> 📖 **Why this exists:** AdminServer is the HTTP replacement for the ancient "four-letter words" interface (`echo srvr | nc localhost 2181`). Prometheus can scrape this endpoint directly, no JMX bridge required.

### 1.2 JConsole — GUI for JMX

#### Setup

If not done yet:

```bash
sudo sh -c 'echo "127.0.0.1 kafka zookeeper" >> /etc/hosts'
```

> 📖 **Why:** JConsole connects via RMI. RMI tells the client "I'm at `kafka:10030`", because `java.rmi.server.hostname=kafka` is set in docker-compose. macOS has no such hostname — either an `/etc/hosts` entry or change the JMX config (which would break the JMX exporter container).

#### Launch

```bash
jconsole &
```

In the window:

1. **Remote Process**
2. Address: `localhost:10030`
3. Username/Password: **empty**
4. **Connect** → "Insecure connection" (we disabled TLS)

#### What to look at — Kafka MBeans (port 10030)

**MBeans** tab, tree on the left:

```
kafka.controller
  └─ KafkaController
       ├─ ActiveControllerCount       → Value: 1     ⭐ alert metric
       └─ OfflinePartitionsCount      → Value: 0     ⭐ alert metric

kafka.server
  └─ ReplicaManager
       └─ UnderReplicatedPartitions    → Value: 0    (RF=1, always 0)
  └─ BrokerTopicMetrics
       ├─ BytesInPerSec
       │    └─ Attributes:
       │         ├─ Count           (total bytes)
       │         ├─ OneMinuteRate   ⭐ double-click = live chart
       │         ├─ FiveMinuteRate
       │         └─ MeanRate
       └─ MessagesInPerSec  (same shape)
```

Double-click any numeric attribute → new window with a live chart. Leave the window open on `BytesInPerSec.OneMinuteRate` — when we run the load, you'll see the spike.

#### What to look at — ZooKeeper MBeans (port 10020)

Second jconsole window:

```bash
jconsole &
```

Connect to `localhost:10020`.

```
org.apache.ZooKeeperService
  └─ StandaloneServer_port2181
       └─ InMemoryDataTree
            ├─ NodeCount      → ~28  (same as znode_count from curl)
            └─ WatchCount     → ~11
```

> 📖 **Same data, three interfaces** — AdminServer JSON via curl, JConsole MBeans, and (next step) JMX Exporter in Prometheus format. Production monitoring uses the third option.

---

## Step 2 — Production stack

Close the JConsole windows, move to the browser.

### 2.1 Kafka Exporter (raw metrics)

```
http://localhost:9308/metrics
```

Raw Prometheus exposition format. Search (Ctrl+F):

| Metric | Meaning |
|---|---|
| `kafka_brokers 1` | one broker in the cluster |
| `kafka_topic_partitions{topic="__consumer_offsets"} 50` | internal Kafka topic (stores committed offsets for all groups) |
| `kafka_topic_partition_current_offset{...}` | current offset (max written) |
| `kafka_consumergroup_lag{...}` | **the** metric. Lag by group |

#### Prometheus exposition format

```
kafka_topic_partition_current_offset{topic="orders",partition="0"} 0
└──────── metric name ─────────────┘└──────── labels ──────────┘ └─┘
                                                                  value
```

> 📖 **kafka-exporter doesn't use JMX.** It connects as a regular Kafka client and reads offsets. That's why lag appears automatically for any new consumer group.

### 2.2 JMX Exporter (raw metrics)

#### Kafka JMX (port 7071)

```
http://localhost:7071/metrics
```

Same metrics as JConsole, translated to Prometheus format by rules in `config/jmx/kafka.yml`.

Find:
- `kafka_controller_kafkacontroller_activecontrollercount{} 1.0`
- `kafka_controller_kafkacontroller_offlinepartitionscount{} 0.0`
- `kafka_server_replicamanager_underreplicatedpartitions{} 0.0`
- `kafka_server_brokertopicmetrics_bytesinpersec_count{} 0.0`

Plus JVM metrics: `jvm_memory_bytes_used`, `jvm_threads_current`, `jvm_gc_collection_seconds_count`.

#### ZooKeeper JMX (port 7072)

```
http://localhost:7072/metrics
```

- `zookeeper_inmemorydatatree_nodecount` ← 28 (= znode_count)
- `zookeeper_avglatency`
- `zookeeper_numalivenamespaces`

### 2.3 Prometheus UI

```
http://localhost:9090
```

#### Targets — stack health check

**Status → Targets**

3 jobs, all green (UP):

```
kafka-exporter           1/1 up
kafka-jmx-exporter       1/1 up
zookeeper-jmx-exporter   1/1 up
```

> 📖 The `up{job="..."}` metric is the most basic alert. If a scrape failed, it's 0. The "scrape failure" rule lives on this.

#### Graph — PromQL queries

Query input:

```promql
up
```

Execute → table: 3 rows, all = 1.

```promql
kafka_controller_kafkacontroller_activecontrollercount
```

→ 1 ✓

```promql
rate(kafka_server_brokertopicmetrics_bytesinpersec_count[1m])
```

`rate()` converts a counter to a rate. Currently 0. Remember this function — after `demo.sh` you'll see a spike.

#### Alerts

**Alerts** in the menu. Rules from `prometheus/kafka-rules.yml` should all be **Inactive** (none firing).

### 2.4 Grafana

```
http://localhost:3000
```

Login: **admin** / **admin** → Skip change password.

#### What's pre-configured (via provisioning)

**Connections → Data sources** → one source **Prometheus** ✓

**Dashboards** (four-squares icon) → **kafka-demo-dashboard** ✓

Open the dashboard. Top right:
- **Time range:** Last 5 minutes
- **Refresh:** 5s

You should see:
- ActiveControllerCount: 1
- OfflinePartitionsCount: 0
- UnderReplicatedPartitions: 0
- BytesIn/MessagesIn: 0 (nothing to write)
- JVM Heap: ~200 MB

#### Edit a panel

Click on panel title → **Edit** → PromQL query at the bottom. Change it, **Apply**, see the new graph. Any changes are wiped on container restart (provisioning overwrites).

### 2.5 Kafka UI (Kafbat)

```
http://localhost:8090
```

This is the **fourth** category of monitoring — not metrics but data. Grafana answers "how is the cluster?", Kafka UI answers "what's in the cluster?".

In the UI:

- **Dashboard** — cluster overview
- **Brokers** → click the broker → **JMX Metrics** (uses our port 10030)
- **Topics** — only `__consumer_offsets` (system topic) for now
- **Consumers** — empty

#### Create a topic via UI (optional)

**Topics → Add a Topic** → name `test-from-ui`, 3 partitions, RF=1 → **Create**.

You can enter it → **Messages → Produce Message** → write something in Value → Send. The topic gets the message.

> 📖 Handy alternative to `kafka-console-producer` for one-off debugging.

---

## Step 3 — Load ⚡

The interesting part. We generate traffic and watch **all four monitors react simultaneously**.

### Prep — 4 browser tabs

1. **Grafana** — `http://localhost:3000` → dashboard, refresh 5s, range "Last 5 min"
2. **Kafka UI** — `http://localhost:8090` → Topics
3. **Prometheus** — `http://localhost:9090/graph?g0.expr=rate(kafka_server_brokertopicmetrics_bytesinpersec_count%5B1m%5D)`
4. **(optional) JConsole** with `BytesInPerSec.OneMinuteRate` live chart open

### Run

```bash
chmod +x scripts/*.sh
./scripts/demo.sh
```

### What the script does

| Step | Action | What you'll see |
|---|---|---|
| 1 | Creates topic `orders` (3 partitions, RF=1) | Topic appears in Kafka UI |
| 2 | Sends 100 messages | First BytesIn/MessagesIn spike |
| 3 | Group `demo-group` reads 10 messages and **exits** | Lag = 90 briefly, then 0 in Kafka UI |
| 4 | Sends another 200 messages | Second spike |
| 5 | `kafka-consumer-groups --describe` | Lag ≈ 290 (300 − 10) |

### Expected final output

```
GROUP        TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
demo-group   orders  0          ~3              ~100            ~97
demo-group   orders  1          ~3              ~100            ~97
demo-group   orders  2          ~4              ~100            ~96
```

(numbers approximate — depends on how Kafka spread messages across partitions)

Total LAG ≈ **290**.

### What happened in the monitors

**Grafana:**
- BytesIn / MessagesIn — two spikes (from 100 and 200 messages)
- If there's a Consumer Lag panel — shows ~290

**Prometheus** query `kafka_consumergroup_lag` → 3 rows (per partition), sum ~290

**Kafka UI:**
- Topics → `orders` → 300 messages in 3 partitions
- Consumers → `demo-group` with lag 290

---

## Step 4 — Understanding the lag = 290

### What's "wrong" in our demo

`demo-group` read 10 messages and **exited** (`--max-messages 10`). Therefore:

- Group committed offset = 10
- Topic has 300 messages
- Lag = 300 − 10 = 290
- **But the consumer process no longer exists** — it terminated

This is the equivalent of **a crashed consumer in production**. In a real Kafka cluster, this would trigger alerts.

### When lag is genuinely bad

| Scenario | Diagnosis |
|---|---|
| Lag grows linearly | Consumer can't keep up. Need more partitions or more consumer instances |
| Lag jumped suddenly and stays high | Consumer crashed / hung. Page |
| Lag consistently ~0 | Real-time ✓ |
| Lag steady, small (~50) | Normal due to batching. Not scary |

### "Fix" in our demo

Restart the consumer without `--max-messages`:

```bash
docker-compose exec -T kafka bash -lc \
  "unset KAFKA_JMX_OPTS KAFKA_JMX_PORT JMX_PORT; \
   kafka-console-consumer --bootstrap-server kafka:9092 \
   --topic orders --group demo-group"
```

Watch Grafana — `consumer lag` will fall toward zero as the consumer catches up. **This is the most satisfying graph in all of Kafka monitoring.**

After all 290 have been read and lag = 0 — press **Ctrl+C** to exit the consumer.

---

## Step 5 — Extra load (optional)

To practice with the dashboards:

```bash
./scripts/produce-load.sh 5000
```

Sends 5000 messages to `orders` in one shot. Grafana will show a big spike.

Run them in parallel from several terminals:

```bash
./scripts/produce-load.sh 10000 &
./scripts/produce-load.sh 10000 &
./scripts/produce-load.sh 10000 &
wait
```

30000 messages in three parallel writes — the graphs come alive.

---

## Step 6 — Cleanup

When done:

```bash
./scripts/cleanup.sh
```

This tears down containers **and volumes** — Kafka and Prometheus data is wiped. Next `docker-compose up -d` starts fresh.

To keep data (e.g., continue tomorrow):

```bash
docker-compose stop
```

→ containers stopped, volumes preserved. `docker-compose start` to resume.

---

## Checklist — what I can do now

- [ ] Start the 8-container stack with a single command
- [ ] Connect JConsole to running Kafka via JMX (understand the `/etc/hosts` hack)
- [ ] Find `ActiveControllerCount` and `OfflinePartitionsCount` in three places: JConsole / JMX exporter raw / Grafana
- [ ] Understand how kafka-exporter differs from JMX exporter (one reads the Kafka API, the other reads MBeans)
- [ ] Open Prometheus Targets and check all UP
- [ ] Write a simple PromQL query and see the result in Graph
- [ ] Open the pre-loaded Grafana dashboard via provisioning
- [ ] Create a topic and write messages via Kafka UI
- [ ] Generate load and watch it **simultaneously** in JConsole, Prometheus, Grafana, Kafka UI
- [ ] Understand why `lag = 290` after `demo.sh` is a simulation of a crashed consumer

---

## Further reading

- `LECTURE.md` — the theory behind this lab
- [Kafka monitoring docs (Apache)](https://kafka.apache.org/documentation/#monitoring)
- [Prometheus JMX Exporter README](https://github.com/prometheus/jmx_exporter)
- [Awesome Prometheus Alerts: Kafka](https://samber.github.io/awesome-prometheus-alerts/rules.html#kafka)

---

*Next OTUS lab: Security (SASL, SSL, ACL).*
