# Kafka Monitoring Demo

Single-broker Kafka stack to demonstrate the full monitoring pipeline:

```
Kafka + ZooKeeper
    ↓ JMX
JMX Exporter        +    Kafka Exporter (lag, topic/group metrics)
    ↓ /metrics
Prometheus
    ↓
Grafana   +   Kafbat UI (browse topics/messages)
```

Educational stand for the OTUS Kafka lesson 5 (monitoring).

📚 **For theory** see `LECTURE.md`. **For step-by-step lab** see `LAB.md`. This README is a quick reference.

---

## Prerequisites

### macOS / Apple Silicon

```bash
brew install colima docker docker-compose
brew install --cask temurin     # JDK for JConsole
brew install jq                  # optional, for JSON viewing

colima start
```

### Windows

- Docker Desktop with WSL2 backend
- PowerShell 5.1+ (built-in)
- JDK with JConsole — `winget install EclipseAdoptium.Temurin.17.JDK`

### macOS gotcha: JMX hostname

JConsole on macOS needs the docker-internal hostnames resolvable on the host:

```bash
sudo sh -c 'echo "127.0.0.1 kafka zookeeper" >> /etc/hosts'
```

Without this, JConsole fails with "Connection failed" when connecting to `localhost:10030`. See `LAB.md` Step 1.2 for the explanation.

---

## Containers and ports

| Container | Purpose | Address from host |
|---|---|---|
| `demo-zookeeper` | ZooKeeper | `localhost:2181`, AdminServer `:8080`, JMX `:10020` |
| `demo-kafka` | Kafka broker | `localhost:29092`, JMX `:10030` |
| `demo-kafka-jmx-exporter` | Kafka JMX → Prometheus | `http://localhost:7071/metrics` |
| `demo-zookeeper-jmx-exporter` | ZK JMX → Prometheus | `http://localhost:7072/metrics` |
| `demo-kafka-exporter` | consumer lag, topic metrics | `http://localhost:9308/metrics` |
| `demo-prometheus` | TSDB + scraper | `http://localhost:9090` |
| `demo-grafana` | dashboards | `http://localhost:3000` (admin/admin) |
| `demo-kafka-ui` | Kafbat UI | `http://localhost:8090` |

---

## Quick start

### macOS

```bash
cd /Users/maria.talvik/REPOS/teaching/kafka/lesson5
docker-compose up -d --build
docker-compose ps
```

### Windows

```powershell
cd $HOME\Downloads\kafka-monitoring-win-demo
docker compose up -d --build
docker compose ps
```

First run takes 80-100 seconds. All 8 containers should be `Up`.

---

## Demo scripts

### macOS (bash)

```bash
chmod +x scripts/*.sh
./scripts/demo.sh                  # create topic, write 100, read 10, write 200, show lag
./scripts/produce-load.sh 5000     # extra load
./scripts/cleanup.sh               # full teardown including volumes
```

### Windows (PowerShell)

```powershell
.\scripts\demo.ps1
.\scripts\produce-load.ps1 -Count 5000
.\scripts\cleanup.ps1
```

---

## Common gotchas

### `unknown shorthand flag: 'd' in -d` (macOS)

You don't have the Docker Compose v2 plugin. Use the hyphenated version:

```bash
docker-compose up -d            # not "docker compose"
```

The bash scripts use `docker-compose`. If you have v2 plugin and prefer `docker compose`, sed-replace in `scripts/*.sh`.

### `Cannot connect to the Docker daemon` (macOS)

Colima isn't running:

```bash
colima start
```

### JConsole "Connection Failed" on 10030

The `/etc/hosts` entry is missing. See macOS gotcha above.

### `ERR_EMPTY_RESPONSE` in browser on port 10030 or 10020

These are JMX (RMI) ports, not HTTP. Use JConsole, not a browser:

```bash
jconsole &
# Remote Process → localhost:10030 → Insecure connection
```

### `Port already in use: 10030` when running kafka-* CLI

The Kafka broker holds JMX on 10030. When you `docker-compose exec kafka kafka-topics ...`, the new process inherits `KAFKA_JMX_PORT` and tries to grab the same port.

Always unset the JMX vars first:

```bash
docker-compose exec kafka bash -lc \
  "unset KAFKA_JMX_OPTS KAFKA_JMX_PORT JMX_PORT; \
   kafka-topics --bootstrap-server kafka:9092 --list"
```

The demo scripts already do this.

### Grafana dashboard empty

1. Check `http://localhost:9090/targets` — all targets should be `UP`
2. Wait 30-60 seconds (one or two scrape cycles)
3. Refresh Grafana
4. Time range top-right: set to "Last 5 minutes" minimum

### PowerShell blocks script execution (Windows)

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\scripts\demo.ps1
```

---

## Repo structure

```
lesson5/
├── LECTURE.md                    # full theory (~60 min read)
├── LAB.md                        # step-by-step lab walkthrough
├── README.md                     # this file
├── docker-compose.yml            # 8-container stack
├── config/jmx/
│   ├── kafka.yml                 # JMX exporter rules for Kafka
│   └── zookeeper.yml             # JMX exporter rules for ZK
├── docker/jmx-exporter/
│   └── Dockerfile                # JMX exporter container image
├── prometheus/
│   ├── prometheus.yml            # scrape targets
│   └── kafka-rules.yml           # alert rules
├── grafana/
│   ├── dashboards/
│   │   └── kafka-demo-dashboard.json
│   └── provisioning/
│       ├── dashboards/dashboards.yml
│       └── datasources/prometheus.yml
└── scripts/
    ├── demo.sh / demo.ps1
    ├── produce-load.sh / produce-load.ps1
    └── cleanup.sh / cleanup.ps1
```

---

## What's where (cheat sheet)

| Question | Where to look |
|---|---|
| Is the broker alive? | `http://localhost:9090/targets` (kafka-jmx UP) |
| `ActiveControllerCount` = 1? | JConsole `kafka.controller > KafkaController` OR `localhost:7071/metrics` |
| `OfflinePartitionsCount` = 0? | Same as above |
| Consumer lag for `demo-group`? | `localhost:9308/metrics` (`kafka_consumergroup_lag`) OR Kafka UI Consumers |
| Browse messages in a topic? | Kafka UI `localhost:8090` → Topics → messages tab |
| ZK metrics without JMX? | `localhost:8080/commands/monitor` |
| Pre-loaded Kafka dashboard? | Grafana `localhost:3000` → Dashboards → kafka-demo-dashboard |

---

## Stack versions (June 2026)

- `confluentinc/cp-kafka:7.6.1`
- `confluentinc/cp-zookeeper:7.6.1`
- `ghcr.io/kafbat/kafka-ui:latest`
- `danielqsj/kafka-exporter:v1.7.0`
- `prom/prometheus:v2.52.0`
- `grafana/grafana:10.4.3`
- JMX Exporter: `1.4.0`

ZK mode (not KRaft) — chosen to match OTUS course materials. See `LECTURE.md` §5.1 for notes on KRaft.

---

## Teardown

```bash
# macOS
docker-compose down -v           # remove containers + volumes (full wipe)
docker-compose stop              # stop containers, keep volumes (resume tomorrow)
```

```powershell
# Windows
docker compose down -v
docker compose stop
```
