# Apache Kafka with KRaft, SASL/PLAIN and ACL on AWS

Reference lab demonstrating Kafka 4.3 security primitives end-to-end:

- KRaft single-node bootstrap (no ZooKeeper)
- SASL/PLAIN authentication with four principals
- `StandardAuthorizer` ACL authorization
- Real-world pipeline: Filebeat → Kafka → Vector → OpenSearch, with
  Filebeat and Vector binding under non-privileged SASL principals so ACL
  enforcement is observable at the application layer
- Kafbat UI for live cluster inspection

Infrastructure is provisioned by `aws-lab.sh` against an AWS free-tier-eligible
account. Three EC2 instances in a single AZ within `eu-north-1`.

## Architecture

### Deployment topology

```
                  CloudShell (provisioning, ssh)
                  ──────────────┬──────────────
                                │
       ┌────────────────────────┼────────────────────────┐
       ▼                        ▼                        ▼
┌────────────────┐    ┌────────────────────┐    ┌────────────────────┐
│  kafka         │    │  elastic           │    │  clients           │
│  t3.micro 1 GB │    │  t3.small 2 GB     │    │  t3.micro 1 GB     │
│  EBS 8 GB      │    │  EBS 16 GB         │    │  EBS 8 GB          │
│                │    │                    │    │                    │
│  Kafka 4.3.0   │    │  OpenSearch :9200  │    │  log generator     │
│  :9092 SASL    │    │  Dashboards :5601  │    │  filebeat (alice)  │
│  :9093 KRaft   │    │  Kafbat UI :8080   │    │  vector   (bob)    │
│  StandardAuth  │    │  (Docker compose)  │    │  (Docker compose)  │
└────────────────┘    └────────────────────┘    └────────────────────┘
```

| Listener           | Port | Purpose                                  |
|--------------------|------|------------------------------------------|
| `SASL_PLAINTEXT`   | 9092 | Client traffic (producers/consumers/admin) |
| `CONTROLLER`       | 9093 | KRaft controller quorum; also SASL_PLAINTEXT (see notes) |

Inter-host traffic uses private IPs over the default VPC. The security group
authorizes the listed ports for the operator's public IP plus all-traffic
self-reference for host-to-host communication.

### Data flow

```
  clients host                     kafka host                  elastic host
  ─────────────                    ──────────                  ────────────

  loggen (alpine)
    │  ①  emits JSON line every 2s
    ▼
  /var/log/app/*.log
    │  ②  filebeat tails
    ▼
  filebeat ──────────③──────────►  broker :9092
    SASL/PLAIN: alice               │   topic "logs"
    ACL: producer (WRITE)           │   3 partitions, RF=1
                                    │
  vector  ◄─────────④──────────────┘
    SASL/PLAIN: bob
    ACL: consumer (READ)
    │  ⑤  bulk index over HTTP
    └──────────────────────────────────────────►  OpenSearch :9200
                                                    │  index applogs-YYYY.MM.DD
                                                    │
                                                    ▼
                                                  Dashboards :5601
                                                  (⑥  HTTP queries)

  Independent observer:
    Kafbat UI (elastic host) ──── SASL/PLAIN: admin ───►  broker :9092
      :8080                                              read topics, ACLs,
                                                          consumer groups

  Operator access:
    CloudShell  ──── ssh :22 ───►  any host
    laptop      ──── HTTPS  ───►   elastic :5601 (Dashboards), :8080 (Kafbat)
```

The pipeline is configured so that each application binds to Kafka under a
distinct SASL principal whose ACL grants exactly the operation it needs:
filebeat can only write, vector can only read. Substituting either principal
with `charlie` (no ACL) breaks the pipeline at the corresponding stage with
a `TopicAuthorizationException` or `GroupAuthorizationException`, making
ACL enforcement directly observable in the application logs.

## Resource sizing

| Component                              | Host    | RSS    |
|----------------------------------------|---------|--------|
| Kafka broker (heap `-Xms256m -Xmx384m`)| kafka   | ~700 MB |
| OpenSearch (heap 384 MB)               | elastic | ~580 MB |
| OpenSearch Dashboards                  | elastic | ~130 MB |
| Kafbat UI (heap 128–256 MB)            | elastic | ~250 MB |
| Filebeat                               | clients | ~50 MB  |
| Vector                                 | clients | ~50 MB  |
| Log generator (alpine + shell loop)    | clients | ~10 MB  |

The kafka host runs with a 1 GB swap file; without it the broker fails to
allocate the default JVM heap.

Free-tier cost: kafka and clients are within the 750 t3.micro hours/month
allowance; elastic at t3.small is approximately `$0.21 per 10 h`. All EBS
volumes fit within the 30 GB/month free-tier allowance.

## Provisioning

```bash
./aws-lab.sh up                    # creates SG, key pair, 3 instances
./aws-lab.sh add-ip <laptop-ip>    # if up was run from CloudShell
./aws-lab.sh info                  # show DNS, IPs, URLs
./aws-lab.sh ssh kafka             # ssh into a host
./aws-lab.sh stop                  # stop all (storage retained)
./aws-lab.sh down                  # terminate everything
```

`aws-lab.sh` is idempotent: re-running `up` skips existing resources. The
private key is created locally in `~/.ssh/otus-kafka-lab-key.pem`.

## Walkthrough

### 1. Kafka KRaft bootstrap (kafka host)

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jre-headless

wget https://archive.apache.org/dist/kafka/4.3.0/kafka_2.13-4.3.0.tgz
tar -xzf kafka_2.13-4.3.0.tgz
mv kafka_2.13-4.3.0 ~/kafka && cd ~/kafka

# 1 GB swap (required for JVM allocation on t3.micro)
sudo fallocate -l 1G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# KRaft cluster bootstrap
KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format --standalone \
    -t "$KAFKA_CLUSTER_ID" -c config/server.properties

export KAFKA_HEAP_OPTS='-Xms256m -Xmx384m'
bin/kafka-server-start.sh -daemon config/server.properties
```

Smoke test:

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 2. SASL/PLAIN authentication

JAAS file at `config/kafka_server_jaas.conf` declares one inter-broker
identity (`admin`) and four user records (`admin`, `alice`, `bob`,
`charlie`). The committed `kafka_server_jaas.conf.example` carries
placeholders; the live file with secrets is gitignored.

Broker configuration changes (`config/server.properties`):

```properties
listeners=SASL_PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=SASL_PLAINTEXT://<kafka-private-ip>:9092
listener.security.protocol.map=CONTROLLER:SASL_PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
inter.broker.listener.name=SASL_PLAINTEXT

sasl.enabled.mechanisms=PLAIN
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.mechanism.controller.protocol=PLAIN

authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin
allow.everyone.if.no.acl.found=false
```

Restart the broker with `KAFKA_OPTS` referencing the JAAS file:

```bash
export KAFKA_OPTS="-Djava.security.auth.login.config=$HOME/kafka/config/kafka_server_jaas.conf"
bin/kafka-server-start.sh -daemon config/server.properties
```

Client `.properties` files (one per principal) point CLI tools at the
broker with the right credentials, e.g. `clients/alice.properties`:

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="alice" password="changeme";
```

### 3. ACL authorization

Topic and ACL setup (run as admin):

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --command-config clients/admin.properties \
  --create --topic logs --partitions 3 --replication-factor 1

bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --command-config clients/admin.properties \
  --add --producer --topic logs --allow-principal User:alice

bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --command-config clients/admin.properties \
  --add --consumer --topic logs --group '*' --allow-principal User:bob
```

`charlie` is left without any ACL.

### 4. Data pipeline (clients host)

`docker-compose.yml` defines three services with shared `applogs` volume:

- `loggen` — alpine container running a shell loop emitting one JSON line
  every 2 seconds into `/var/log/app/app.log`
- `filebeat` — reads `/var/log/app/*.log`, writes to Kafka topic `logs`
  via `output.kafka` with SASL/PLAIN as principal `alice`
- `vector` — `kafka` source with SASL/PLAIN as principal `bob` reading
  topic `logs`, `elasticsearch` sink writing to OpenSearch at
  `http://<elastic-private-ip>:9200/applogs-<date>`

Notable Vector config:

```yaml
sinks:
  opensearch:
    type: elasticsearch
    api_version: v8         # required for OpenSearch 2.x bulk API
    mode: bulk
    bulk:
      index: "applogs-%Y.%m.%d"
```

OpenSearch is deployed with `DISABLE_SECURITY_PLUGIN=true` for lab
simplicity; production deployments require the security plugin and TLS.

## Verification

### Per-principal CLI tests against topic `logs`

| Principal | List topics       | Produce                       | Consume                                    |
|-----------|-------------------|-------------------------------|--------------------------------------------|
| alice     | `logs` returned   | accepted                      | `GroupAuthorizationException` (no READ)   |
| bob       | `logs` returned   | `ClusterAuthorizationException` | `Processed a total of 6 messages`        |
| charlie   | empty             | `ClusterAuthorizationException` | `GroupAuthorizationException`            |

Screenshots: `alice.png`, `bob.png`, `charlie.png`.

### End-to-end pipeline

- OpenSearch index `applogs-YYYY.MM.DD` populates as Filebeat emits to Kafka
  and Vector forwards to OpenSearch.
- `curl 'http://<elastic-ip>:9200/_cat/indices/applogs-*?v'` reports a
  growing `docs.count`.
- OpenSearch Dashboards `Discover` view shows live JSON documents.
- Kafbat UI shows the `logs` topic, the `vector-consumer` group, ACLs for
  alice and bob, and live messages in `Topics → logs → Messages`.

Screenshots: `opensearch_applogs.png`, `kafbat_ui_messages.png`,
`kafbat_acl.png`.

## Notes

**JVM heap on t3.micro.** Apache Kafka defaults to a 1 GB heap. With
~600 MB available RAM on a freshly-booted t3.micro, the JVM fails
allocation (`__vm_enough_memory ... not enough memory`) before logging
initialization. Setting `KAFKA_HEAP_OPTS=-Xms256m -Xmx384m` and adding a
1 GB swap file resolved it.

**Controller listener self-authentication.** With `StandardAuthorizer`
enabled and the controller listener configured as `PLAINTEXT`, the broker's
own registration request arrives as `User:ANONYMOUS`, fails the
`CLUSTER_ACTION` check, and the broker enters a registration retry loop.
Configuring `CONTROLLER` as `SASL_PLAINTEXT` plus
`sasl.mechanism.controller.protocol=PLAIN` lets the broker authenticate
to itself as `admin` (the super-user declared in JAAS).

**Consumer offsets shadow `--from-beginning`.** A `kafka-console-consumer`
that fails or is killed mid-flight may have already committed its
group's position to `__consumer_offsets`. Subsequent runs with the same
`--group` and `--from-beginning` resume from the committed offset, not
the topic head. Use a unique group name (`--group bob-$(date +%s)`) for
clean reproductions.

**Vector ↔ OpenSearch API compatibility.** Vector's `elasticsearch` sink
with `api_version: auto` sends bulk requests using the Elasticsearch 7
metadata format, which OpenSearch 2.x rejects with
`unknown parameter [_type]`. Pinning `api_version: v8` resolves it.

**EBS sizing for Docker.** Pulling the OpenSearch image (~975 MB) plus
Dashboards (~457 MB) onto a fresh 8 GB Ubuntu volume left no headroom
for layer extraction. The lab volume on `elastic` was extended to 16 GB
via `aws ec2 modify-volume` and `growpart` / `resize2fs`.

**RAM headroom for the elastic stack.** OpenSearch + Dashboards + Kafbat
all on a single t3.micro caused swap thrashing severe enough to make the
host effectively unresponsive. The elastic host was changed to t3.small
(2 GB RAM); the architecture is documented at that size.

**CloudShell vs. operator IP.** When `aws-lab.sh up` is run from
CloudShell, `curl checkip.amazonaws.com` returns CloudShell's egress IP,
not the operator's laptop IP. The browser-facing ports (5601, 8080) need
the operator's real IP to be authorized separately:

```bash
./aws-lab.sh add-ip <laptop-ip>
```

**Deprecated CLI flags in 4.x.** `kafka-console-producer --producer.config`
and `kafka-console-consumer --consumer.config` are deprecated; use
`--command-config` instead. Both forms still work in 4.3.

## Repository layout

```
hw2/
├── LAB.md                          # this document
├── aws-lab.sh                      # AWS provisioning
├── kafka-configs/                  # broker + client configs
│   ├── server.properties.example
│   ├── kafka_server_jaas.conf.example
│   └── clients/<principal>.properties.example
├── elastic-stack/
│   └── docker-compose.yml          # OpenSearch + Dashboards + Kafbat
├── clients-stack/
│   ├── docker-compose.yml          # loggen + filebeat + vector
│   ├── loggen.sh
│   ├── filebeat/filebeat.yml.example
│   └── vector/vector.yaml.example
└── *.png                           # verification screenshots
```

Files containing secrets (live `*.properties`, `kafka_server_jaas.conf`,
keystore files, AWS private key) are excluded by `.gitignore`. `.example`
versions are committed with placeholders.

## Out of scope

- **SSL / TLS.** Adding a `SASL_SSL://9093` listener with a self-signed
  CA, broker keystore (SAN including private IP and public DNS), and
  client truststores. The architecture supports it (port 9093 is already
  authorized in the SG); the configuration is left as a future
  extension.
- **High availability.** Single broker, replication factor 1, single AZ.

## References

- Kafka SASL: https://kafka.apache.org/41/security/authentication-using-sasl/
- Kafka ACLs: https://kafka.apache.org/documentation/#security_authz
- KRaft documentation: https://kafka.apache.org/documentation/#kraft
- OpenSearch Docker: https://hub.docker.com/r/opensearchproject/opensearch
- Vector Kafka source: https://vector.dev/docs/reference/configuration/sources/kafka/
- Vector Elasticsearch sink: https://vector.dev/docs/reference/configuration/sinks/elasticsearch/
- Kafbat UI: https://github.com/kafbat/kafka-ui
