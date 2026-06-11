# Lesson 3 — Kafka Cluster Setup & Fault Tolerance

3-broker Kafka cluster in KRaft mode with fault tolerance demo.

## Stack
- Confluent Kafka 7.6.1 — 3 brokers, KRaft mode
- Kafka UI — http://localhost:8080

## Start

```bash
cd scripts
docker-compose -f docker-compose.kraft.yml up -d
```

## Stop

```bash
docker-compose -f docker-compose.kraft.yml down
```

## Key config explained

| Parameter | Value | Meaning |
|---|---|---|
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | 3 | Every topic replicated to all 3 brokers |
| `KAFKA_MIN_INSYNC_REPLICAS` | 2 | At least 2 brokers must confirm write |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | false | Topics must be created explicitly |
| `KAFKA_PROCESS_ROLES` | broker,controller | Each node is both broker and controller (KRaft) |

## Demo: create topic

```bash
docker exec -ti kafka1 kafka-topics \
  --create --topic demo-topic \
  --partitions 3 \
  --replication-factor 3 \
  --bootstrap-server kafka1:19092

docker exec -ti kafka1 kafka-topics \
  --describe --topic demo-topic \
  --bootstrap-server kafka1:19092
```

Expected output — each broker is leader for exactly one partition:
```
Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

## Demo: fault tolerance

Stop one broker:
```bash
docker stop kafka3
```

Check topic — new leader elected automatically:
```bash
docker exec -ti kafka1 kafka-topics \
  --describe --topic demo-topic \
  --bootstrap-server kafka1:19092
```

Cluster keeps working with 2 brokers. Bring kafka3 back:
```bash
docker start kafka3
```

kafka3 rejoins ISR automatically.

## Ports
- `9092` — kafka1 external
- `9093` — kafka2 external
- `9094` — kafka3 external
- `8080` — Kafka UI
