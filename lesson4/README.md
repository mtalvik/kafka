# Kafka KRaft + UI

Single-node Kafka in KRaft mode (no Zookeeper) with Kafka UI.

## Stack
- Apache Kafka 7.6.1 (Confluent) — KRaft mode, broker + controller in one node
- Kafka UI (Provectus) — web interface at http://localhost:8080

## Start

```bash
docker-compose up -d
```

Open http://localhost:8080

## Stop

```bash
docker-compose down
```

## Usage

### Create topic
```bash
docker exec -ti kafka kafka-topics --create \
  --topic <topic-name> \
  --partitions 3 \
  --bootstrap-server kafka:9092
```

### Producer
```bash
docker exec -ti kafka kafka-console-producer \
  --topic <topic-name> \
  --bootstrap-server kafka:9092
```

### Consumer
```bash
docker exec -ti kafka kafka-console-consumer \
  --topic <topic-name> \
  --from-beginning \
  --bootstrap-server kafka:9092
```

## Ports
- `19092` — Kafka external (for clients outside Docker)
- `9092` — Kafka internal (inside Docker network)
- `8080` — Kafka UI
