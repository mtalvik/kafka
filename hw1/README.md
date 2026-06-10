# Homework 1

## Stack
- Java 23
- Apache Kafka 3.7.1

## Steps

### 1. Start Zookeeper
```bash
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
```

### 2. Start Kafka Broker
```bash
bin/kafka-server-start.sh -daemon config/server.properties
```

### 3. Verify services are running
```bash
nc -zv localhost 2181  # Zookeeper
nc -zv localhost 9092  # Kafka broker
```

### 4. Create topic
```bash
bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic test --bootstrap-server localhost:9092
```

### 5. Send messages (Producer)
```bash
bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
```

### 6. Read messages (Consumer)
```bash
bin/kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server localhost:9092
```
