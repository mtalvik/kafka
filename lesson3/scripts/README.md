# Kafka Cluster Lab: ZooKeeper и KRaft в Docker

Проект для занятия «Установка и настройка кластера. ZooKeeper. KRaft. Настройка брокеров».

## Требования

- Windows 10/11
- Docker Desktop
- PowerShell

## Что внутри

- `docker-compose.zookeeper.yml` — Kafka-кластер из 3 брокеров + ZooKeeper + Kafka UI.
- `docker-compose.kraft.yml` — Kafka-кластер из 3 брокеров в KRaft-режиме + Kafka UI.
- `scripts/` — команды для быстрого запуска и остановки.

## Важно перед запуском

Если раньше уже запускались контейнеры с такими именами, сначала очистите их:

```powershell
docker compose -f docker-compose.zookeeper.yml down -v --remove-orphans
docker compose -f docker-compose.kraft.yml down -v --remove-orphans
docker rm -f kafka1 kafka2 kafka3 zookeeper kafka-ui 2>$null
```

## Запуск варианта с ZooKeeper

```powershell
docker compose -f docker-compose.zookeeper.yml pull
docker compose -f docker-compose.zookeeper.yml up -d
```

Проверка:

```powershell
docker ps
```

Kafka UI:

```text
http://localhost:8080
```

Если Kafka UI открылся не сразу, подождите 30-60 секунд: брокерам нужно время, чтобы подключиться к ZooKeeper и поднять служебные топики.

## Создание топика

```powershell
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:19092 --create --topic demo-topic --partitions 3 --replication-factor 3
```

Проверка топика:

```powershell
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:19092 --describe --topic demo-topic
```

## Producer

```powershell
docker exec -it kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic demo-topic
```

Введите несколько сообщений. Завершение: `Ctrl+C`.

## Consumer

В другом окне PowerShell:

```powershell
docker exec -it kafka2 kafka-console-consumer --bootstrap-server kafka2:19093 --topic demo-topic --from-beginning
```

## Демонстрация отказоустойчивости

Остановите один брокер:

```powershell
docker stop kafka3
```

Посмотрите состояние топика:

```powershell
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:19092 --describe --topic demo-topic
```

Верните брокер:

```powershell
docker start kafka3
```

## Остановка ZooKeeper-варианта

```powershell
docker compose -f docker-compose.zookeeper.yml down -v
```

## Запуск KRaft-варианта

Перед запуском KRaft остановите ZooKeeper-вариант:

```powershell
docker compose -f docker-compose.zookeeper.yml down -v --remove-orphans
```

Запустите KRaft:

```powershell
docker compose -f docker-compose.kraft.yml pull
docker compose -f docker-compose.kraft.yml up -d
```

Kafka UI:

```text
http://localhost:8080
```

Создание топика в KRaft такое же:

```powershell
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:19092 --create --topic kraft-topic --partitions 3 --replication-factor 3
```

## Что объяснять на занятии

### Kafka

Kafka — распределенная платформа для хранения и передачи потоковых событий. Основные сущности: broker, topic, partition, producer, consumer, consumer group.

### ZooKeeper

В старой архитектуре Kafka ZooKeeper хранил метаданные кластера и помогал брокерам координироваться. Сейчас это legacy-подход, полезный для понимания эволюции Kafka.

### KRaft

KRaft — современный режим Kafka без ZooKeeper. Метаданные Kafka управляются встроенным controller quorum. В этом проекте каждый узел одновременно является broker и controller.

### Важные настройки брокеров

- `KAFKA_BROKER_ID` / `KAFKA_NODE_ID` — уникальный номер узла.
- `KAFKA_LISTENERS` — где брокер слушает подключения.
- `KAFKA_ADVERTISED_LISTENERS` — какие адреса брокер сообщает клиентам.
- `KAFKA_INTER_BROKER_LISTENER_NAME` — listener для общения брокеров между собой.
- `KAFKA_DEFAULT_REPLICATION_FACTOR=3` — репликация по умолчанию.
- `KAFKA_MIN_INSYNC_REPLICAS=2` — минимум синхронных реплик для надежной записи.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3` — репликация служебного топика consumer offsets.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` — топики создаются явно, чтобы конфигурация была контролируемой.

## Диагностика

Логи ZooKeeper:

```powershell
docker logs zookeeper
```

Логи брокера:

```powershell
docker logs kafka1
```

Проверить, какие образы указаны в compose-файле:

```powershell
Select-String -Path .\docker-compose.zookeeper.yml -Pattern "image:"
```
