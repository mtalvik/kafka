# Kafka Topics Practice в Docker на Windows

Практика к теме: **топики, records, offsets, partitions и основные операции `kafka-topics.sh`**.

Проект запускает один Kafka-брокер в Docker без Zookeeper, в режиме KRaft. Этого достаточно для учебной практики на Windows через Docker Desktop.

---

## 1. Что понадобится

Установите:

1. **Docker Desktop for Windows**
2. Включённый Docker Engine
3. PowerShell или Windows Terminal

Проверка:

```powershell
docker --version
docker compose version
```

Если команды работают, можно запускать практику.

---

## 2. Запуск Kafka

Откройте PowerShell в папке проекта и выполните:

```powershell
docker compose up -d
```

Проверьте, что контейнер запущен:

```powershell
docker ps
```

Вы должны увидеть контейнер:

```text
kafka-practice
```

Посмотреть логи Kafka:

```powershell
docker logs -f kafka-practice
```

Когда в логах нет постоянных ошибок и контейнер не перезапускается, Kafka готова.

---

## 3. Как выполнять Kafka-команды

Все команды будем запускать **внутри контейнера** через `docker exec`.

Общий шаблон:

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 <операция>
```

Например:

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## 4. Практика 1: список топиков

Команда:

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Сначала список может быть пустым. Это нормально: мы ещё не создавали свои топики.

Смысл команды:

```text
--list — вывести список существующих топиков
```

---

## 5. Практика 2: создать топик

Создадим топик `orders_created` с тремя партициями:

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders_created --partitions 3 --replication-factor 1
```

Ожидаемый результат:

```text
Created topic orders_created.
```

Что здесь важно:

```text
--create                  создать топик
--topic orders_created    имя топика
--partitions 3            количество партиций
--replication-factor 1    одна копия каждой партиции
```

Для локальной практики `replication-factor 1` подходит, потому что у нас один брокер. В реальном production-кластере обычно используют больше брокеров и replication factor, например `3`.

---

## 6. Практика 3: снова вывести список топиков

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Теперь должен появиться топик:

```text
orders_created
```

Так мы проверяем, что создание прошло успешно.

---

## 7. Практика 4: описание топика

Посмотрим устройство топика:

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders_created
```

Пример вывода:

```text
Topic: orders_created TopicId: ... PartitionCount: 3 ReplicationFactor: 1 Configs: ...
Topic: orders_created Partition: 0 Leader: 1 Replicas: 1 Isr: 1
Topic: orders_created Partition: 1 Leader: 1 Replicas: 1 Isr: 1
Topic: orders_created Partition: 2 Leader: 1 Replicas: 1 Isr: 1
```

Расшифровка:

```text
PartitionCount: 3    у топика три партиции
ReplicationFactor: 1 каждая партиция хранится в одной копии
Leader: 1            брокер-лидер для партиции
Replicas: 1          где лежат копии партиции
Isr: 1               in-sync replicas, синхронные реплики
```

Главная идея: топик не является одной физической очередью. Он состоит из партиций.

---

## 8. Практика 5: записать сообщения в топик

Теперь отправим несколько сообщений. Запустите producer:

```powershell
docker exec -it kafka-practice kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders_created
```

После запуска вводите сообщения по одному и нажимайте Enter:

```text
order-1 created
order-2 created
order-3 created
```

Чтобы выйти из producer:

```text
Ctrl + C
```

Что произошло: producer опубликовал records в topic `orders_created`.

---

## 9. Практика 6: прочитать сообщения из начала топика

Запустите consumer:

```powershell
docker exec -it kafka-practice kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders_created --from-beginning
```

Вы должны увидеть сообщения:

```text
order-1 created
order-2 created
order-3 created
```

Чтобы остановить consumer:

```text
Ctrl + C
```

`--from-beginning` означает: читать существующие сообщения с начала доступного лога.

---

## 10. Практика 7: посмотреть offsets и partitions

Запустим consumer так, чтобы он показывал служебную информацию: topic, partition и offset.

В PowerShell выполните:

```powershell
docker exec -it kafka-practice kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders_created --from-beginning --property print.partition=true --property print.offset=true --property print.key=true --property print.value=true
```

Пример вывода может быть таким:

```text
Partition:0 Offset:0 null order-1 created
Partition:1 Offset:0 null order-2 created
Partition:2 Offset:0 null order-3 created
```

Или сообщения могут оказаться в других партициях. Это нормально.

Важно:

```text
offset считается внутри конкретной partition
partition 0 имеет свои offsets
partition 1 имеет свои offsets
partition 2 имеет свои offsets
```

---

## 11. Практика 8: создать сообщения с ключами

Ключ помогает Kafka выбирать партицию. Сообщения с одинаковым ключом обычно попадают в одну и ту же партицию.

Запустите producer с парсингом ключа:

```powershell
docker exec -it kafka-practice kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders_created --property parse.key=true --property key.separator=:
```

Вводите сообщения в формате `key:value`:

```text
order-777:created
order-777:paid
order-777:shipped
order-888:created
order-888:paid
```

Выйдите через `Ctrl + C`.

Теперь прочитайте с partition и offset:

```powershell
docker exec -it kafka-practice kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders_created --from-beginning --property print.partition=true --property print.offset=true --property print.key=true --property print.value=true
```

Обратите внимание: события с одинаковым ключом должны идти в одной партиции. Так Kafka сохраняет порядок событий для одной сущности, например для одного заказа.

---

## 12. Практика 9: увеличить количество партиций

Сейчас у топика 3 партиции. Увеличим до 6:

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic orders_created --partitions 6
```

Проверим:

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders_created
```

Теперь должно быть:

```text
PartitionCount: 6
```

Важно: количество партиций можно увеличить, но нельзя просто так уменьшить.

Также увеличение количества партиций может изменить распределение новых сообщений по ключам. Поэтому в реальных системах это делают осторожно.

---

## 13. Практика 10: удалить топик

Удалим учебный топик:

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic orders_created
```

Проверим список:

```powershell
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Топика `orders_created` больше не должно быть.

Удаление топика удаляет и данные внутри него.

---

## 14. Быстрый справочник команд

```powershell
# Запустить Kafka
docker compose up -d

# Остановить Kafka
docker compose down

# Остановить Kafka и удалить данные
docker compose down -v

# Список топиков
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --list

# Создать топик
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders_created --partitions 3 --replication-factor 1

# Описать топик
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders_created

# Увеличить число партиций
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic orders_created --partitions 6

# Удалить топик
docker exec -it kafka-practice kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic orders_created

# Producer без ключей
docker exec -it kafka-practice kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders_created

# Producer с ключами
docker exec -it kafka-practice kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders_created --property parse.key=true --property key.separator=:

# Consumer с начала
docker exec -it kafka-practice kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders_created --from-beginning

# Consumer с partition, offset, key, value
docker exec -it kafka-practice kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders_created --from-beginning --property print.partition=true --property print.offset=true --property print.key=true --property print.value=true
```

---

## 15. Задания для самостоятельной проверки

1. Создайте топик `payments_completed` с двумя партициями.
2. Выведите список топиков.
3. Посмотрите описание `payments_completed`.
4. Отправьте 5 сообщений без ключей.
5. Прочитайте сообщения с `--from-beginning`.
6. Отправьте 5 сообщений с ключами `payment-1` и `payment-2`.
7. Проверьте, в какие партиции попали сообщения.
8. Увеличьте количество партиций до 4.
9. Проверьте описание топика.
10. Удалите оба учебных топика.

---

## 16. Частые проблемы на Windows

### Контейнер не запускается

Проверьте Docker Desktop. Он должен быть запущен.

```powershell
docker ps
```

### Порт 9092 занят

Если порт занят, остановите старые Kafka-контейнеры:

```powershell
docker ps
```

Затем остановите ненужный контейнер:

```powershell
docker stop <container_name>
```

### Хочу начать практику заново

Остановите контейнеры и удалите volume с данными:

```powershell
docker compose down -v
```

Потом снова:

```powershell
docker compose up -d
```

### PowerShell странно обрабатывает переносы строк

В README команды даны одной строкой. Просто копируйте их целиком.

---

## 17. Короткая лекционная формула

```text
Topic — имя потока данных.
Record — одно сообщение в топике.
Offset — позиция сообщения внутри партиции.
Partition — упорядоченный раздел топика.
Порядок гарантируется только внутри partition.
kafka-topics.sh управляет топиками: list, describe, create, delete, alter.
```
