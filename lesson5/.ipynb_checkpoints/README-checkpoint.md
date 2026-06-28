# Kafka Monitoring Demo на Windows Docker

Это демонстрационный стенд для лекции по мониторингу Kafka.

Цель стенда: показать на одной машине Windows, где установлен только Docker Desktop, всю цепочку:

```text
Kafka + ZooKeeper
    ↓ JMX
JMX Exporter
    ↓ /metrics
Prometheus
    ↓
Grafana
```

Дополнительно в стенд добавлен `kafka-exporter`, чтобы удобно показать consumer lag, topic partitions и consumer group метрики.

---

# 0. Что нужно заранее

На компьютере должен быть установлен и запущен **Docker Desktop**.

Больше ничего ставить не нужно:

```text
Java не нужна.
Kafka не нужна.
Prometheus не нужен.
Grafana не нужна.
PowerShell уже есть в Windows.
```

Все компоненты запускаются в Docker-контейнерах.

---

# 1. Что находится в архиве

После распаковки ZIP будет папка:

```text
kafka-monitoring-win-demo
```

Внутри:

```text
kafka-monitoring-win-demo/
├── docker-compose.yml
├── README.md
├── config/
│   └── jmx/
│       ├── kafka.yml
│       └── zookeeper.yml
├── docker/
│   ├── kafka/
│   │   └── Dockerfile
│   └── zookeeper/
│       └── Dockerfile
├── prometheus/
│   └── prometheus.yml
├── grafana/
│   ├── dashboards/
│   │   └── kafka-demo-dashboard.json
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboards.yml
│       └── datasources/
│           └── prometheus.yml
└── scripts/
    ├── demo.ps1
    ├── produce-load.ps1
    └── cleanup.ps1
```

Коротко по файлам:

```text
docker-compose.yml              — запускает весь стенд
config/jmx/kafka.yml            — правила JMX Exporter для Kafka
config/jmx/zookeeper.yml        — правила JMX Exporter для ZooKeeper
prometheus/prometheus.yml       — targets, откуда Prometheus забирает метрики
grafana/dashboards/...          — готовый dashboard для лекции
scripts/demo.ps1                — основной демонстрационный сценарий
scripts/produce-load.ps1        — генерация дополнительной нагрузки
scripts/cleanup.ps1             — очистка demo-topic и consumer group
```

---

# 2. Какие контейнеры поднимаются

| Контейнер | Что это | Зачем нужен | Адрес на Windows |
|---|---|---|---|
| `demo-zookeeper` | ZooKeeper | классическая координация Kafka | `localhost:2181`, `localhost:8080`, `localhost:7072` |
| `demo-kafka` | Kafka broker | принимает, хранит и отдаёт сообщения | `localhost:29092`, `localhost:7071`, `localhost:10030` |
| `demo-kafka-exporter` | Kafka Exporter | consumer lag, topic/group metrics | `http://localhost:9308/metrics` |
| `demo-prometheus` | Prometheus | собирает и хранит метрики | `http://localhost:9090` |
| `demo-grafana` | Grafana | dashboard'ы и визуализация | `http://localhost:3000` |

---

# 3. Главные адреса для лекции

Эту таблицу удобно держать открытой во время демонстрации.

| Что показываем | Адрес | Что должно быть видно |
|---|---|---|
| Grafana | `http://localhost:3000` | dashboard с Kafka-метриками |
| Prometheus targets | `http://localhost:9090/targets` | targets со статусом `UP` |
| Prometheus graph | `http://localhost:9090/graph` | ручной запрос PromQL |
| Kafka JMX Exporter | `http://localhost:7071/metrics` | сырые Kafka JMX-метрики |
| ZooKeeper JMX Exporter | `http://localhost:7072/metrics` | сырые ZooKeeper JMX-метрики |
| Kafka Exporter | `http://localhost:9308/metrics` | topic и consumer group метрики |
| ZooKeeper AdminServer commands | `http://localhost:8080/commands` | список команд ZooKeeper |
| ZooKeeper AdminServer stats | `http://localhost:8080/commands/stats` | статистика ZooKeeper |
| ZooKeeper AdminServer monitor | `http://localhost:8080/commands/monitor` | monitoring-показатели ZooKeeper |

Логин и пароль Grafana:

```text
login: admin
password: admin
```

Если Grafana предложит сменить пароль, для лекции можно нажать `Skip`.

---

# 4. Запуск стенда

## 4.1. Открыть PowerShell

Откройте PowerShell в папке проекта.

Например, если архив распакован в `Downloads`:

```powershell
cd $HOME\Downloads\kafka-monitoring-win-demo
```

Проверьте, что в этой папке есть `docker-compose.yml`:

```powershell
dir
```

## 4.2. Запустить контейнеры

```powershell
docker compose up -d --build
```

Первый запуск может занять несколько минут. Docker скачает образы и соберёт Kafka/ZooKeeper-образы с JMX Exporter.

## 4.3. Проверить, что контейнеры запущены

```powershell
docker compose ps
```

Должны быть контейнеры:

```text
demo-zookeeper
demo-kafka
demo-kafka-exporter
demo-prometheus
demo-grafana
```

В колонке `State` / `Status` должно быть что-то вроде:

```text
Up
```

## 4.4. Подождать готовности Kafka

Kafka может стартовать немного дольше, чем контейнер. Обычно достаточно подождать 20–40 секунд.

Можно посмотреть логи Kafka:

```powershell
docker compose logs -f kafka
```

Когда увидите, что broker стартовал, можно остановить просмотр логов:

```text
Ctrl + C
```

---

# 5. Сценарий демонстрации на лекции

Ниже готовый порядок показа. Можно идти прямо по шагам.

---

## Шаг 1. Показать архитектуру стенда

Сначала показать файл `docker-compose.yml` или просто объяснить словами:

```text
У нас есть Kafka broker.
У Kafka включён JMX.
JMX Exporter превращает JMX в /metrics.
Prometheus забирает /metrics.
Grafana строит dashboard.
ZooKeeper тоже отдаёт JMX и AdminServer HTTP.
Kafka Exporter отдельно показывает consumer lag.
```

Схема:

```text
Kafka broker
  ├─ JMX Exporter: http://localhost:7071/metrics
  └─ Kafka protocol: localhost:29092

ZooKeeper
  ├─ JMX Exporter: http://localhost:7072/metrics
  └─ AdminServer: http://localhost:8080/commands/monitor

Kafka Exporter
  └─ http://localhost:9308/metrics

Prometheus
  └─ http://localhost:9090/targets

Grafana
  └─ http://localhost:3000
```

Что сказать:

```text
Docker здесь заменяет нам отдельные серверы.
Все сервисы работают локально, но с точки зрения мониторинга логика такая же, как на реальном стенде.
```

---

## Шаг 2. Показать Prometheus targets

Открыть в браузере:

```text
http://localhost:9090/targets
```

Что показывать:

```text
kafka-jmx      UP
zookeeper-jmx  UP
kafka-exporter UP
prometheus     UP
```

Что объяснить:

```text
Prometheus работает по pull-модели.
Он сам ходит на endpoint'ы /metrics и забирает метрики.
Статус UP означает, что endpoint доступен и Prometheus может его читать.
```

Если какой-то target не `UP`, подождать 20–30 секунд и обновить страницу.

---

## Шаг 3. Показать сырые Kafka JMX-метрики

Открыть:

```text
http://localhost:7071/metrics
```

Это endpoint JMX Exporter у Kafka.

Что сказать:

```text
Это не Grafana и не dashboard.
Это сырой формат метрик, который читает Prometheus.
Kafka отдаёт метрики через JMX, а JMX Exporter показывает их как HTTP /metrics.
```

В браузере нажать `Ctrl + F` и найти по очереди:

```text
activecontrollercount
```

Что объяснить:

```text
ActiveControllerCount показывает количество активных controller'ов в Kafka cluster.
В норме должен быть 1.
```

Найти:

```text
offlinepartitionscount
```

Что объяснить:

```text
OfflinePartitionsCount показывает partitions без leader'а.
В норме должен быть 0.
Если больше 0 — часть данных Kafka недоступна.
```

Найти:

```text
underreplicatedpartitions
```

Что объяснить:

```text
UnderReplicatedPartitions показывает partitions, у которых не все replicas синхронизированы.
В норме 0.
В нашем demo replication factor = 1, поэтому эта метрика обычно 0.
```

Найти:

```text
bytesinpersec
```

Что объяснить:

```text
BytesInPerSec показывает входящий поток байт в broker.
После отправки сообщений значение и rate будут меняться.
```

---

## Шаг 4. Показать ZooKeeper JMX-метрики

Открыть:

```text
http://localhost:7072/metrics
```

Что сказать:

```text
ZooKeeper тоже Java-приложение, поэтому его можно мониторить через JMX.
Здесь JMX Exporter делает то же самое: читает MBeans и отдаёт Prometheus format.
```

В браузере можно искать:

```text
zookeeper
latency
connections
```

Что объяснить:

```text
Для ZooKeeper важны latency, количество подключений, outstanding requests и состояние node.
```

---

## Шаг 5. Показать ZooKeeper AdminServer

Открыть:

```text
http://localhost:8080/commands
```

Что показывать:

```text
Это список административных команд ZooKeeper.
```

Потом открыть:

```text
http://localhost:8080/commands/stats
```

Что объяснить:

```text
Здесь ZooKeeper отдаёт статистику через HTTP.
Это не JMX, а отдельный AdminServer.
```

Потом открыть:

```text
http://localhost:8080/commands/monitor
```

Что можно отметить:

```text
avg_latency
max_latency
min_latency
num_alive_connections
outstanding_requests
open_file_descriptor_count
```

Что сказать:

```text
В лекции мы говорили, что ZooKeeper можно смотреть через JMX и через AdminServer.
Сейчас мы видим AdminServer в браузере.
```

---

## Шаг 6. Показать Grafana dashboard

Открыть:

```text
http://localhost:3000
```

Войти:

```text
login: admin
password: admin
```

Если спросит смену пароля — нажать:

```text
Skip
```

Открыть dashboard:

```text
Dashboards → Kafka Demo → Kafka Monitoring Demo: JMX + Prometheus + Grafana
```

Если слева меню скрыто, можно открыть через:

```text
Home → Dashboards
```

Что показывать на dashboard:

```text
Kafka JMX endpoint
ActiveControllerCount должно быть 1
OfflinePartitionsCount должно быть 0
UnderReplicatedPartitions должно быть 0
Broker bytes in / out
Messages in per second
Consumer lag из kafka-exporter
Requests per second by request type
```

Что объяснить:

```text
Grafana сама не собирает Kafka-метрики.
Она читает их из Prometheus.
Prometheus собирает их с JMX Exporter и Kafka Exporter.
```

---

# 6. Основная демонстрация: producer, consumer и consumer lag

Теперь покажем работу Kafka и появление lag.

## 6.1. Запустить demo-скрипт

В PowerShell из папки проекта:

```powershell
.\scripts\demo.ps1
```

Скрипт делает:

```text
1. Создаёт topic orders с 3 partitions.
2. Отправляет 100 сообщений.
3. Запускает consumer group demo-group и читает только 10 сообщений.
4. Отправляет ещё 200 сообщений.
5. Показывает lag через kafka-consumer-groups.
```

На экране появится таблица примерно такого смысла:

```text
GROUP       TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
 demo-group orders  0          ...             ...             ...
 demo-group orders  1          ...             ...             ...
 demo-group orders  2          ...             ...             ...
```

Что объяснить:

```text
LOG-END-OFFSET — последний offset в partition.
CURRENT-OFFSET — offset, до которого дочитала consumer group.
LAG — разница между ними.
```

Главная мысль:

```text
Producer записал больше сообщений, чем consumer прочитал.
Kafka помнит offset consumer group.
Поэтому появляется consumer lag.
```

## 6.2. Показать lag в Grafana

Открыть или обновить:

```text
http://localhost:3000
```

Dashboard:

```text
Dashboards → Kafka Demo → Kafka Monitoring Demo: JMX + Prometheus + Grafana
```

Найти панель:

```text
Consumer lag из kafka-exporter
```

Что сказать:

```text
Lag появился не из JMX broker'а, а из kafka-exporter.
kafka-exporter подключается к Kafka и читает информацию о topic'ах, partitions и consumer groups.
Prometheus забирает эти метрики, Grafana показывает график.
```

Если график не обновился сразу, подождать 15–30 секунд и нажать refresh в Grafana.

---

# 7. Дополнительная нагрузка для графиков

Чтобы графики `MessagesInPerSec`, `BytesInPerSec`, `RequestsPerSec` стали заметнее, можно отправить больше сообщений.

В PowerShell:

```powershell
.\scripts\produce-load.ps1 -Count 5000
```

Или сильнее:

```powershell
.\scripts\produce-load.ps1 -Count 20000
```

После этого открыть Grafana:

```text
http://localhost:3000
```

Смотреть панели:

```text
Broker bytes in / out
Messages in per second
Requests per second by request type
Consumer lag из kafka-exporter
```

Что объяснить:

```text
Когда producer отправляет сообщения, у broker'а растут входящие bytes и messages in.
Если consumer не читает эти сообщения, consumer lag растёт.
```

---

# 8. Ручные Kafka-команды для показа

Все команды выполняются из PowerShell в папке проекта.

## 8.1. Список topic'ов

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Что увидим:

```text
orders
```

Также будут служебные topic'и Kafka, например:

```text
__consumer_offsets
```

Что объяснить:

```text
__consumer_offsets — служебный topic, где Kafka хранит offsets consumer groups.
```

## 8.2. Описание topic orders

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic orders
```

Что увидим:

```text
Topic: orders
PartitionCount: 3
ReplicationFactor: 1
```

Что объяснить:

```text
Topic orders разделён на 3 partitions.
В demo replication factor = 1, потому что у нас один broker.
В production replication factor обычно 3.
```

## 8.3. Отправить одно сообщение вручную

```powershell
"order_id=manual-1;status=created" | docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:9092 --topic orders
```

Что объяснить:

```text
Это producer.
Он отправляет сообщение в topic orders.
```

## 8.4. Прочитать сообщения вручную

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 5
```

Что объяснить:

```text
Это consumer без group.
Он читает сообщения из topic.
```

## 8.5. Посмотреть consumer group lag

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group demo-group
```

Что объяснить:

```text
Это табличное представление lag.
Grafana показывает то же самое в виде графика.
```

---

# 9. Ручные Prometheus-запросы

Открыть:

```text
http://localhost:9090/graph
```

В поле query можно вставлять запросы.

## 9.1. Проверить, что targets живы

```promql
up
```

Что объяснить:

```text
up = 1 означает, что Prometheus успешно собрал target.
up = 0 означает, что target недоступен.
```

## 9.2. Найти Kafka controller

Попробовать найти метрику через вкладку Graph/Console и autocomplete.

Часто она называется примерно так после JMX Exporter:

```promql
kafka_controller_kafkacontroller_activecontrollercount
```

Смысл:

```text
Должно быть 1.
```

## 9.3. Consumer lag

Для kafka-exporter:

```promql
kafka_consumergroup_lag
```

Что объяснить:

```text
Это lag consumer group по topic/partition.
```

Суммарный lag по group:

```promql
sum(kafka_consumergroup_lag) by (consumergroup)
```

## 9.4. Kafka exporter availability

```promql
up{job="kafka-exporter"}
```

Смысл:

```text
Prometheus видит kafka-exporter.
```

---

# 10. Как показать JMX Exporter на примере Kafka

Открыть:

```text
http://localhost:7071/metrics
```

Сказать:

```text
Внутри Kafka метрики существуют как JMX MBeans.
Prometheus напрямую JMX не читает.
Поэтому мы запускаем JMX Exporter как Java Agent.
Он читает JMX и отдаёт HTTP endpoint /metrics.
```

В docker-compose это видно в Kafka service:

```yaml
KAFKA_OPTS: "-javaagent:/usr/share/java/jmx_prometheus_javaagent.jar=7071:/etc/jmx/kafka.yml"
```

Объяснение:

```text
7071 — порт JMX Exporter.
/etc/jmx/kafka.yml — конфигурация правил, какие MBeans преобразовывать в Prometheus-метрики.
```

---

# 11. Как показать ZooKeeper AdminServer

Открыть:

```text
http://localhost:8080/commands
```

Сказать:

```text
Это ZooKeeper AdminServer.
Он работает через HTTP.
Это не JMX.
```

Открыть:

```text
http://localhost:8080/commands/monitor
```

Сказать:

```text
Здесь видны monitoring-поля ZooKeeper: latency, connections, requests, file descriptors.
```

Связать с лекцией:

```text
ZooKeeper можно смотреть через JMX, а можно через AdminServer.
В production эти данные обычно забираются exporter'ом и попадают в Prometheus.
```

---

# 12. Как показать отличие Whitebox и Blackbox

## Whitebox

Открыть:

```text
http://localhost:7071/metrics
```

Сказать:

```text
Это whitebox: мы смотрим внутренние метрики Kafka.
```

Примеры whitebox-метрик:

```text
ActiveControllerCount
OfflinePartitionsCount
UnderReplicatedPartitions
BytesInPerSec
MessagesInPerSec
```

## Blackbox / внешняя проверка

Показать, что порт Kafka доступен с Windows:

```powershell
Test-NetConnection localhost -Port 29092
```

Что объяснить:

```text
Это внешняя проверка доступности порта.
Она говорит, что порт открыт, но не показывает внутреннее здоровье Kafka.
```

Проверка HTTP endpoint:

```powershell
Invoke-WebRequest http://localhost:7071/metrics | Select-Object -ExpandProperty StatusCode
```

Ожидаемый результат:

```text
200
```

Что сказать:

```text
Blackbox говорит: endpoint доступен.
Whitebox говорит: что происходит внутри.
```

---

# 13. Что именно говорить по ключевым метрикам

## ActiveControllerCount

Где показать:

```text
Grafana dashboard
или http://localhost:7071/metrics
```

Что говорить:

```text
В Kafka cluster должен быть один active controller.
Норма: 1.
Alert: значение не равно 1.
```

## OfflinePartitionsCount

Где показать:

```text
Grafana dashboard
или http://localhost:7071/metrics
```

Что говорить:

```text
Offline partition — это partition без leader'а.
Норма: 0.
Alert: больше 0.
```

## UnderReplicatedPartitions

Где показать:

```text
Grafana dashboard
или http://localhost:7071/metrics
```

Что говорить:

```text
Показывает partitions, у которых не все replicas синхронизированы.
В demo replication factor = 1, поэтому значение обычно 0.
В production это одна из главных метрик здоровья replication.
```

## BytesIn / BytesOut

Где показать:

```text
Grafana dashboard
после .\scripts\produce-load.ps1 -Count 5000
```

Что говорить:

```text
BytesIn растёт, когда producer пишет данные.
BytesOut растёт, когда consumer читает данные.
```

## Consumer lag

Где показать:

```text
PowerShell: kafka-consumer-groups
Grafana: Consumer lag из kafka-exporter
Prometheus: kafka_consumergroup_lag
```

Что говорить:

```text
Lag — это разница между последним offset в Kafka и offset consumer group.
Если lag растёт, consumer не успевает.
```

---

# 14. Очистка demo-данных

Если нужно повторить демонстрацию заново:

```powershell
.\scripts\cleanup.ps1
```

Скрипт удаляет topic `orders`.

Потом можно снова запустить:

```powershell
.\scripts\demo.ps1
```

---

# 15. Полная остановка стенда

Остановить контейнеры:

```powershell
docker compose down
```

Остановить и удалить volumes, если нужно полностью очистить состояние:

```powershell
docker compose down -v
```

Если Docker занял много места, можно посмотреть:

```powershell
docker system df
```

И при необходимости очистить неиспользуемые ресурсы:

```powershell
docker system prune
```

Осторожно: `docker system prune` удаляет неиспользуемые Docker-ресурсы не только этого проекта.

---

# 16. Частые проблемы

## Проблема: порт уже занят

Симптом:

```text
port is already allocated
```

Что делать:

Проверить, кто занимает порт, например:

```powershell
netstat -ano | findstr :3000
netstat -ano | findstr :9090
netstat -ano | findstr :29092
```

Или поменять порт в `docker-compose.yml`.

Например, если занят Grafana port `3000`, заменить:

```yaml
- "3000:3000"
```

на:

```yaml
- "3001:3000"
```

Тогда Grafana будет открываться:

```text
http://localhost:3001
```

## Проблема: Grafana открылась, но dashboard пустой

Что сделать:

```text
1. Открыть http://localhost:9090/targets
2. Проверить, что targets имеют статус UP
3. Подождать 30–60 секунд
4. Обновить Grafana
```

Также проверьте в Grafana правый верхний угол: период времени должен быть, например:

```text
Last 15 minutes
```

## Проблема: consumer lag не виден сразу

Что сделать:

```text
1. Запустить .\scripts\demo.ps1
2. Подождать 15–30 секунд
3. Обновить Grafana
4. Проверить вручную командой kafka-consumer-groups
```

Команда:

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group demo-group
```

## Проблема: Kafka ещё не готова

Симптом:

```text
Connection refused
Broker not available
```

Что сделать:

```powershell
docker compose logs -f kafka
```

Подождать, пока broker полностью стартует.

## Проблема: PowerShell не запускает скрипт

Симптом:

```text
running scripts is disabled on this system
```

Можно запустить так:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\demo.ps1
```

Или для текущей PowerShell-сессии:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

После этого:

```powershell
.\scripts\demo.ps1
```

---

# 17. Краткая шпаргалка лектора

Запуск:

```powershell
cd $HOME\Downloads\kafka-monitoring-win-demo
docker compose up -d --build
docker compose ps
```

Открыть по порядку:

```text
1. Prometheus targets:
   http://localhost:9090/targets

2. Kafka raw metrics:
   http://localhost:7071/metrics

3. ZooKeeper AdminServer:
   http://localhost:8080/commands/monitor

4. Grafana:
   http://localhost:3000
   admin / admin
```

Запустить demo:

```powershell
.\scripts\demo.ps1
```

Показать lag вручную:

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group demo-group
```

Дать нагрузку:

```powershell
.\scripts\produce-load.ps1 -Count 5000
```

Повторить demo с нуля:

```powershell
.\scripts\cleanup.ps1
.\scripts\demo.ps1
```

Остановить:

```powershell
docker compose down
```

Полностью очистить:

```powershell
docker compose down -v
```

---

# 18. Что студент должен понять после демонстрации

После демонстрации нужно зафиксировать основные идеи:

```text
Kafka отдаёт внутренние метрики через JMX.
JMX Exporter превращает JMX в Prometheus /metrics.
Prometheus собирает метрики по pull-модели.
Grafana показывает dashboard на основе Prometheus.
ZooKeeper можно диагностировать через JMX и AdminServer.
Consumer lag показывает отставание consumer group.
Broker metrics и server metrics нужно смотреть вместе.
```

Короткая итоговая схема:

```text
Producer пишет сообщения в Kafka.
Consumer читает сообщения из Kafka.
Kafka хранит offsets consumer group.
Если consumer читает медленнее, чем producer пишет, появляется lag.
Kafka/JMX Exporter/Prometheus/Grafana позволяют это увидеть.
```
