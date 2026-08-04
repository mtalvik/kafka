# Kafka Schema Registry REST API Demo

Учебный пример для темы **Schema Registry REST API**.

Цель: поднять Kafka и Schema Registry в Docker, а затем руками выполнить REST-запросы к Schema Registry: зарегистрировать схему, посмотреть subject, получить последнюю версию, проверить совместимость и увидеть пример несовместимой схемы.

Все запускается на Windows, если установлен только Docker Desktop.

## Что внутри

```text
schema-registry-rest-demo/
  docker-compose.yml
  README.md
  schemas/
    user-v1.avsc
    user-v2-compatible.avsc
    user-v3-incompatible.avsc
    register-user-v1.json
    register-user-v2-compatible.json
    register-user-v3-incompatible.json
  scripts/
    00-wait-for-schema-registry.sh
    01-register-v1.sh
    02-list-subjects.sh
    03-get-latest.sh
    04-check-compatible-v2.sh
    05-register-v2.sh
    06-check-incompatible-v3.sh
    07-show-config.sh
    08-set-backward.sh
    09-delete-subject-soft.sh
```

## Какие контейнеры запускаются

В `docker-compose.yml` описаны три сервиса.

| Сервис | Что делает |
|---|---|
| `kafka` | Одноузловой Kafka broker в KRaft-режиме, без ZooKeeper |
| `schema-registry` | Confluent Schema Registry, доступен на `http://localhost:8081` |
| `tools` | Контейнер с `curl`, из него запускаются учебные REST-скрипты |

Почему есть отдельный контейнер `tools`?

На Windows у пользователя может не быть `curl`, Bash, Java, Maven или Git Bash. Поэтому все учебные команды выполняются внутри Docker-контейнера. На хосте нужен только Docker Desktop.

## Быстрый старт на Windows

Откройте PowerShell или Windows Terminal в папке проекта:

```powershell
cd schema-registry-rest-demo
```

Запустите контейнеры:

```powershell
docker compose up -d
```

Если у вас старая версия Docker Compose, команда может называться так:

```powershell
docker-compose up -d
```

Проверьте, что контейнеры поднялись:

```powershell
docker compose ps
```

Ожидаем примерно такую картину:

```text
kafka                    running
schema-registry          running
schema-registry-tools    running
```

Подождите готовности Schema Registry:

```powershell
docker compose exec tools sh /demo/scripts/00-wait-for-schema-registry.sh
```

Если все хорошо, в конце будет:

```text
Schema Registry is ready.
[]
```

Пустой массив `[]` означает, что Schema Registry работает, но пока ни одной схемы не зарегистрировано.

## Основные понятия перед практикой

Kafka хранит сообщения как байты. Она сама не знает, что внутри сообщения: JSON, Avro, Protobuf, строка или бинарный объект.

Schema Registry нужен, чтобы хранить **схемы сообщений**. Схема описывает структуру данных: какие поля есть, какие у них типы, какие поля обязательны, какие имеют значения по умолчанию.

В этом примере мы используем Avro-схемы.

Главный subject в лабораторной:

```text
users-value
```

Это означает: схема для `value` сообщений условного Kafka-топика `users`.

Subject можно воспринимать как контейнер, внутри которого лежат версии одной логической схемы:

```text
users-value
  version 1
  version 2
  version 3
```

## Шаг 1. Зарегистрировать первую схему

Выполните:

```powershell
docker compose exec tools sh /demo/scripts/01-register-v1.sh
```

Скрипт вызывает endpoint:

```http
POST /subjects/users-value/versions
```

Он регистрирует схему `schemas/user-v1.avsc`.

Схема выглядит так:

```json
{
  "type": "record",
  "name": "User",
  "namespace": "demo.kafka",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    }
  ]
}
```

Пример ответа:

```json
{"id":1}
```

Это значит, что Schema Registry зарегистрировал схему и выдал ей глобальный schema ID.

Важно:

`id` и `version` не одно и то же.

| Понятие | Значение |
|---|---|
| `id` | Глобальный ID схемы во всем Schema Registry |
| `version` | Номер версии схемы внутри конкретного subject |

## Шаг 2. Посмотреть список subject

Выполните:

```powershell
docker compose exec tools sh /demo/scripts/02-list-subjects.sh
```

Скрипт вызывает:

```http
GET /subjects
```

Ожидаемый ответ:

```json
["users-value"]
```

Теперь в Schema Registry есть один subject:

```text
users-value
```

## Шаг 3. Получить последнюю версию схемы

Выполните:

```powershell
docker compose exec tools sh /demo/scripts/03-get-latest.sh
```

Скрипт вызывает:

```http
GET /subjects/users-value/versions/latest
```

Ответ будет примерно таким:

```json
{
  "subject": "users-value",
  "version": 1,
  "id": 1,
  "schema": "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"demo.kafka\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}"
}
```

Здесь видно:

| Поле | Что означает |
|---|---|
| `subject` | Имя subject |
| `version` | Версия схемы внутри subject |
| `id` | Глобальный schema ID |
| `schema` | Сама Avro-схема, записанная строкой |

Обратите внимание: поле `schema` в REST API приходит как строка с экранированным JSON.

## Шаг 4. Проверить совместимую схему v2

Теперь представим, что бизнес попросил добавить пользователю email.

Новая схема `schemas/user-v2-compatible.avsc`:

```json
{
  "type": "record",
  "name": "User",
  "namespace": "demo.kafka",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "email",
      "type": "string",
      "default": ""
    }
  ]
}
```

Главная деталь:

```json
{
  "name": "email",
  "type": "string",
  "default": ""
}
```

Поле `email` добавлено со значением по умолчанию.

Это важно, потому что в Kafka уже могут лежать старые сообщения, где поля `email` нет. Если новая схема умеет подставить `default`, она сможет читать старые данные.

Проверим совместимость:

```powershell
docker compose exec tools sh /demo/scripts/04-check-compatible-v2.sh
```

Скрипт вызывает:

```http
POST /compatibility/subjects/users-value/versions/latest
```

Ожидаемый ответ:

```json
{"is_compatible":true}
```

Это значит: новую схему можно регистрировать как следующую версию.

## Шаг 5. Зарегистрировать совместимую схему v2

Выполните:

```powershell
docker compose exec tools sh /demo/scripts/05-register-v2.sh
```

Ожидаемый ответ:

```json
{"id":2}
```

Теперь внутри subject `users-value` есть две версии:

```text
users-value
  version 1: id + name
  version 2: id + name + email
```

Можно снова посмотреть latest:

```powershell
docker compose exec tools sh /demo/scripts/03-get-latest.sh
```

Теперь `version` должен быть `2`.

## Шаг 6. Проверить несовместимую схему v3

Теперь посмотрим на плохое изменение.

Схема `schemas/user-v3-incompatible.avsc` меняет тип поля `id`:

Было:

```json
{
  "name": "id",
  "type": "int"
}
```

Стало:

```json
{
  "name": "id",
  "type": "string"
}
```

Это опасное изменение. Старые сообщения содержат `id` как число, а новая схема ожидает строку.

Проверим:

```powershell
docker compose exec tools sh /demo/scripts/06-check-incompatible-v3.sh
```

Ожидаемый ответ:

```json
{"is_compatible":false}
```

Это главный смысл Schema Registry: он не просто хранит схемы, а помогает не сломать контракт между producer и consumer.

## Шаг 7. Посмотреть настройки compatibility

Выполните:

```powershell
docker compose exec tools sh /demo/scripts/07-show-config.sh
```

Скрипт вызывает два endpoint:

```http
GET /config
GET /config/users-value
```

Глобальная настройка обычно выглядит так:

```json
{"compatibilityLevel":"BACKWARD"}
```

Для subject может вернуться ошибка, если отдельная настройка еще не задана. Это нормально: тогда subject использует глобальное значение.

## Шаг 8. Явно установить BACKWARD для subject

Выполните:

```powershell
docker compose exec tools sh /demo/scripts/08-set-backward.sh
```

Скрипт вызывает:

```http
PUT /config/users-value
```

И отправляет тело:

```json
{"compatibility":"BACKWARD"}
```

Ожидаемый ответ:

```json
{"compatibility":"BACKWARD"}
```

Теперь именно для `users-value` задан режим `BACKWARD`.

## Что значит BACKWARD

`BACKWARD` означает:

> Новая схема должна уметь читать данные, записанные старой схемой.

Это очень частый режим для Kafka, потому что сообщения могут долго лежать в топике. Consumer обновился сегодня, а читает сообщения, которые producer записал вчера или неделю назад.

Пример совместимого изменения:

```text
добавить новое поле с default
```

Пример несовместимого изменения:

```text
поменять тип существующего поля int -> string
```

## Как посмотреть Schema Registry из браузера

После запуска можно открыть:

```text
http://localhost:8081/subjects
```

Браузер должен показать JSON со списком subject.

Например:

```json
["users-value"]
```

Также можно открыть:

```text
http://localhost:8081/subjects/users-value/versions/latest
```

И увидеть последнюю версию схемы.

## Как очистить пример

Остановить контейнеры:

```powershell
docker compose down
```

Остановить контейнеры и удалить данные Kafka:

```powershell
docker compose down -v
```

Если хотите удалить только subject через REST API:

```powershell
docker compose exec tools sh /demo/scripts/09-delete-subject-soft.sh
```

Для учебного повторения с нуля проще выполнить:

```powershell
docker compose down -v
docker compose up -d
```

## Частые проблемы

### Команда `docker compose` не найдена

Попробуйте старый синтаксис:

```powershell
docker-compose up -d
```

Если не работает и он, проверьте, что установлен Docker Desktop.

### Schema Registry не готов сразу после запуска

Это нормально. Kafka должна стартовать первой, потом Schema Registry подключается к Kafka.

Используйте:

```powershell
docker compose exec tools sh /demo/scripts/00-wait-for-schema-registry.sh
```

### Порт 8081 занят

В `docker-compose.yml` поменяйте строку:

```yaml
ports:
  - "8081:8081"
```

например на:

```yaml
ports:
  - "18081:8081"
```

Тогда с Windows-хоста Schema Registry будет доступен по адресу:

```text
http://localhost:18081
```

Но внутри контейнеров адрес останется прежним:

```text
http://schema-registry:8081
```

Скрипты менять не надо.

## Мини-конспект

Schema Registry REST API позволяет управлять схемами через HTTP.

Главные endpoint:

| Endpoint | Что делает |
|---|---|
| `GET /subjects` | Показывает все subject |
| `POST /subjects/{subject}/versions` | Регистрирует новую версию схемы |
| `GET /subjects/{subject}/versions/latest` | Возвращает последнюю версию схемы |
| `POST /compatibility/subjects/{subject}/versions/latest` | Проверяет совместимость новой схемы |
| `GET /config` | Показывает глобальный compatibility mode |
| `PUT /config/{subject}` | Настраивает compatibility для subject |

Главная идея:

Kafka хранит сообщения, а Schema Registry хранит контракт этих сообщений.

Subject `users-value` означает:

> Это набор версий схемы для value сообщений условного топика `users`.

Совместимая эволюция:

```text
добавили поле email с default
```

Несовместимая эволюция:

```text
поменяли id с int на string
```
