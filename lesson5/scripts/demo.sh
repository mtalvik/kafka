#!/usr/bin/env bash
# Kafka Monitoring Demo — bash version of demo.ps1
# Цель: создать топик orders, налить 100 сообщений, прочитать 10 (offset зафиксируется),
# налить ещё 200, показать lag через kafka-consumer-groups.

set -euo pipefail

NOJMX='unset KAFKA_JMX_OPTS KAFKA_JMX_PORT JMX_PORT;'
BOOTSTRAP='kafka:9092'
TOPIC='orders'
GROUP='demo-group'

cyan()  { printf '\033[36m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }

cyan "Создаём топик ${TOPIC} (3 partitions, RF=1)"
docker-compose exec -T kafka bash -lc "${NOJMX} kafka-topics --bootstrap-server ${BOOTSTRAP} \
  --create --if-not-exists --topic ${TOPIC} --partitions 3 --replication-factor 1"

cyan "Список топиков"
docker-compose exec -T kafka bash -lc "${NOJMX} kafka-topics --bootstrap-server ${BOOTSTRAP} --list"

cyan "Шлём 100 сообщений в ${TOPIC}"
ts=$(date -u +%FT%TZ)
seq 1 100 | awk -v ts="$ts" '{printf "order_id=%s;status=created;ts=%s\n",$1,ts}' \
  | docker-compose exec -T kafka bash -lc "${NOJMX} kafka-console-producer --bootstrap-server ${BOOTSTRAP} --topic ${TOPIC}"

cyan "Читаем 10 сообщений группой ${GROUP} (зафиксируем committed offset)"
docker-compose exec -T kafka bash -lc "${NOJMX} kafka-console-consumer --bootstrap-server ${BOOTSTRAP} \
  --topic ${TOPIC} --group ${GROUP} --from-beginning --max-messages 10"

cyan "Шлём ещё 200 сообщений — group ${GROUP} начнёт отставать"
ts=$(date -u +%FT%TZ)
seq 101 300 | awk -v ts="$ts" '{printf "order_id=%s;status=created;ts=%s\n",$1,ts}' \
  | docker-compose exec -T kafka bash -lc "${NOJMX} kafka-console-producer --bootstrap-server ${BOOTSTRAP} --topic ${TOPIC}"

cyan "Lag через kafka-consumer-groups"
docker-compose exec -T kafka bash -lc "${NOJMX} kafka-consumer-groups --bootstrap-server ${BOOTSTRAP} \
  --describe --group ${GROUP}"

green "Done. Grafana: http://localhost:3000  (admin/admin)"
