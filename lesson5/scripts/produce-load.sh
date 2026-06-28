#!/usr/bin/env bash
# Produce-load — bash version of produce-load.ps1
# Usage: ./produce-load.sh [count] [topic]
#   count: количество сообщений (default 1000)
#   topic: топик (default orders)

set -euo pipefail

COUNT="${1:-1000}"
TOPIC="${2:-orders}"
NOJMX='unset KAFKA_JMX_OPTS KAFKA_JMX_PORT JMX_PORT;'
BOOTSTRAP='kafka:9092'

cyan()  { printf '\033[36m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }

cyan "Шлём ${COUNT} сообщений в топик ${TOPIC}"

seq 1 "${COUNT}" | awk '{printf "order_id=%s;status=load-test;payload=load-%d\n",$1,$1}' \
  | docker-compose exec -T kafka bash -lc "${NOJMX} kafka-console-producer --bootstrap-server ${BOOTSTRAP} --topic ${TOPIC}"

green "Готово. Посмотри в Grafana: http://localhost:3000"
