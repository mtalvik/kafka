#!/usr/bin/env bash
# Bootstrap a single-node Kafka 4.3 broker on Ubuntu 24.04 with SASL/PLAIN
# and StandardAuthorizer. Idempotent — safe to run multiple times.
#
# Usage:
#   ./setup.sh
#
# Requires the following files alongside this script:
#   - server.properties
#   - kafka_server_jaas.conf.example  (will be copied to kafka_server_jaas.conf)
#   - kafka.service
#
# Run as the ubuntu user. Assumes a fresh Ubuntu 24.04 EC2 instance with
# sudo access. Will install OpenJDK 17 and download Kafka if not present.

set -euo pipefail

KAFKA_VERSION="4.3.0"
KAFKA_SCALA="2.13"
KAFKA_HOME="/home/ubuntu/kafka"
KAFKA_DATA="/home/ubuntu/kafka/data"
KAFKA_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { echo "[setup] $*" >&2; }
err()  { echo "[setup] ERROR: $*" >&2; }
die()  { err "$*"; exit 1; }

# --- Prerequisites --------------------------------------------------------

log "Installing OpenJDK 17 if missing"
if ! command -v java >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq openjdk-17-jre-headless
fi

# --- Kafka distribution ---------------------------------------------------

if [[ ! -d "${KAFKA_HOME}/bin" ]]; then
  log "Downloading Kafka ${KAFKA_VERSION}"
  cd /tmp
  curl -fsSL "${KAFKA_URL}" -o kafka.tgz
  tar xzf kafka.tgz
  mv "kafka_${KAFKA_SCALA}-${KAFKA_VERSION}" "${KAFKA_HOME}"
  rm kafka.tgz
else
  log "Kafka already installed at ${KAFKA_HOME}"
fi

# --- Configuration --------------------------------------------------------

log "Installing server.properties"
cp "${INFRA_DIR}/server.properties" "${KAFKA_HOME}/config/server.properties"

log "Installing JAAS configuration"
if [[ ! -f "${KAFKA_HOME}/config/kafka_server_jaas.conf" ]]; then
  cp "${INFRA_DIR}/kafka_server_jaas.conf.example" \
     "${KAFKA_HOME}/config/kafka_server_jaas.conf"
  err "Edit ${KAFKA_HOME}/config/kafka_server_jaas.conf and replace placeholders"
  err "before running the broker."
fi

# --- Data directory and storage format -----------------------------------

mkdir -p "${KAFKA_DATA}"

if [[ ! -f "${KAFKA_DATA}/meta.properties" ]]; then
  log "Formatting KRaft storage in ${KAFKA_DATA}"
  CLUSTER_ID=$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)
  "${KAFKA_HOME}/bin/kafka-storage.sh" format \
    -t "${CLUSTER_ID}" \
    -c "${KAFKA_HOME}/config/server.properties" \
    --standalone
  log "Storage formatted with cluster ID: ${CLUSTER_ID}"
else
  log "KRaft storage already formatted (skipping)"
fi

# --- systemd unit ---------------------------------------------------------

log "Installing systemd unit"
sudo cp "${INFRA_DIR}/kafka.service" /etc/systemd/system/kafka.service
sudo systemctl daemon-reload
sudo systemctl enable kafka

# --- Start ----------------------------------------------------------------

log "Starting Kafka"
sudo systemctl start kafka
sleep 10

if systemctl is-active --quiet kafka; then
  log "Kafka is running"
else
  die "Kafka failed to start. Check: journalctl -u kafka --no-pager | tail -50"
fi

# --- Smoke test -----------------------------------------------------------

if [[ -f "${KAFKA_HOME}/kafka-configs/clients/admin.properties" ]]; then
  log "Running smoke test against broker"
  "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server "$(grep '^advertised.listeners' "${KAFKA_HOME}/config/server.properties" \
                          | sed 's|.*://||' | cut -d, -f1)" \
    --command-config "${KAFKA_HOME}/kafka-configs/clients/admin.properties" \
    | head -1
fi

log "Done. Broker is up on port 9092."
