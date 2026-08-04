#!/bin/sh
set -eu

URL="http://schema-registry:8081"

echo "Waiting for Schema Registry at $URL ..."

i=1
while [ "$i" -le 60 ]; do
  if curl -fsS "$URL/subjects" >/dev/null 2>&1; then
    echo "Schema Registry is ready."
    curl -sS "$URL/subjects"
    echo
    exit 0
  fi

  echo "Attempt $i/60: not ready yet"
  i=$((i + 1))
  sleep 2
done

echo "Schema Registry did not become ready in time."
exit 1
