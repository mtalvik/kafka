#!/bin/sh
set -eu

URL="http://schema-registry:8081"
SUBJECT="users-value"

echo "Set BACKWARD compatibility for subject: $SUBJECT"
curl -sS -X PUT "$URL/config/$SUBJECT" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}'
echo
