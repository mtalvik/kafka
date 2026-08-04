#!/bin/sh
set -eu

URL="http://schema-registry:8081"
SUBJECT="users-value"

echo "Register compatible Avro schema v2 into subject: $SUBJECT"
curl -sS -X POST "$URL/subjects/$SUBJECT/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @/demo/schemas/register-user-v2-compatible.json
echo
