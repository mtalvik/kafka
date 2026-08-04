#!/bin/sh
set -eu

URL="http://schema-registry:8081"
SUBJECT="users-value"

echo "Register Avro schema v1 into subject: $SUBJECT"
curl -sS -X POST "$URL/subjects/$SUBJECT/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @/demo/schemas/register-user-v1.json
echo
