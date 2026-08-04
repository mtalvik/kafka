#!/bin/sh
set -eu

URL="http://schema-registry:8081"
SUBJECT="users-value"

echo "Check intentionally incompatible v3 against latest version of $SUBJECT"
curl -sS -X POST "$URL/compatibility/subjects/$SUBJECT/versions/latest" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @/demo/schemas/register-user-v3-incompatible.json
echo
