#!/bin/sh
set -eu

URL="http://schema-registry:8081"
SUBJECT="users-value"

echo "Check that v2 is compatible with the latest version of $SUBJECT"
curl -sS -X POST "$URL/compatibility/subjects/$SUBJECT/versions/latest" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @/demo/schemas/register-user-v2-compatible.json
echo
