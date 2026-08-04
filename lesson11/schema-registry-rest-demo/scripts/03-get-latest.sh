#!/bin/sh
set -eu

URL="http://schema-registry:8081"
SUBJECT="users-value"

echo "Get latest schema version for subject: $SUBJECT"
curl -sS "$URL/subjects/$SUBJECT/versions/latest"
echo
