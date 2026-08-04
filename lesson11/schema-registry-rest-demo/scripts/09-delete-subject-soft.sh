#!/bin/sh
set -eu

URL="http://schema-registry:8081"
SUBJECT="users-value"

echo "Soft-delete subject: $SUBJECT"
curl -sS -X DELETE "$URL/subjects/$SUBJECT"
echo

echo "Subjects after soft delete"
curl -sS "$URL/subjects"
echo
