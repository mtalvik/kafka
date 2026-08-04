#!/bin/sh
set -eu

URL="http://schema-registry:8081"
SUBJECT="users-value"

echo "Global compatibility config"
curl -sS "$URL/config"
echo

echo "Subject compatibility config for $SUBJECT"
curl -sS "$URL/config/$SUBJECT"
echo
