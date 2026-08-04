#!/bin/sh
set -eu

URL="http://schema-registry:8081"

echo "List all subjects"
curl -sS "$URL/subjects"
echo
