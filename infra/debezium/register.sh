#!/bin/sh
# Registers one outbox connector per service. Idempotent: PUT replaces an existing config.
set -eu
CONNECT=${CONNECT:-http://connect:8083}

for file in /connectors/*.json; do
  name=$(basename "$file" .json)
  echo "registering connector $name"
  curl -fsS -X PUT -H "Content-Type: application/json" \
    --data-binary "@$file" "$CONNECT/connectors/$name/config" > /dev/null
done

curl -fsS "$CONNECT/connectors" && echo " connectors registered"
