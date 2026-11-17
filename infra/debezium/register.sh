#!/bin/sh
# Registers one outbox connector per service. Idempotent: PUT replaces an existing config.
set -eu
CONNECT=${CONNECT:-http://connect:8083}
DIR=$(dirname "$0")   # /connectors in the container, infra/debezium from the host

for file in "$DIR"/*.json; do
  name=$(basename "$file" .json)
  echo "registering connector $name"
  curl -fsS -X PUT -H "Content-Type: application/json" \
    --data-binary "@$file" "$CONNECT/connectors/$name/config" > /dev/null
done

curl -fsS "$CONNECT/connectors" && echo " connectors registered"
