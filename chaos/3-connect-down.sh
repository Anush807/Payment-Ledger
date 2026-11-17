#!/usr/bin/env bash
# Outbox documents accumulate while Debezium is down, then drain. No event is lost.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

banner "scenario 3: Kafka Connect (Debezium) down for 60s"
wallet=$(new_wallet 5000000)
$COMPOSE stop connect
place_orders "$wallet" 20 49900
echo "outbox documents are piling up in MongoDB; nothing reaches Kafka yet"
sleep 60
$COMPOSE start connect
until curl -fsS http://localhost:8083/connectors > /dev/null 2>&1; do sleep 3; done

DEADLINE=360 "$(dirname "$0")/verify.sh"
