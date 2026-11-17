#!/usr/bin/env bash
# min.insync.replicas=2 of 3: producers and consumers continue, no message is lost.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

banner "scenario 4: one Kafka broker killed"
wallet=$(new_wallet 5000000)
place_orders "$wallet" 20 49900 &
load=$!
sleep 2
$COMPOSE kill kafka-2
wait $load || true
place_orders "$wallet" 10 49900
$COMPOSE start kafka-2

DEADLINE=360 "$(dirname "$0")/verify.sh"
