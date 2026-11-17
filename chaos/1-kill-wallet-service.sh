#!/usr/bin/env bash
# Redelivered events must be skipped as duplicates: no double holds, no double charge.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

banner "scenario 1: wallet-service killed mid-batch"
wallet=$(new_wallet 5000000)
place_orders "$wallet" 40 49900 &
load=$!
sleep 3
$COMPOSE kill wallet-service
sleep 5
$COMPOSE start wallet-service
wait $load || true
wait_for_health 8082

"$(dirname "$0")/verify.sh"
