#!/usr/bin/env bash
# Orders older than saga.timeout cancel via the pivot; the rest complete; nothing is stuck.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

banner "scenario 2: payment-service down for 60s"
wallet=$(new_wallet 5000000)
place_orders "$wallet" 20 49900
$COMPOSE stop payment-service
sleep 60
place_orders "$wallet" 10 49900
$COMPOSE start payment-service
wait_for_health 8083

DEADLINE=360 "$(dirname "$0")/verify.sh"
