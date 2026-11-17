#!/usr/bin/env bash
# After the pivot there is no cancellation, only forward recovery: orders wait in
# PAYMENT_AUTHORIZED and complete once the ledger is back.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

banner "scenario 5: ledger-service down for 2 minutes after authorizations"
wallet=$(new_wallet 5000000)
$COMPOSE stop ledger-service
place_orders "$wallet" 20 49900
sleep 30
api GET "$ORDER/orders/summary" | jq '.countByStatus'
echo "expect PAYMENT_AUTHORIZED orders above, and no CANCELLED ones from the timeout"
sleep 90
$COMPOSE start ledger-service
wait_for_health 8084

DEADLINE=360 "$(dirname "$0")/verify.sh"
