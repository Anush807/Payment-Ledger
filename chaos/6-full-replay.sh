#!/usr/bin/env bash
# Resetting wallet-service to the earliest offset must change no balance: only
# duplicate_events_skipped_total rises.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

banner "scenario 6: full replay of wallet-service's consumer group"
wallet=$(new_wallet 5000000)
place_orders "$wallet" 10 49900
sleep 20
"$(dirname "$0")/verify.sh"

before=$(api GET "$WALLET/wallets/$wallet" | jq -r .balanceMinor)
skipped_before=$(curl -fsS http://localhost:8082/actuator/prometheus \
  | awk '/^duplicate_events_skipped_total/ {sum += $2} END {print sum + 0}')

$COMPOSE stop wallet-service
docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 \
  --group wallet-service --reset-offsets --to-earliest --all-topics --execute
$COMPOSE start wallet-service
wait_for_health 8082
sleep 30

after=$(api GET "$WALLET/wallets/$wallet" | jq -r .balanceMinor)
skipped_after=$(curl -fsS http://localhost:8082/actuator/prometheus \
  | awk '/^duplicate_events_skipped_total/ {sum += $2} END {print sum + 0}')
echo "balance before=$before after=$after; duplicates skipped $skipped_before -> $skipped_after"
[ "$before" = "$after" ] || { echo "balance changed on replay" >&2; exit 1; }

"$(dirname "$0")/verify.sh"
