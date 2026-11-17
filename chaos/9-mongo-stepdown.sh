#!/usr/bin/env bash
# Retryable writes and transaction retries must ride out a primary election; Debezium resumes
# from its resume token. No effect is lost or doubled.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

banner "scenario 9: MongoDB primary steps down during load"
wallet=$(new_wallet 5000000)
place_orders "$wallet" 30 49900 &
load=$!
sleep 3
primary=$(docker exec mongo-1 mongosh --quiet --eval 'db.hello().primary' | tr -d '\r')
echo "current primary: $primary"
docker exec mongo-1 mongosh --quiet --eval 'try { rs.stepDown(30) } catch (e) { print(e.message) }'
wait $load || true

DEADLINE=360 "$(dirname "$0")/verify.sh"
