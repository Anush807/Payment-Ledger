#!/usr/bin/env bash
# Shared helpers for the chaos scenarios.
set -euo pipefail

API_KEY=${API_KEY:-dev-key}
ORDER=${ORDER:-http://localhost:8081/api/v1}
WALLET=${WALLET:-http://localhost:8082/api/v1}
PAYMENT=${PAYMENT:-http://localhost:8083/api/v1}
LEDGER=${LEDGER:-http://localhost:8084/api/v1}
NOTIFICATION=${NOTIFICATION:-http://localhost:8085/api/v1}
COMPOSE="docker compose -f $(dirname "${BASH_SOURCE[0]}")/../infra/docker-compose.yml"

api() { # api METHOD URL [BODY]
  local method=$1 url=$2 body=${3:-}
  if [ -n "$body" ]; then
    curl -fsS -X "$method" "$url" \
      -H "X-Api-Key: $API_KEY" -H "Content-Type: application/json" \
      -H "Idempotency-Key: $(uuidgen)" --data "$body"
  else
    curl -fsS -X "$method" "$url" -H "X-Api-Key: $API_KEY"
  fi
}

new_wallet() { # new_wallet AMOUNT_MINOR -> walletId
  local amount=$1 id
  id=$(api POST "$WALLET/wallets" '{"ownerId":"chaos","currency":"INR"}' | jq -r .walletId)
  api POST "$WALLET/wallets/$id/top-ups" "{\"amountMinor\":$amount}" > /dev/null
  echo "$id"
}

place_orders() { # place_orders WALLET_ID COUNT AMOUNT_MINOR
  local wallet=$1 count=$2 amount=$3
  for _ in $(seq 1 "$count"); do
    api POST "$ORDER/orders" \
      "{\"walletId\":\"$wallet\",\"merchantId\":\"m-42\",\"amountMinor\":$amount,\"currency\":\"INR\"}" \
      > /dev/null || true
  done
}

wait_for_health() { # wait_for_health SERVICE_PORT
  local port=$1 i
  for i in $(seq 1 60); do
    if curl -fsS "http://localhost:$port/actuator/health" | grep -q '"status":"UP"'; then
      return 0
    fi
    sleep 2
  done
  echo "service on port $port never became healthy" >&2
  return 1
}

banner() { printf '\n=== %s ===\n' "$*"; }
