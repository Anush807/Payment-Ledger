#!/usr/bin/env bash
# Waits until no order is in flight, then checks all four invariants through the public APIs.
# Exits non-zero on any violation. Every chaos scenario ends with this script.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

DEADLINE=${DEADLINE:-300}
failures=0
fail() { echo "INVARIANT VIOLATED: $*" >&2; failures=$((failures + 1)); }

banner "waiting for the saga to drain (max ${DEADLINE}s)"
elapsed=0
while :; do
  summary=$(api GET "$ORDER/orders/summary")
  in_flight=$(echo "$summary" | jq -r .inFlight)
  [ "$in_flight" = "0" ] && break
  if [ "$elapsed" -ge "$DEADLINE" ]; then
    echo "$summary" | jq .
    fail "4: $in_flight orders still in flight after ${DEADLINE}s"
    break
  fi
  sleep 5
  elapsed=$((elapsed + 5))
done
api GET "$ORDER/orders/summary" | jq .

banner "invariant 1: the ledger balances"
report=$(api GET "$LEDGER/ledger/invariants?recheck=true")
echo "$report" | jq .
[ "$(echo "$report" | jq -r .ok)" = "true" ] || fail "1: $report"

banner "invariant 2: every wallet balance matches its ledger account"
for row in $(api GET "$WALLET/wallets" | jq -r '.[] | "\(.walletId):\(.balanceMinor)"'); do
  wallet_id=${row%%:*}
  balance=${row##*:}
  account=$(api GET "$LEDGER/ledger/accounts/CUSTOMER:$wallet_id" 2>/dev/null || echo '{}')
  credit=$(echo "$account" | jq -r '.creditTotalMinor // 0')
  debit=$(echo "$account" | jq -r '.debitTotalMinor // 0')
  ledger_balance=$((credit - debit))
  if [ "$balance" != "$ledger_balance" ]; then
    fail "2: wallet $wallet_id balance $balance != ledger $ledger_balance"
  fi
done

banner "invariant 3: no RESERVED hold belongs to a terminal order"
for order_id in $(api GET "$WALLET/wallets/holds?status=RESERVED" | jq -r '.[].orderId'); do
  status=$(api GET "$ORDER/orders/$order_id" | jq -r .status)
  case "$status" in
    COMPLETED|CANCELLED) fail "3: hold still RESERVED for $status order $order_id" ;;
  esac
done

banner "invariant 4: no order sits in a non-terminal state for over 5 minutes"
oldest=$(api GET "$ORDER/orders/summary" | jq -r .oldestNonTerminalAgeSeconds)
[ "$oldest" -le 300 ] || fail "4: oldest non-terminal order is ${oldest}s old"

if [ "$failures" -eq 0 ]; then
  banner "ALL INVARIANTS GREEN"
else
  banner "$failures INVARIANT(S) VIOLATED"
fi
exit "$failures"
