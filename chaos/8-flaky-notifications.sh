#!/usr/bin/env bash
# Retry topics absorb a 70%-failing provider; the saga is unaffected.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

banner "scenario 8: notification provider failing 70% of the time"
api PUT "$NOTIFICATION/admin/provider" '{"failRate":0.7}' > /dev/null
wallet=$(new_wallet 5000000)
place_orders "$wallet" 20 49900

"$(dirname "$0")/verify.sh"
api PUT "$NOTIFICATION/admin/provider" '{"failRate":0.0}' > /dev/null
echo "notifications stored so far:"
api GET "$NOTIFICATION/notifications" | jq 'length'
