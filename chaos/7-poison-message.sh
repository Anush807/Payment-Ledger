#!/usr/bin/env bash
# A malformed event must land in the DLT at once and the partition must keep flowing.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

banner "scenario 7: poison message on payment.events"
wallet=$(new_wallet 5000000)
docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server kafka-1:9092 --topic payment.events \
  --property parse.headers=true --property headers.delimiter='|' \
  --property parse.key=true --property key.separator=';' <<'MSG'
id:11111111-1111-1111-1111-111111111111,eventType:PaymentAuthorized,schemaVersion:1|poison;{ this is not json
MSG

sleep 10
for port in 8081 8082 8084; do
  echo "dead letters on port $port:"
  api GET "http://localhost:$port/api/v1/admin/dlt" | jq 'length'
done

place_orders "$wallet" 5 49900
"$(dirname "$0")/verify.sh"
