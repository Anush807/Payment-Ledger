#!/usr/bin/env bash
# Mongo must be a replica set even locally: multi-document transactions and the change streams
# Debezium reads both require one.
set -euo pipefail

until mongosh --host mongo-1 --quiet --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; do
  echo "waiting for mongo-1..."
  sleep 2
done

mongosh --host mongo-1 --quiet <<'JS'
const config = {
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo-1:27017", priority: 2 },
    { _id: 1, host: "mongo-2:27017" },
    { _id: 2, host: "mongo-3:27017" }
  ]
};
try {
  rs.status();
  print("replica set already initiated");
} catch (e) {
  printjson(rs.initiate(config));
}
JS

echo "waiting for a primary..."
until mongosh --host mongo-1 --quiet --eval 'db.hello().isWritablePrimary' | grep -q true; do
  sleep 2
done

mongosh --host mongo-1 --quiet /scripts/create-roles.js
echo "replica set ready"
