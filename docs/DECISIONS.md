# Decision log

Short ADRs. Newest last. Every non-obvious choice, pinned version and deviation from
`docs/SPEC.md` belongs here.

## ADR-001: Spring Boot 3.3.5 and Mongock 5.5.1, both pinned

The spec says "latest stable" Spring Boot. Mongock is the schema tool and its Spring Boot 3
runner (`mongock-springboot-v3` 5.5.1) is the constraint, so both versions are pinned in the
parent POM rather than floating. Revisit together, never separately.

## ADR-002: duplicate detection cannot use try/catch inside the transaction

The spec's consumer template catches `DuplicateKeyException` from the `processed_events` insert
*inside* the transaction and returns. MongoDB does not allow that: a write error inside a
multi-document transaction aborts the whole transaction, so the handler could not continue and a
"return" would commit nothing.

What `EventListenerSupport` does instead:

1. a cheap `exists` check **outside** the transaction (a full topic replay then never opens one);
2. inside the transaction, insert the marker first, then run the handler;
3. a `DuplicateKeyException` escaping the transaction means the marker lost a race - the
   transaction aborted, nothing was applied, so the event is a duplicate and is skipped.

The effect is the same as the spec intends: exactly-once effects, at-least-once delivery.

## ADR-003: `BusinessConflictException` separates the two duplicate-key cases

payment-service deliberately races two events to insert `payments._id = orderId`, and the loser
must re-read and re-decide. That is also a `DuplicateKeyException`, which step 3 above would
misread as "already processed" and silently drop the event. So the consumer template wraps any
`DuplicateKeyException` thrown by the *handler* in `BusinessConflictException` and retries the
whole transaction (3 attempts) before handing the record to the error handler's backoff.

## ADR-004: topics are auto-created from broker defaults

No topic-creation script. The brokers set `num.partitions=6`, `default.replication.factor=3`,
`min.insync.replicas=2` and 7-day retention, so every topic Debezium or a retry mechanism
creates already has the shape the spec asks for. One less thing to keep in sync.

## ADR-005: Debezium 2.7.3.Final, connector JSON kept verbatim

`MongoEventRouter` property names have changed across Debezium releases, so the connector image
is pinned to `debezium/connect:2.7.3.Final` and the exact config lives in
`infra/debezium/<service>-outbox.json`, registered by `infra/debezium/register.sh` (idempotent
`PUT .../config`). The relevant lines:

```json
"transforms.outbox.type": "io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter",
"transforms.outbox.route.by.field": "aggregatetype",
"transforms.outbox.route.topic.replacement": "${routedByValue}.events",
"transforms.outbox.collection.field.event.id": "_id",
"transforms.outbox.collection.field.event.key": "aggregateid",
"transforms.outbox.collection.field.event.type": "type",
"transforms.outbox.collection.field.event.payload": "payload",
"transforms.outbox.collection.expand.json.payload": "true",
"transforms.outbox.collection.fields.additional.placement":
    "type:header:eventType,traceparent:header:traceparent,schemaVersion:header:schemaVersion"
```

`capture.mode=change_streams_update_full` and `snapshot.mode=never`: the router only acts on
inserts, and the outbox TTL deletes are ignored because `tombstones.on.delete=false` and no
delete handler is configured. notification-service has no connector - it emits no events.

## ADR-006: `schemaVersion` is stored in the outbox document

The spec wants a `schemaVersion` Kafka header. Debezium can only lift a header from a field, so
`OutboxDocument` carries `schemaVersion` and the connector places it. Consumers reject an unknown
version as non-retryable (straight to the DLT).

## ADR-007: traceparent by hand, no OpenTelemetry SDK

`common/tracing/Trace` generates and carries the W3C `traceparent` through the outbox document
and the Kafka header, and puts `traceId` in the MDC so one order is one trace id across all five
services in the JSON logs. There is no Jaeger container and no span export: that needs the OTel
Java agent, which is a deployment change, not a code change. Logs join on `traceId` today.

## ADR-008: the invariant job checks invariant 1 only

Invariants 2-4 span the wallet and order databases, and no service may read another service's
database. ledger-service therefore checks invariant 1 (every journal balances, total debits =
total credits, cross-checked against the `accounts` totals) every 30s and exposes it at
`GET /api/v1/ledger/invariants`. `chaos/verify.sh` checks all four through the public APIs, which
is what every chaos scenario ends with.

## ADR-009: Mongo runs without `--auth`, so the append-only role is created but not attached

Enabling authentication on a replica set also needs an internal-auth keyfile, which is deployment
work outside this project's point. `infra/mongo/create-roles.js` creates the `ledgerAppend` role
with `find`+`insert` on `journal_entries` and no `update`/`remove`, listing every other
collection explicitly (MongoDB privileges are additive, so a database-wide write grant would
cancel the restriction). The unique index on `sourceRef` and the `JournalBuilder` are the
enforcement that is actually live.

## ADR-010: the simulated processor is called inside the MongoDB transaction

payment-service decides inside the consumer's transaction, so `latencyMs` above roughly 30s can
hit the transaction lifetime limit. Flagged with a `shortcut:` comment in `PaymentService`. A
real gateway call would have to move outside the transaction with its own idempotency record.

## ADR-011: one Docker image for all five services

`infra/Dockerfile` builds every module once and `SERVICE_JAR` selects the jar at runtime. Five
near-identical Dockerfiles would drift.

## ADR-012: `saga_stuck_total` is a gauge

The name says total, but what matters on the dashboard is "how many orders are stuck right now",
which a counter cannot express after recovery. It is a gauge of non-terminal orders older than
`saga.stuck-after` (default 5m).

## ADR-013: the order state machine is monotonic by rank

Events for one order arrive from four topics, so they can arrive in any order. Instead of
enumerating legal pairs, `OrderStatus` ranks the states and allows any transition to a higher
rank; `CANCELLED` is reachable only from `PENDING` or `FUNDS_RESERVED`; terminal states never
move. A late `FundsReserved` after `PAYMENT_AUTHORIZED` is therefore ignored but still appended
to `history`, as the spec requires.

## ADR-014: notification-service has its own container factory

Non-blocking retries (`@RetryableTopic`) commit per record, so it cannot use the saga services'
`MANUAL_IMMEDIATE` factory. `RetryableKafkaConfig` provides a `RECORD`-ack factory for it.

## ADR-015: what is not built

- Phase 8 stretch work: Avro + Schema Registry, Kafka Streams fraud velocity, Debezium
  exactly-once source mode.
- A Testcontainers-Compose end-to-end test (whole stack including Debezium) and the
  "timeout racing an authorization, 100 runs" test. The chaos suite plus `chaos/verify.sh`
  covers the same ground manually; the 100-run race test is the main gap.
- Jaeger (see ADR-007).

## ADR-016: Testcontainers 1.21.3 and `api.version=1.43` pinned for tests

Docker Engine 29 rejects API requests below 1.44 with HTTP 400, and the docker-java client
Testcontainers ships with asks for 1.32, so every Testcontainers test failed with
"Could not find a valid Docker environment". The parent POM therefore overrides
`testcontainers.version` and passes `-Dapi.version=${docker.api.version}` (1.43) to Surefire.

Docker Desktop on macOS also puts its socket in `~/.docker/run/docker.sock` rather than
`/var/run/docker.sock`, which Testcontainers' socket strategy cannot find. `make test` exports
`DOCKER_HOST` for that socket when it exists, so `mvn test` alone needs
`DOCKER_HOST=unix://$HOME/.docker/run/docker.sock` on such machines.
