# Rules for working in this repo

The full specification is in `docs/SPEC.md`. These rules come from it and win over habit.

1. Work one phase at a time (see the build-phase table in `docs/SPEC.md`). Finish its "Done when"
   with passing tests, then commit with a message naming the phase.
2. Money is a `long` in minor units (paise) everywhere. No `double`, no `float`, no `Decimal128`.
3. Never publish to Kafka from business code. Every outgoing event is an `outbox` document written
   in the same MongoDB transaction as the state change. `OutboxPublisher` is the only way.
4. Every consumer goes through `EventListenerSupport` in `common`. No hand-rolled listeners.
   (notification-service is the one documented exception: it uses `@RetryableTopic` and is not
   part of the saga.)
5. Don't change an event's name, fields or topic without updating the events table in
   `docs/SPEC.md` and adding an entry to `docs/DECISIONS.md`.
6. Integration tests use Testcontainers with real MongoDB (replica set) and real Kafka. No
   embedded Kafka, no embedded or mocked MongoDB, no mocked brokers.
7. Record every non-obvious choice (library version, config value, deviation from the spec) as a
   short ADR in `docs/DECISIONS.md`.
8. If the spec is ambiguous or seems wrong, stop and ask rather than guessing, especially about
   the pivot and timeout rules.
9. Keep the Grafana dashboard and Debezium connector configs in the repo as files, provisioned
   automatically. Nothing is configured by clicking.
10. Any write that touches more than one document runs in a MongoDB transaction; a business
    document plus its outbox document always counts. Never read a balance and write it back: use
    a conditional `findOneAndUpdate` with `$inc`.
11. Collections, validators and indexes are created only by Mongock change units, never by Spring
    Data auto-index creation (`auto-index-creation=false`).
12. A shortcut with a known limit gets a code comment in the form
    `shortcut: <the limit>, <when to upgrade>`.
