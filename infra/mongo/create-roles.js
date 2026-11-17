// The ledger is append-only. MongoDB privileges are additive, so the role lists every collection
// it may write and grants journal_entries find+insert only - never update or remove.
// Mistakes are corrected with reversing entries.
//
// shortcut: the Compose stack runs mongod without --auth (that needs a replica-set keyfile), so
// this role is created but not yet attached to a user. Enable auth and run
// db.createUser({user:"ledger_app", pwd:..., roles:[{role:"ledgerAppend", db:"ledger_db"}]})
// when the demo needs real enforcement.
const ledger = db.getSiblingDB("ledger_db");

const writable = ["accounts", "outbox", "processed_events", "idempotency_keys", "dead_letters",
                  "mongockChangeLog", "mongockLock"];

const privileges = [
  {
    resource: { db: "ledger_db", collection: "journal_entries" },
    actions: ["find", "insert", "createIndex", "listIndexes", "createCollection", "collStats"]
  },
  {
    // database-level actions that touch no document
    resource: { db: "ledger_db", collection: "" },
    actions: ["listCollections", "listIndexes", "createCollection", "createIndex", "collStats"]
  }
];

for (const collection of writable) {
  privileges.push({
    resource: { db: "ledger_db", collection: collection },
    actions: ["find", "insert", "update", "remove", "createIndex", "listIndexes",
              "createCollection", "collStats"]
  });
}

const role = { role: "ledgerAppend", privileges: privileges, roles: [] };
if (ledger.getRole("ledgerAppend")) {
  ledger.updateRole("ledgerAppend", { privileges: privileges, roles: [] });
  print("ledgerAppend role updated");
} else {
  ledger.createRole(role);
  print("ledgerAppend role created");
}
