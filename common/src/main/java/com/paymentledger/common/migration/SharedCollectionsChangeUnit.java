package com.paymentledger.common.migration;

import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

import java.time.Duration;

/** Collections, validators and indexes exist only because of change units like this one. */
@ChangeUnit(id = "shared-collections", order = "000", author = "payment-ledger")
public class SharedCollectionsChangeUnit {

    @Execution
    public void execute(MongoTemplate mongo) {
        for (String name : new String[]{"outbox", "processed_events", "idempotency_keys", "dead_letters"}) {
            if (!mongo.collectionExists(name)) {
                mongo.createCollection(name);
            }
        }
        // TTL replaces a purge job; the Debezium router only reacts to inserts
        mongo.indexOps("outbox").ensureIndex(
                new Index("createdAt", Sort.Direction.ASC).expire(Duration.ofHours(24)));
        mongo.indexOps("idempotency_keys").ensureIndex(
                new Index("createdAt", Sort.Direction.ASC).expire(Duration.ofHours(24)));
        mongo.indexOps("processed_events").ensureIndex(
                new Index("processedAt", Sort.Direction.ASC).expire(Duration.ofDays(7)));
    }

    @RollbackExecution
    public void rollback() {
        // collections are kept: dropping them would lose the idempotency history
    }
}
