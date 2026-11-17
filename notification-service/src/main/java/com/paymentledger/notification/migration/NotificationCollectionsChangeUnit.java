package com.paymentledger.notification.migration;

import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

@ChangeUnit(id = "notification-collections", order = "001", author = "payment-ledger")
public class NotificationCollectionsChangeUnit {

    @Execution
    public void execute(MongoTemplate mongo) {
        if (!mongo.collectionExists("notifications")) {
            mongo.createCollection("notifications");
        }
        mongo.indexOps("notifications").ensureIndex(new Index("orderId", Sort.Direction.ASC));
    }

    @RollbackExecution
    public void rollback() {
    }
}
