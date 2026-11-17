package com.paymentledger.order.migration;

import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.validation.Validator;

@ChangeUnit(id = "order-collections", order = "001", author = "payment-ledger")
public class OrderCollectionsChangeUnit {

    @Execution
    public void execute(MongoTemplate mongo) {
        if (!mongo.collectionExists("orders")) {
            Document schema = Document.parse("""
                    { $jsonSchema: {
                        bsonType: "object",
                        required: ["walletId", "merchantId", "amountMinor", "currency", "status"],
                        properties: {
                          amountMinor: { bsonType: "long", minimum: 1 },
                          currency: { bsonType: "string", pattern: "^[A-Z]{3}$" },
                          status: { enum: ["PENDING", "FUNDS_RESERVED", "PAYMENT_AUTHORIZED",
                                           "LEDGER_POSTED", "COMPLETED", "CANCELLED"] }
                        } } }
                    """);
            mongo.createCollection("orders", CollectionOptions.empty().validator(Validator.document(schema)));
        }
        // the timeout job scans by status and age
        mongo.indexOps("orders").ensureIndex(new Index()
                .on("status", Sort.Direction.ASC)
                .on("createdAt", Sort.Direction.ASC));
    }

    @RollbackExecution
    public void rollback() {
    }
}
