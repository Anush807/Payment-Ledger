package com.paymentledger.wallet.migration;

import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.validation.Validator;

@ChangeUnit(id = "wallet-collections", order = "001", author = "payment-ledger")
public class WalletCollectionsChangeUnit {

    @Execution
    public void execute(MongoTemplate mongo) {
        if (!mongo.collectionExists("wallets")) {
            // last line of defence against an overdraft, whatever the application does
            Document validator = Document.parse("""
                    { $expr: { $and: [
                        { $gte: ["$heldMinor", 0] },
                        { $gte: [{ $subtract: ["$balanceMinor", "$heldMinor"] }, 0] } ] } }
                    """);
            mongo.createCollection("wallets",
                    CollectionOptions.empty().validator(Validator.document(validator)));
        }
        if (!mongo.collectionExists("holds")) {
            mongo.createCollection("holds");
        }
        if (!mongo.collectionExists("top_ups")) {
            mongo.createCollection("top_ups");
        }
        // one hold per order, ever
        mongo.indexOps("holds").ensureIndex(new Index("orderId", Sort.Direction.ASC).unique());
        mongo.indexOps("top_ups").ensureIndex(new Index("idempotencyKey", Sort.Direction.ASC).unique());
    }

    @RollbackExecution
    public void rollback() {
    }
}
