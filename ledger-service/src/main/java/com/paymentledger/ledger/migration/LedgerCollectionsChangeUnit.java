package com.paymentledger.ledger.migration;

import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.validation.Validator;

@ChangeUnit(id = "ledger-collections", order = "001", author = "payment-ledger")
public class LedgerCollectionsChangeUnit {

    @Execution
    public void execute(MongoTemplate mongo) {
        if (!mongo.collectionExists("journal_entries")) {
            Document schema = Document.parse("""
                    { $jsonSchema: {
                        bsonType: "object",
                        required: ["sourceRef", "postings"],
                        properties: {
                          sourceRef: { bsonType: "string" },
                          postings: { bsonType: "array", minItems: 2, items: {
                            bsonType: "object",
                            required: ["account", "direction", "amountMinor"],
                            properties: {
                              direction: { enum: ["DEBIT", "CREDIT"] },
                              amountMinor: { bsonType: "long", minimum: 1 }
                            } } }
                        } } }
                    """);
            mongo.createCollection("journal_entries",
                    CollectionOptions.empty().validator(Validator.document(schema)));
        }
        if (!mongo.collectionExists("accounts")) {
            mongo.createCollection("accounts");
        }
        // blocks a double posting for the same order or top-up
        mongo.indexOps("journal_entries").ensureIndex(
                new Index("sourceRef", Sort.Direction.ASC).unique());
    }

    @RollbackExecution
    public void rollback() {
    }
}
