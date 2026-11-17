package com.paymentledger.payment.migration;

import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.bson.Document;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.validation.Validator;

@ChangeUnit(id = "payment-collections", order = "001", author = "payment-ledger")
public class PaymentCollectionsChangeUnit {

    @Execution
    public void execute(MongoTemplate mongo) {
        if (!mongo.collectionExists("payments")) {
            Document schema = Document.parse("""
                    { $jsonSchema: {
                        bsonType: "object",
                        required: ["status"],
                        properties: {
                          amountMinor: { bsonType: "long", minimum: 0 },
                          status: { enum: ["AUTHORIZED", "DECLINED", "VOIDED"] }
                        } } }
                    """);
            mongo.createCollection("payments",
                    CollectionOptions.empty().validator(Validator.document(schema)));
        }
    }

    @RollbackExecution
    public void rollback() {
    }
}
