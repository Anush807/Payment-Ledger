package com.paymentledger.common.idempotency;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class ProcessedEvents {

    private final MongoTemplate mongo;

    public ProcessedEvents(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    /** Fast path outside the transaction: a replayed topic never opens a transaction at all. */
    public boolean alreadyProcessed(String group, UUID eventId) {
        return mongo.exists(Query.query(Criteria.where("_id").is(new ProcessedEvent.Key(group, eventId))),
                ProcessedEvent.class);
    }

    /** Inside the transaction. A DuplicateKeyException here means another consumer won the race. */
    public void mark(String group, UUID eventId) {
        mongo.insert(ProcessedEvent.of(group, eventId));
    }
}
