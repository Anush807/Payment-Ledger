package com.paymentledger.common.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentledger.common.events.Events;
import com.paymentledger.common.tracing.Trace;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * The only way business code emits an event: a document in the same MongoDB transaction as the
 * state change. Debezium relays it to Kafka.
 */
@Component
public class OutboxPublisher {

    private final MongoTemplate mongo;
    private final ObjectMapper mapper;

    public OutboxPublisher(MongoTemplate mongo, ObjectMapper mapper) {
        this.mongo = mongo;
        this.mapper = mapper;
    }

    @SuppressWarnings("unchecked")
    public UUID publish(String aggregateType, String aggregateId, String type, Object payload) {
        UUID eventId = UUID.randomUUID();
        Map<String, Object> body = mapper.convertValue(payload, Map.class);
        mongo.insert(new OutboxDocument(eventId, aggregateType, aggregateId, type, body,
                Events.SCHEMA_VERSION, Trace.current(), Instant.now()));
        return eventId;
    }
}
