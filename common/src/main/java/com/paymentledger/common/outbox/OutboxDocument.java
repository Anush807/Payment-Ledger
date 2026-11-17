package com.paymentledger.common.outbox;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Field names match the Debezium MongoEventRouter defaults - do not rename. */
@Document("outbox")
public record OutboxDocument(
        @Id UUID id,
        String aggregatetype,
        String aggregateid,
        String type,
        Map<String, Object> payload,
        String schemaVersion,
        String traceparent,
        Instant createdAt) {
}
