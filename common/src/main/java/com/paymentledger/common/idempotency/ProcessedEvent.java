package com.paymentledger.common.idempotency;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.UUID;

/** The compound _id makes a duplicate physically impossible. */
@Document("processed_events")
public record ProcessedEvent(@Id Key id, Instant processedAt) {

    public record Key(String group, UUID eventId) {}

    public static ProcessedEvent of(String group, UUID eventId) {
        return new ProcessedEvent(new Key(group, eventId), Instant.now());
    }
}
