package com.paymentledger.notification.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.UUID;

@Document("notifications")
public record Notification(@Id UUID id, UUID orderId, String eventType, String message,
                           Instant sentAt) {
}
