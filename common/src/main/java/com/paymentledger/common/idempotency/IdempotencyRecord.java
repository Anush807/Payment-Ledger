package com.paymentledger.common.idempotency;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;

@Document("idempotency_keys")
public record IdempotencyRecord(@Id String id, String requestHash, Map<String, Object> response,
                                Instant createdAt) {
}
