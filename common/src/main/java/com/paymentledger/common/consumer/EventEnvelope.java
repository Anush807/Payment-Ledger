package com.paymentledger.common.consumer;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

public record EventEnvelope(UUID eventId, String eventType, String schemaVersion, String key,
                            JsonNode payload, String traceparent) {
}
