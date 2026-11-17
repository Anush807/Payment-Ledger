package com.paymentledger.common.dlt;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Document("dead_letters")
public record DeadLetter(@Id UUID id, String originalTopic, String dltTopic, String key, String value,
                         Map<String, String> headers, String reason, Instant receivedAt) {
}
