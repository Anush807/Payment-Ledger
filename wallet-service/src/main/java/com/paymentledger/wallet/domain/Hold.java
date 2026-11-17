package com.paymentledger.wallet.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.UUID;

@Document("holds")
public record Hold(@Id UUID id, UUID walletId, UUID orderId, long amountMinor, Status status,
                   Instant createdAt, Instant updatedAt) {

    public enum Status {RESERVED, CAPTURED, RELEASED}
}
