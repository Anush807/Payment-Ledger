package com.paymentledger.wallet.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.UUID;

@Document("top_ups")
public record TopUp(@Id UUID id, UUID walletId, String idempotencyKey, long amountMinor,
                    Instant createdAt) {
}
