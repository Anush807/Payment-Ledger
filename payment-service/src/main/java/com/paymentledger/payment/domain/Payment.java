package com.paymentledger.payment.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.UUID;

/** _id is the orderId, so there is exactly one decision per order. */
@Document("payments")
public record Payment(@Id UUID id, UUID walletId, String merchantId, long amountMinor, String currency,
                      Status status, String declineReason, String processorRef, Instant createdAt,
                      Instant updatedAt, @Version Long version) {

    public enum Status {AUTHORIZED, DECLINED, VOIDED}

    public static Payment decided(UUID orderId, UUID walletId, String merchantId, long amountMinor,
                                  String currency, Status status, String declineReason,
                                  String processorRef) {
        Instant now = Instant.now();
        return new Payment(orderId, walletId, merchantId, amountMinor, currency, status,
                declineReason, processorRef, now, now, null);
    }
}
