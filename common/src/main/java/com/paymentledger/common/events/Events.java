package com.paymentledger.common.events;

import java.time.Instant;
import java.util.UUID;

/** Every saga event, in one place. Payloads are records; money is always a long in minor units. */
public final class Events {
    private Events() {}

    public static final String SCHEMA_VERSION = "1";

    public static final String ORDER_CREATED = "OrderCreated";
    public static final String FUNDS_RESERVED = "FundsReserved";
    public static final String FUNDS_REJECTED = "FundsRejected";
    public static final String PAYMENT_AUTHORIZED = "PaymentAuthorized";
    public static final String PAYMENT_DECLINED = "PaymentDeclined";
    public static final String LEDGER_ENTRY_POSTED = "LedgerEntryPosted";
    public static final String FUNDS_CAPTURED = "FundsCaptured";
    public static final String FUNDS_RELEASED = "FundsReleased";
    public static final String ORDER_CANCELLATION_REQUESTED = "OrderCancellationRequested";
    public static final String ORDER_COMPLETED = "OrderCompleted";
    public static final String ORDER_CANCELLED = "OrderCancelled";
    public static final String WALLET_TOPPED_UP = "WalletToppedUp";

    public static final String AGG_ORDER = "order";
    public static final String AGG_WALLET = "wallet";
    public static final String AGG_PAYMENT = "payment";
    public static final String AGG_LEDGER = "ledger";

    public record OrderCreated(UUID orderId, UUID walletId, String merchantId, long amountMinor,
                               String currency, Instant occurredAt) {}

    public record FundsReserved(UUID orderId, UUID holdId, UUID walletId, String merchantId,
                                long amountMinor, String currency, Instant occurredAt) {}

    public record FundsRejected(UUID orderId, UUID walletId, String reason, Instant occurredAt) {}

    public record PaymentAuthorized(UUID orderId, UUID walletId, String merchantId, long amountMinor,
                                    String currency, String processorRef, Instant occurredAt) {}

    public record PaymentDeclined(UUID orderId, UUID walletId, String reason, Instant occurredAt) {}

    public record LedgerEntryPosted(UUID orderId, UUID journalId, Instant occurredAt) {}

    public record FundsCaptured(UUID orderId, UUID walletId, long amountMinor, Instant occurredAt) {}

    public record FundsReleased(UUID orderId, UUID holdId, Instant occurredAt) {}

    public record OrderCancellationRequested(UUID orderId, String reason, Instant occurredAt) {}

    public record OrderCompleted(UUID orderId, long amountMinor, Instant occurredAt) {}

    public record OrderCancelled(UUID orderId, String reason, Instant occurredAt) {}

    public record WalletToppedUp(UUID walletId, UUID topUpId, long amountMinor, String currency,
                                 Instant occurredAt) {}
}
