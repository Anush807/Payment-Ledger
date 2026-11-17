package com.paymentledger.order.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Document("orders")
public class Order {

    private static final int MAX_HISTORY = 10;

    @Id
    private UUID id;
    private UUID walletId;
    private String merchantId;
    private long amountMinor;
    private String currency;
    private OrderStatus status;
    private String cancelReason;
    private Instant cancellationRequestedAt;
    private List<StatusChange> history = new ArrayList<>();
    private Instant createdAt;
    private Instant updatedAt;
    @Version
    private Long version;

    protected Order() {
    }

    public Order(UUID id, UUID walletId, String merchantId, long amountMinor, String currency) {
        this.id = id;
        this.walletId = walletId;
        this.merchantId = merchantId;
        this.amountMinor = amountMinor;
        this.currency = currency;
        this.status = OrderStatus.PENDING;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
        this.history.add(new StatusChange(null, OrderStatus.PENDING, null, null, true, this.createdAt));
    }

    /** Returns true when the status actually moved. Ignored transitions are still recorded. */
    public boolean apply(OrderStatus target, UUID eventId, String eventType, String reason) {
        boolean applied = status.canTransitionTo(target);
        record(status, target, eventId, eventType, applied);
        if (applied) {
            status = target;
            if (target == OrderStatus.CANCELLED) {
                cancelReason = reason;
            }
            updatedAt = Instant.now();
        }
        return applied;
    }

    /** History only, for events that carry no transition (FundsReleased). */
    public void note(UUID eventId, String eventType) {
        record(status, status, eventId, eventType, false);
    }

    private void record(OrderStatus from, OrderStatus to, UUID eventId, String eventType, boolean applied) {
        history.add(new StatusChange(from, to, eventId, eventType, applied, Instant.now()));
        if (history.size() > MAX_HISTORY) {
            history.remove(0);
        }
    }

    public void markCancellationRequested(Instant at) {
        this.cancellationRequestedAt = at;
    }

    public UUID getId() {
        return id;
    }

    public UUID getWalletId() {
        return walletId;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public long getAmountMinor() {
        return amountMinor;
    }

    public String getCurrency() {
        return currency;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public String getCancelReason() {
        return cancelReason;
    }

    public Instant getCancellationRequestedAt() {
        return cancellationRequestedAt;
    }

    public List<StatusChange> getHistory() {
        return history;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Long getVersion() {
        return version;
    }
}
