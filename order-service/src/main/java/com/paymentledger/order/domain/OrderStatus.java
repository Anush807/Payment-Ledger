package com.paymentledger.order.domain;

/**
 * The order state machine. Events from different topics arrive in any order, so progress is
 * monotonic by rank: a transition to an equal or earlier state is ignored (but still recorded).
 */
public enum OrderStatus {
    PENDING(0),
    FUNDS_RESERVED(1),
    PAYMENT_AUTHORIZED(2),
    LEDGER_POSTED(3),
    COMPLETED(4),
    CANCELLED(-1);

    private final int rank;

    OrderStatus(int rank) {
        this.rank = rank;
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == CANCELLED;
    }

    public boolean canTransitionTo(OrderStatus target) {
        if (isTerminal()) {
            return false;
        }
        if (target == CANCELLED) {
            return this == PENDING || this == FUNDS_RESERVED;
        }
        return target.rank > this.rank;
    }
}
