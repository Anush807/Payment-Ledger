package com.paymentledger.payment.domain;

/**
 * The saga's pivot rules, as a pure function so every branch is unit-tested.
 * Authorization and the timeout are decided against the same payments document.
 */
public final class PaymentPivot {

    public enum Action {
        /** No document yet: ask the processor. */
        DECIDE,
        /** The timeout won: tell the saga the payment is declined. */
        DECLINE_TIMEOUT,
        /** No document yet and cancellation was requested: block authorization. */
        VOID,
        /** Already decided, or cancellation arrived too late. */
        IGNORE
    }

    private PaymentPivot() {}

    public static Action onFundsReserved(Payment existing) {
        if (existing == null) {
            return Action.DECIDE;
        }
        return existing.status() == Payment.Status.VOIDED ? Action.DECLINE_TIMEOUT : Action.IGNORE;
    }

    public static Action onCancellationRequested(Payment existing) {
        return existing == null ? Action.VOID : Action.IGNORE;
    }
}
