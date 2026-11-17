package com.paymentledger.payment;

import com.paymentledger.payment.domain.Payment;
import com.paymentledger.payment.domain.PaymentPivot;
import com.paymentledger.payment.domain.PaymentPivot.Action;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** The pivot rules decide both authorization and the timeout against one payments document. */
class PaymentPivotTest {

    @Test
    void fundsReservedWithNoDocumentAsksTheProcessor() {
        assertThat(PaymentPivot.onFundsReserved(null)).isEqualTo(Action.DECIDE);
    }

    @Test
    void fundsReservedAfterAVoidDeclinesWithTimeout() {
        assertThat(PaymentPivot.onFundsReserved(payment(Payment.Status.VOIDED)))
                .isEqualTo(Action.DECLINE_TIMEOUT);
    }

    @Test
    void fundsReservedTwiceChangesNothing() {
        assertThat(PaymentPivot.onFundsReserved(payment(Payment.Status.AUTHORIZED)))
                .isEqualTo(Action.IGNORE);
        assertThat(PaymentPivot.onFundsReserved(payment(Payment.Status.DECLINED)))
                .isEqualTo(Action.IGNORE);
    }

    @Test
    void cancellationBeforeAnyDecisionVoidsThePayment() {
        assertThat(PaymentPivot.onCancellationRequested(null)).isEqualTo(Action.VOID);
    }

    @Test
    void cancellationAfterAuthorizationIsIgnoredSoTheOrderCompletes() {
        assertThat(PaymentPivot.onCancellationRequested(payment(Payment.Status.AUTHORIZED)))
                .isEqualTo(Action.IGNORE);
    }

    @Test
    void cancellationIsIdempotent() {
        assertThat(PaymentPivot.onCancellationRequested(payment(Payment.Status.VOIDED)))
                .isEqualTo(Action.IGNORE);
    }

    private static Payment payment(Payment.Status status) {
        return Payment.decided(UUID.randomUUID(), UUID.randomUUID(), "m-42", 49900L, "INR", status,
                null, null);
    }
}
