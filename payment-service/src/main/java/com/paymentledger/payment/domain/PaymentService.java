package com.paymentledger.payment.domain;

import com.paymentledger.common.api.ApiException;
import com.paymentledger.common.events.Events;
import com.paymentledger.common.outbox.OutboxPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * payment-service is the single decision point of the saga. After AUTHORIZED there is no
 * cancellation, only forward recovery.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final MongoTemplate mongo;
    private final OutboxPublisher outbox;
    private final SimulatedProcessor processor;

    public PaymentService(MongoTemplate mongo, OutboxPublisher outbox, SimulatedProcessor processor) {
        this.mongo = mongo;
        this.outbox = outbox;
        this.processor = processor;
    }

    public Payment get(UUID orderId) {
        Payment payment = mongo.findById(orderId, Payment.class);
        if (payment == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "no payment for order " + orderId);
        }
        return payment;
    }

    public void onFundsReserved(Events.FundsReserved e) {
        Payment existing = mongo.findById(e.orderId(), Payment.class);
        switch (PaymentPivot.onFundsReserved(existing)) {
            case DECIDE -> decide(e);
            case DECLINE_TIMEOUT -> declined(e.orderId(), e.walletId(), "TIMEOUT");
            case IGNORE -> log.info("payment for order {} already {}", e.orderId(),
                    existing == null ? "unknown" : existing.status());
            default -> throw new IllegalStateException();
        }
    }

    public void onCancellationRequested(Events.OrderCancellationRequested e) {
        Payment existing = mongo.findById(e.orderId(), Payment.class);
        if (PaymentPivot.onCancellationRequested(existing) == PaymentPivot.Action.VOID) {
            // no authorization yet: block it. The later FundsReserved emits PaymentDeclined(TIMEOUT).
            mongo.insert(Payment.decided(e.orderId(), null, null, 0L, null,
                    Payment.Status.VOIDED, e.reason(), null));
            log.info("voided order {} before authorization: {}", e.orderId(), e.reason());
        } else {
            log.info("cancellation of order {} ignored, payment is {}", e.orderId(),
                    existing.status());
        }
    }

    /**
     * shortcut: the simulated processor is called inside the MongoDB transaction, so a latencyMs
     * knob above ~30s can hit the transaction lifetime limit. Move the call before the
     * transaction (with its own idempotency record) if real gateway latency is ever needed.
     */
    private void decide(Events.FundsReserved e) {
        SimulatedProcessor.Decision decision = processor.authorize(e.orderId());
        if (!decision.approved()) {
            mongo.insert(Payment.decided(e.orderId(), e.walletId(), e.merchantId(), e.amountMinor(),
                    e.currency(), Payment.Status.DECLINED, decision.declineReason(), null));
            declined(e.orderId(), e.walletId(), decision.declineReason());
            return;
        }
        mongo.insert(Payment.decided(e.orderId(), e.walletId(), e.merchantId(), e.amountMinor(),
                e.currency(), Payment.Status.AUTHORIZED, null, decision.processorRef()));
        outbox.publish(Events.AGG_PAYMENT, e.orderId().toString(), Events.PAYMENT_AUTHORIZED,
                new Events.PaymentAuthorized(e.orderId(), e.walletId(), e.merchantId(),
                        e.amountMinor(), e.currency(), decision.processorRef(), Instant.now()));
    }

    private void declined(UUID orderId, UUID walletId, String reason) {
        outbox.publish(Events.AGG_PAYMENT, orderId.toString(), Events.PAYMENT_DECLINED,
                new Events.PaymentDeclined(orderId, walletId, reason, Instant.now()));
    }
}
