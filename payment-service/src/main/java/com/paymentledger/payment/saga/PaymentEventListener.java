package com.paymentledger.payment.saga;

import com.paymentledger.common.consumer.EventEnvelope;
import com.paymentledger.common.consumer.EventListenerSupport;
import com.paymentledger.common.events.Events;
import com.paymentledger.payment.domain.PaymentService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventListener extends EventListenerSupport {

    private static final String GROUP = "payment-service";

    private final PaymentService payments;

    public PaymentEventListener(PaymentService payments) {
        this.payments = payments;
    }

    @KafkaListener(topics = {"order.events", "wallet.events"}, groupId = GROUP)
    public void on(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        consume(rec, ack);
    }

    @Override
    protected String group() {
        return GROUP;
    }

    @Override
    protected void handle(EventEnvelope event) {
        switch (event.eventType()) {
            case Events.FUNDS_RESERVED ->
                    payments.onFundsReserved(payload(event, Events.FundsReserved.class));
            case Events.ORDER_CANCELLATION_REQUESTED -> payments.onCancellationRequested(
                    payload(event, Events.OrderCancellationRequested.class));
            default -> { /* not ours */ }
        }
    }
}
