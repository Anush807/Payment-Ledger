package com.paymentledger.ledger.saga;

import com.paymentledger.common.consumer.EventEnvelope;
import com.paymentledger.common.consumer.EventListenerSupport;
import com.paymentledger.common.events.Events;
import com.paymentledger.ledger.domain.LedgerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class LedgerEventListener extends EventListenerSupport {

    private static final String GROUP = "ledger-service";

    private final LedgerService ledger;

    public LedgerEventListener(LedgerService ledger) {
        this.ledger = ledger;
    }

    @KafkaListener(topics = {"payment.events", "wallet.events"}, groupId = GROUP)
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
            case Events.PAYMENT_AUTHORIZED ->
                    ledger.postPurchase(payload(event, Events.PaymentAuthorized.class));
            case Events.WALLET_TOPPED_UP ->
                    ledger.postTopUp(payload(event, Events.WalletToppedUp.class));
            default -> { /* not ours */ }
        }
    }
}
