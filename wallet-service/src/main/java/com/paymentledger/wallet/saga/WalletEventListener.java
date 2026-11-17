package com.paymentledger.wallet.saga;

import com.paymentledger.common.consumer.EventEnvelope;
import com.paymentledger.common.consumer.EventListenerSupport;
import com.paymentledger.common.events.Events;
import com.paymentledger.wallet.domain.WalletService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class WalletEventListener extends EventListenerSupport {

    private static final String GROUP = "wallet-service";

    private final WalletService wallets;

    public WalletEventListener(WalletService wallets) {
        this.wallets = wallets;
    }

    @KafkaListener(topics = {"order.events", "payment.events", "ledger.events"}, groupId = GROUP)
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
            case Events.ORDER_CREATED -> {
                Events.OrderCreated e = payload(event, Events.OrderCreated.class);
                wallets.placeHold(e.orderId(), e.walletId(), e.merchantId(), e.amountMinor(),
                        e.currency());
            }
            case Events.PAYMENT_DECLINED ->
                    wallets.release(payload(event, Events.PaymentDeclined.class).orderId());
            case Events.LEDGER_ENTRY_POSTED ->
                    wallets.capture(payload(event, Events.LedgerEntryPosted.class).orderId());
            default -> { /* not ours */ }
        }
    }
}
