package com.paymentledger.order.saga;

import com.paymentledger.common.consumer.EventEnvelope;
import com.paymentledger.common.consumer.EventListenerSupport;
import com.paymentledger.common.events.Events;
import com.paymentledger.order.domain.OrderService;
import com.paymentledger.order.domain.OrderStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.UUID;

/** order-service only ever reacts to events: it never cancels on its own timer. */
@Component
public class OrderEventListener extends EventListenerSupport {

    private static final String GROUP = "order-service";

    private final OrderService orders;

    public OrderEventListener(OrderService orders) {
        this.orders = orders;
    }

    @KafkaListener(topics = {"wallet.events", "payment.events", "ledger.events"}, groupId = GROUP)
    public void on(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        consume(rec, ack);
    }

    @Override
    protected String group() {
        return GROUP;
    }

    @Override
    protected void handle(EventEnvelope event) {
        UUID orderId = orderId(event);
        if (orderId == null) {
            return; // wallet top-ups are keyed by walletId and carry no order
        }
        switch (event.eventType()) {
            case Events.FUNDS_RESERVED -> orders.applyEvent(orderId, OrderStatus.FUNDS_RESERVED,
                    event.eventId(), event.eventType(), null);
            case Events.FUNDS_REJECTED -> orders.applyEvent(orderId, OrderStatus.CANCELLED,
                    event.eventId(), event.eventType(), reason(event));
            case Events.PAYMENT_AUTHORIZED -> orders.applyEvent(orderId, OrderStatus.PAYMENT_AUTHORIZED,
                    event.eventId(), event.eventType(), null);
            case Events.PAYMENT_DECLINED -> orders.applyEvent(orderId, OrderStatus.CANCELLED,
                    event.eventId(), event.eventType(), reason(event));
            case Events.LEDGER_ENTRY_POSTED -> orders.applyEvent(orderId, OrderStatus.LEDGER_POSTED,
                    event.eventId(), event.eventType(), null);
            case Events.FUNDS_CAPTURED -> orders.applyEvent(orderId, OrderStatus.COMPLETED,
                    event.eventId(), event.eventType(), null);
            case Events.FUNDS_RELEASED -> orders.note(orderId, event.eventId(), event.eventType());
            default -> { /* not ours */ }
        }
    }

    private UUID orderId(EventEnvelope event) {
        return event.payload().hasNonNull("orderId")
                ? UUID.fromString(event.payload().get("orderId").asText()) : null;
    }

    private String reason(EventEnvelope event) {
        return event.payload().hasNonNull("reason") ? event.payload().get("reason").asText() : null;
    }
}
