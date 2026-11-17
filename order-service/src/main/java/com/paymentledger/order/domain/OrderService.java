package com.paymentledger.order.domain;

import com.paymentledger.common.api.ApiException;
import com.paymentledger.common.events.Events;
import com.paymentledger.common.metrics.SagaMetrics;
import com.paymentledger.common.outbox.OutboxPublisher;
import com.paymentledger.order.api.OrderResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final OutboxPublisher outbox;
    private final TransactionTemplate tx;
    private final SagaMetrics metrics;

    public OrderService(OrderRepository orders, OutboxPublisher outbox, TransactionTemplate tx,
                        SagaMetrics metrics) {
        this.orders = orders;
        this.outbox = outbox;
        this.tx = tx;
        this.metrics = metrics;
    }

    public OrderResponse create(UUID walletId, String merchantId, long amountMinor, String currency) {
        Order order = new Order(UUID.randomUUID(), walletId, merchantId, amountMinor, currency);
        return tx.execute(status -> {
            orders.save(order);
            outbox.publish(Events.AGG_ORDER, order.getId().toString(), Events.ORDER_CREATED,
                    new Events.OrderCreated(order.getId(), walletId, merchantId, amountMinor,
                            currency, Instant.now()));
            return OrderResponse.of(order);
        });
    }

    public OrderResponse get(UUID id) {
        return orders.findById(id).map(OrderResponse::of)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "no order " + id));
    }

    /**
     * Applies one saga event. Must be called inside the consumer's transaction so the order
     * update and any outgoing event are written together.
     */
    public void applyEvent(UUID orderId, OrderStatus target, UUID eventId, String eventType,
                           String reason) {
        Order order = orders.findById(orderId).orElse(null);
        if (order == null) {
            // the order document is created before OrderCreated is relayed, so this cannot be a race
            log.warn("event {} for unknown order {}", eventType, orderId);
            return;
        }
        if (!order.apply(target, eventId, eventType, reason)) {
            orders.save(order);
            log.info("ignored {} for order {} in {}", eventType, orderId, order.getStatus());
            return;
        }
        orders.save(order);
        if (target == OrderStatus.COMPLETED) {
            outbox.publish(Events.AGG_ORDER, orderId.toString(), Events.ORDER_COMPLETED,
                    new Events.OrderCompleted(orderId, order.getAmountMinor(), Instant.now()));
            metrics.sagaCompleted("completed", Duration.between(order.getCreatedAt(), Instant.now()));
        } else if (target == OrderStatus.CANCELLED) {
            outbox.publish(Events.AGG_ORDER, orderId.toString(), Events.ORDER_CANCELLED,
                    new Events.OrderCancelled(orderId, reason, Instant.now()));
            metrics.sagaCompleted("cancelled", Duration.between(order.getCreatedAt(), Instant.now()));
        }
    }

    public com.paymentledger.order.api.OrderSummary summary() {
        java.util.Map<String, Long> counts = new java.util.LinkedHashMap<>();
        for (OrderStatus status : OrderStatus.values()) {
            counts.put(status.name(), 0L);
        }
        long inFlight = 0;
        long oldestAge = 0;
        java.util.List<UUID> inFlightIds = new java.util.ArrayList<>();
        for (Order order : orders.findAll()) {
            counts.merge(order.getStatus().name(), 1L, Long::sum);
            if (!order.getStatus().isTerminal()) {
                inFlight++;
                long age = Duration.between(order.getCreatedAt(), Instant.now()).toSeconds();
                oldestAge = Math.max(oldestAge, age);
                if (inFlightIds.size() < 100) {
                    inFlightIds.add(order.getId());
                }
            }
        }
        return new com.paymentledger.order.api.OrderSummary(counts, inFlight, oldestAge, inFlightIds);
    }

    public void note(UUID orderId, UUID eventId, String eventType) {
        orders.findById(orderId).ifPresent(order -> {
            order.note(eventId, eventType);
            orders.save(order);
        });
    }
}
