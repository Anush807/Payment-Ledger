package com.paymentledger.order.domain;

import java.time.Instant;
import java.util.UUID;

public record StatusChange(OrderStatus from, OrderStatus to, UUID eventId, String eventType,
                           boolean applied, Instant at) {
}
