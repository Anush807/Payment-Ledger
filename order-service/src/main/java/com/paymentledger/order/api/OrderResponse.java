package com.paymentledger.order.api;

import com.paymentledger.order.domain.Order;
import com.paymentledger.order.domain.OrderStatus;
import com.paymentledger.order.domain.StatusChange;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderResponse(UUID orderId, UUID walletId, String merchantId, long amountMinor,
                            String currency, OrderStatus status, String cancelReason,
                            List<StatusChange> history, Instant createdAt, Instant updatedAt) {

    public static OrderResponse of(Order order) {
        return new OrderResponse(order.getId(), order.getWalletId(), order.getMerchantId(),
                order.getAmountMinor(), order.getCurrency(), order.getStatus(),
                order.getCancelReason(), List.copyOf(order.getHistory()), order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
