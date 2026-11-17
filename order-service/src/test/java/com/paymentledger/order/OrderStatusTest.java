package com.paymentledger.order;

import com.paymentledger.order.domain.Order;
import com.paymentledger.order.domain.OrderStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OrderStatusTest {

    private static final Set<String> LEGAL = Set.of(
            "PENDING>FUNDS_RESERVED", "PENDING>PAYMENT_AUTHORIZED", "PENDING>LEDGER_POSTED",
            "PENDING>COMPLETED", "PENDING>CANCELLED",
            "FUNDS_RESERVED>PAYMENT_AUTHORIZED", "FUNDS_RESERVED>LEDGER_POSTED",
            "FUNDS_RESERVED>COMPLETED", "FUNDS_RESERVED>CANCELLED",
            "PAYMENT_AUTHORIZED>LEDGER_POSTED", "PAYMENT_AUTHORIZED>COMPLETED",
            "LEDGER_POSTED>COMPLETED");

    @ParameterizedTest
    @EnumSource(OrderStatus.class)
    void everyTransitionMatchesTheStateMachine(OrderStatus from) {
        for (OrderStatus to : OrderStatus.values()) {
            boolean expected = LEGAL.contains(from + ">" + to);
            assertThat(from.canTransitionTo(to))
                    .describedAs("%s -> %s", from, to)
                    .isEqualTo(expected);
        }
    }

    @Test
    void cancellationIsImpossibleAfterAuthorization() {
        assertThat(OrderStatus.PAYMENT_AUTHORIZED.canTransitionTo(OrderStatus.CANCELLED)).isFalse();
        assertThat(OrderStatus.LEDGER_POSTED.canTransitionTo(OrderStatus.CANCELLED)).isFalse();
    }

    @Test
    void terminalStatesNeverMove() {
        for (OrderStatus to : OrderStatus.values()) {
            assertThat(OrderStatus.COMPLETED.canTransitionTo(to)).isFalse();
            assertThat(OrderStatus.CANCELLED.canTransitionTo(to)).isFalse();
        }
    }

    @Test
    void anEventArrivingLateIsIgnoredButRecorded() {
        Order order = newOrder();
        order.apply(OrderStatus.PAYMENT_AUTHORIZED, UUID.randomUUID(), "PaymentAuthorized", null);

        boolean applied = order.apply(OrderStatus.FUNDS_RESERVED, UUID.randomUUID(), "FundsReserved", null);

        assertThat(applied).isFalse();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PAYMENT_AUTHORIZED);
        assertThat(order.getHistory()).last()
                .satisfies(change -> assertThat(change.applied()).isFalse());
    }

    @Test
    void historyStaysBounded() {
        Order order = newOrder();
        for (int i = 0; i < 50; i++) {
            order.note(UUID.randomUUID(), "FundsReleased");
        }
        assertThat(order.getHistory()).hasSizeLessThanOrEqualTo(10);
    }

    private static Order newOrder() {
        return new Order(UUID.randomUUID(), UUID.randomUUID(), "m-42", 49900L, "INR");
    }
}
