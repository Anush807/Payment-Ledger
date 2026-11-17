package com.paymentledger.order.api;

import com.paymentledger.common.idempotency.IdempotencyService;
import com.paymentledger.order.domain.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderService orders;
    private final IdempotencyService idempotency;

    public OrderController(OrderService orders, IdempotencyService idempotency) {
        this.orders = orders;
        this.idempotency = idempotency;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public OrderResponse create(@RequestHeader("Idempotency-Key") String key,
                                @Valid @RequestBody CreateOrderRequest request) {
        return idempotency.execute(key, request, OrderResponse.class,
                () -> orders.create(request.walletId(), request.merchantId(), request.amountMinor(),
                        request.currency()));
    }

    /** Status counts plus the oldest non-terminal order, for chaos/verify.sh. */
    @GetMapping("/summary")
    public OrderSummary summary() {
        return orders.summary();
    }

    @GetMapping("/{id}")
    public OrderResponse get(@PathVariable UUID id) {
        return orders.get(id);
    }
}
