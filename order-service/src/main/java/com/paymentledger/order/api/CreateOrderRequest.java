package com.paymentledger.order.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.util.UUID;

public record CreateOrderRequest(
        @NotNull UUID walletId,
        @NotBlank String merchantId,
        @Min(1) long amountMinor,
        @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {
}
