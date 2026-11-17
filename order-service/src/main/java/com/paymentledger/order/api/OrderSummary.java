package com.paymentledger.order.api;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public record OrderSummary(Map<String, Long> countByStatus, long inFlight,
                           long oldestNonTerminalAgeSeconds, List<UUID> nonTerminalOrderIds) {
}
