package com.paymentledger.payment.domain;

import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/** The "bank": no gateway, no card data. The knobs drive the chaos scenarios. */
@Component
public class SimulatedProcessor {

    public record Settings(double declineRate, long latencyMs, double failRate) {}

    private volatile Settings settings = new Settings(0.0, 0L, 0.0);
    private final AtomicLong counter = new AtomicLong();

    public Settings settings() {
        return settings;
    }

    public void configure(Settings settings) {
        this.settings = settings;
    }

    public record Decision(boolean approved, String processorRef, String declineReason) {}

    public Decision authorize(UUID orderId) {
        Settings current = settings;
        if (current.latencyMs() > 0) {
            try {
                Thread.sleep(current.latencyMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (ThreadLocalRandom.current().nextDouble() < current.failRate()) {
            throw new IllegalStateException("simulated processor failure for order " + orderId);
        }
        if (ThreadLocalRandom.current().nextDouble() < current.declineRate()) {
            return new Decision(false, null, "PROCESSOR_DECLINED");
        }
        return new Decision(true, "sim-" + counter.incrementAndGet() + "-" + orderId, null);
    }
}
