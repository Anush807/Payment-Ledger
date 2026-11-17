package com.paymentledger.common.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class SagaMetrics {

    private final MeterRegistry registry;

    public SagaMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void duplicateSkipped(String service) {
        registry.counter("duplicate_events_skipped_total", "service", service).increment();
    }

    public void deadLetter(String topic) {
        registry.counter("dlt_messages_total", "topic", topic).increment();
    }

    public void sagaCompleted(String outcome, Duration duration) {
        registry.counter("saga_completed_total", "outcome", outcome).increment();
        Timer.builder("saga_duration_seconds")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
                .record(duration);
    }

    /** Non-terminal orders past the stuck threshold; the dashboard panel must stay at 0. */
    public void registerStuck(java.util.function.Supplier<Number> stuckCount) {
        io.micrometer.core.instrument.Gauge.builder("saga_stuck_total", stuckCount)
                .register(registry);
    }

    public void registerGauge(String name, java.util.function.Supplier<Number> value) {
        io.micrometer.core.instrument.Gauge.builder(name, value).register(registry);
    }
}
