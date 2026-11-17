package com.paymentledger.notification.domain;

import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/** Stands in for an email/SMS provider. failRate drives chaos scenario 8. */
@Component
public class FakeProvider {

    private volatile double failRate = 0.0;

    public double failRate() {
        return failRate;
    }

    public void setFailRate(double failRate) {
        this.failRate = failRate;
    }

    public void send(String message) {
        if (ThreadLocalRandom.current().nextDouble() < failRate) {
            throw new IllegalStateException("provider unavailable");
        }
    }
}
