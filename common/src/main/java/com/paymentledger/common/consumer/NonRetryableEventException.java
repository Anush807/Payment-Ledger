package com.paymentledger.common.consumer;

/** Straight to the DLT: bad JSON, missing headers, unknown schemaVersion, validation errors. */
public class NonRetryableEventException extends RuntimeException {
    public NonRetryableEventException(String message) {
        super(message);
    }

    public NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
