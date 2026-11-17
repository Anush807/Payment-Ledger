package com.paymentledger.common.consumer;

/**
 * A DuplicateKeyException raised by the business handler (not by the processed_events marker).
 * It aborts the transaction, so the handler is simply retried and re-reads the winning document.
 */
public class BusinessConflictException extends RuntimeException {
    public BusinessConflictException(Throwable cause) {
        super(cause);
    }
}
