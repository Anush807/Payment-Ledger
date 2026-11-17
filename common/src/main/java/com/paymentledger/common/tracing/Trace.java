package com.paymentledger.common.tracing;

import org.slf4j.MDC;

import java.security.SecureRandom;
import java.util.HexFormat;

/**
 * Minimal W3C traceparent propagation: outbox document -> Kafka header -> consumer MDC.
 * shortcut: no OpenTelemetry SDK, so spans are not exported to Jaeger; swap this for the
 * OTel Java agent when per-span timing is needed.
 */
public final class Trace {
    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();
    private static final SecureRandom RNG = new SecureRandom();

    private Trace() {}

    public static String current() {
        String tp = CURRENT.get();
        return tp != null ? tp : start();
    }

    public static String start() {
        byte[] trace = new byte[16];
        byte[] span = new byte[8];
        RNG.nextBytes(trace);
        RNG.nextBytes(span);
        String tp = "00-" + HexFormat.of().formatHex(trace) + "-" + HexFormat.of().formatHex(span) + "-01";
        set(tp);
        return tp;
    }

    public static void set(String traceparent) {
        if (traceparent == null || traceparent.length() < 36) {
            start();
            return;
        }
        CURRENT.set(traceparent);
        MDC.put("traceId", traceparent.substring(3, 35));
    }

    public static void clear() {
        CURRENT.remove();
        MDC.remove("traceId");
        MDC.remove("eventId");
        MDC.remove("orderId");
    }
}
