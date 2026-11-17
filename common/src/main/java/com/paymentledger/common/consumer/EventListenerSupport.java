package com.paymentledger.common.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentledger.common.events.Events;
import com.paymentledger.common.idempotency.ProcessedEvents;
import com.paymentledger.common.metrics.SagaMetrics;
import com.paymentledger.common.tracing.Trace;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * The one consumer template: parse the envelope, skip duplicates, run the handler and its outbox
 * writes in a single MongoDB transaction, then commit the offset. No hand-rolled listeners.
 */
public abstract class EventListenerSupport {

    private static final int TX_ATTEMPTS = 3;

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired protected ObjectMapper mapper;
    @Autowired protected TransactionTemplate tx;
    @Autowired protected ProcessedEvents processedEvents;
    @Autowired protected SagaMetrics metrics;

    /** The consumer group, which is also the idempotency scope. */
    protected abstract String group();

    /** Business logic. Runs inside a transaction; writes its outbox documents there too. */
    protected abstract void handle(EventEnvelope event);

    protected final void consume(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        EventEnvelope event = parse(rec);
        try {
            Trace.set(event.traceparent());
            MDC.put("eventId", event.eventId().toString());
            if (processedEvents.alreadyProcessed(group(), event.eventId())) {
                metrics.duplicateSkipped(group());
                return;
            }
            runInTransaction(event);
        } finally {
            Trace.clear();
            ack.acknowledge();
        }
    }

    private void runInTransaction(EventEnvelope event) {
        for (int attempt = 1; ; attempt++) {
            try {
                tx.executeWithoutResult(status -> {
                    processedEvents.mark(group(), event.eventId());
                    try {
                        handle(event);
                    } catch (DuplicateKeyException e) {
                        throw new BusinessConflictException(e);
                    }
                });
                return;
            } catch (DuplicateKeyException duplicate) {
                // the marker insert lost the race: the transaction aborted, nothing was applied
                metrics.duplicateSkipped(group());
                return;
            } catch (BusinessConflictException | OptimisticLockingFailureException | TransactionSystemException e) {
                if (attempt >= TX_ATTEMPTS) {
                    throw e;
                }
                log.warn("retrying {} for event {} (attempt {}): {}", event.eventType(),
                        event.eventId(), attempt, e.getMessage());
            }
        }
    }

    private EventEnvelope parse(ConsumerRecord<String, String> rec) {
        UUID eventId = UUID.fromString(require(rec, "id"));
        String eventType = require(rec, "eventType");
        String schemaVersion = header(rec, "schemaVersion");
        if (schemaVersion != null && !Events.SCHEMA_VERSION.equals(schemaVersion)) {
            throw new NonRetryableEventException("unknown schemaVersion " + schemaVersion);
        }
        JsonNode payload;
        try {
            payload = mapper.readTree(rec.value());
        } catch (Exception e) {
            throw new NonRetryableEventException("malformed event payload", e);
        }
        return new EventEnvelope(eventId, eventType, schemaVersion, rec.key(), payload,
                header(rec, "traceparent"));
    }

    private String require(ConsumerRecord<String, String> rec, String name) {
        String value = header(rec, name);
        if (value == null) {
            throw new NonRetryableEventException("missing Kafka header " + name);
        }
        return value;
    }

    private String header(ConsumerRecord<String, String> rec, String name) {
        Header h = rec.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    protected <T> T payload(EventEnvelope event, Class<T> type) {
        try {
            return mapper.treeToValue(event.payload(), type);
        } catch (Exception e) {
            throw new NonRetryableEventException("payload does not match " + type.getSimpleName(), e);
        }
    }
}
