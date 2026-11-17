package com.paymentledger.notification.saga;

import com.paymentledger.common.events.Events;
import com.paymentledger.common.idempotency.ProcessedEvents;
import com.paymentledger.common.metrics.SagaMetrics;
import com.paymentledger.notification.domain.FakeProvider;
import com.paymentledger.notification.domain.Notification;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Not part of the saga, so retries are non-blocking: a slow provider must never hold up a
 * partition that saga events share.
 */
@Component
public class NotificationListener {

    private static final String GROUP = "notification-service";
    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);

    private final MongoTemplate mongo;
    private final FakeProvider provider;
    private final ProcessedEvents processedEvents;
    private final SagaMetrics metrics;

    public NotificationListener(MongoTemplate mongo, FakeProvider provider,
                                ProcessedEvents processedEvents, SagaMetrics metrics) {
        this.mongo = mongo;
        this.provider = provider;
        this.processedEvents = processedEvents;
        this.metrics = metrics;
    }

    @RetryableTopic(attempts = "4",
            backoff = @Backoff(delay = 5000, multiplier = 6.0, maxDelay = 120_000),
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            dltTopicSuffix = "." + GROUP + ".DLT")
    @KafkaListener(topics = "order.events", groupId = GROUP,
            containerFactory = "retryableKafkaListenerContainerFactory")
    public void on(ConsumerRecord<String, String> rec) {
        String eventType = header(rec, "eventType");
        if (!Events.ORDER_COMPLETED.equals(eventType) && !Events.ORDER_CANCELLED.equals(eventType)) {
            return;
        }
        UUID eventId = UUID.fromString(header(rec, "id"));
        if (processedEvents.alreadyProcessed(GROUP, eventId)) {
            metrics.duplicateSkipped(GROUP);
            return;
        }
        UUID orderId = UUID.fromString(rec.key());
        String message = "Order " + orderId + ": " + eventType;
        provider.send(message); // may throw; the retry topics absorb it
        processedEvents.mark(GROUP, eventId);
        mongo.insert(new Notification(UUID.randomUUID(), orderId, eventType, message, Instant.now()));
    }

    @DltHandler
    public void dlt(ConsumerRecord<String, String> rec) {
        log.error("notification dead-lettered: topic={} key={}", rec.topic(), rec.key());
    }

    private String header(ConsumerRecord<String, String> rec, String name) {
        var h = rec.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
