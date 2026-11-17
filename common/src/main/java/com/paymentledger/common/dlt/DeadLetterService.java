package com.paymentledger.common.dlt;

import com.paymentledger.common.api.ApiException;
import com.paymentledger.common.metrics.SagaMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Stores every dead letter this service produced and republishes one on demand. */
@Service
public class DeadLetterService {

    private final MongoTemplate mongo;
    private final KafkaTemplate<String, String> kafka;
    private final SagaMetrics metrics;

    public DeadLetterService(MongoTemplate mongo, KafkaTemplate<String, String> kafka,
                             SagaMetrics metrics) {
        this.mongo = mongo;
        this.kafka = kafka;
        this.metrics = metrics;
    }

    @KafkaListener(topicPattern = ".*\\.${spring.kafka.consumer.group-id}\\.DLT",
            groupId = "${spring.kafka.consumer.group-id}-dlt")
    public void onDeadLetter(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        Map<String, String> headers = new HashMap<>();
        for (Header h : rec.headers()) {
            headers.put(h.key(), h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8));
        }
        String original = headers.getOrDefault("kafka_dlt-original-topic",
                rec.topic().replaceAll("\\.[^.]+\\.DLT$", ""));
        mongo.insert(new DeadLetter(UUID.randomUUID(), original, rec.topic(), rec.key(), rec.value(),
                headers, headers.get("kafka_dlt-exception-message"), Instant.now()));
        metrics.deadLetter(rec.topic());
        ack.acknowledge();
    }

    public void replay(UUID id) {
        DeadLetter letter = mongo.findById(id, DeadLetter.class);
        if (letter == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "no dead letter " + id);
        }
        // a raw ProducerRecord, because Spring Messaging reserves the "id" header name
        ProducerRecord<String, String> record = new ProducerRecord<>(letter.originalTopic(),
                null, letter.key(), letter.value());
        letter.headers().forEach((k, v) -> {
            if (!k.startsWith("kafka_") && v != null) {
                record.headers().add(k, v.getBytes(StandardCharsets.UTF_8));
            }
        });
        kafka.send(record);
    }

}
