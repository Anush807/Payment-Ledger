package com.paymentledger.wallet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentledger.common.events.Events;
import com.paymentledger.wallet.domain.Hold;
import com.paymentledger.wallet.domain.Wallet;
import com.paymentledger.wallet.domain.WalletService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real MongoDB replica set and real Kafka. Nothing here is mocked. */
@SpringBootTest
@Testcontainers
class WalletSagaIntegrationTest {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("api.key", () -> "test-key");
    }

    @Autowired MongoTemplate mongoTemplate;
    @Autowired WalletService wallets;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ObjectMapper mapper;

    @Test
    void aTopUpWritesTheBalanceAndTheOutboxDocumentTogether() {
        Wallet wallet = wallets.create("u-7", "INR");

        wallets.topUp(wallet.id(), 100_000L, "key-" + UUID.randomUUID());

        assertThat(wallets.get(wallet.id()).balanceMinor()).isEqualTo(100_000L);
        assertThat(outbox(Events.WALLET_TOPPED_UP, wallet.id().toString())).hasSize(1);
    }

    @Test
    void theSameTopUpKeyNeverAddsMoneyTwice() {
        Wallet wallet = wallets.create("u-7", "INR");
        String key = "key-" + UUID.randomUUID();

        wallets.topUp(wallet.id(), 100_000L, key);
        wallets.topUp(wallet.id(), 100_000L, key);

        assertThat(wallets.get(wallet.id()).balanceMinor()).isEqualTo(100_000L);
    }

    @Test
    void anOrderCreatedEventReservesFundsExactlyOnceEvenWhenRedelivered() {
        Wallet wallet = wallets.create("u-7", "INR");
        wallets.topUp(wallet.id(), 100_000L, "key-" + UUID.randomUUID());
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        publish(Events.ORDER_CREATED, eventId, orderId, new Events.OrderCreated(orderId,
                wallet.id(), "m-42", 49_900L, "INR", Instant.now()));
        await().atMost(Duration.ofSeconds(30))
                .until(() -> !holds(orderId).isEmpty());

        // same event id again: the processed_events marker must make it a no-op
        publish(Events.ORDER_CREATED, eventId, orderId, new Events.OrderCreated(orderId,
                wallet.id(), "m-42", 49_900L, "INR", Instant.now()));
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(20))
                .until(() -> holds(orderId).size() == 1);

        assertThat(wallets.get(wallet.id()).heldMinor()).isEqualTo(49_900L);
        assertThat(outbox(Events.FUNDS_RESERVED, orderId.toString())).hasSize(1);
    }

    @Test
    void anUnderfundedWalletIsRejectedAndNothingIsHeld() {
        Wallet wallet = wallets.create("u-7", "INR");
        UUID orderId = UUID.randomUUID();

        wallets.placeHold(orderId, wallet.id(), "m-42", 49_900L, "INR");

        assertThat(holds(orderId)).isEmpty();
        assertThat(wallets.get(wallet.id()).heldMinor()).isZero();
        assertThat(outbox(Events.FUNDS_REJECTED, orderId.toString())).hasSize(1);
    }

    /** Funds for exactly 10 orders, 50 concurrent attempts, balance never goes negative. */
    @Test
    void concurrentHoldsOnOneWalletNeverOverdraw() throws Exception {
        Wallet wallet = wallets.create("u-7", "INR");
        wallets.topUp(wallet.id(), 10 * 49_900L, "key-" + UUID.randomUUID());
        int attempts = 50;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(16);

        for (int i = 0; i < attempts; i++) {
            UUID orderId = UUID.randomUUID();
            pool.submit(() -> {
                start.await();
                wallets.placeHold(orderId, wallet.id(), "m-42", 49_900L, "INR");
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

        Wallet after = wallets.get(wallet.id());
        assertThat(after.heldMinor()).isEqualTo(10 * 49_900L);
        assertThat(after.availableMinor()).isZero();
        assertThat(mongoTemplate.count(Query.query(Criteria.where("walletId").is(wallet.id())
                .and("status").is("RESERVED")), Hold.class)).isEqualTo(10);
    }

    private List<Hold> holds(UUID orderId) {
        return mongoTemplate.find(Query.query(Criteria.where("orderId").is(orderId)), Hold.class);
    }

    private List<org.bson.Document> outbox(String type, String aggregateId) {
        return mongoTemplate.find(Query.query(Criteria.where("type").is(type)
                .and("aggregateid").is(aggregateId)), org.bson.Document.class, "outbox");
    }

    /** Stands in for Debezium: a raw record with the same headers the MongoEventRouter sets. */
    private void publish(String eventType, UUID eventId, UUID key, Object payload) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("order.events", null,
                    key.toString(), mapper.writeValueAsString(payload));
            record.headers().add("id", eventId.toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add("eventType", eventType.getBytes(StandardCharsets.UTF_8));
            record.headers().add("schemaVersion",
                    Events.SCHEMA_VERSION.getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
