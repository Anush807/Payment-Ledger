package com.paymentledger.order.saga;

import com.paymentledger.common.events.Events;
import com.paymentledger.common.metrics.SagaMetrics;
import com.paymentledger.common.outbox.OutboxPublisher;
import com.paymentledger.order.domain.Order;
import com.paymentledger.order.domain.OrderStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Asks payment-service to decide; it never cancels the order itself. That removes the race where
 * a timeout cancels an order that was just authorized.
 */
@Component
public class SagaTimeoutJob {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutJob.class);
    private static final int MAX_PER_RUN = 200;

    private final MongoTemplate mongo;
    private final OutboxPublisher outbox;
    private final TransactionTemplate tx;
    private final Duration timeout;
    private final Duration stuckAfter;

    public SagaTimeoutJob(MongoTemplate mongo, OutboxPublisher outbox, TransactionTemplate tx,
                          SagaMetrics metrics,
                          @Value("${saga.timeout}") Duration timeout,
                          @Value("${saga.stuck-after}") Duration stuckAfter) {
        this.mongo = mongo;
        this.outbox = outbox;
        this.tx = tx;
        this.timeout = timeout;
        this.stuckAfter = stuckAfter;
        metrics.registerStuck(this::countStuck);
    }

    @Scheduled(fixedDelay = 5000)
    public void requestCancellations() {
        for (int i = 0; i < MAX_PER_RUN; i++) {
            Order claimed = tx.execute(status -> claimAndPublish());
            if (claimed == null) {
                return;
            }
            log.info("requested cancellation of order {} after {}", claimed.getId(), timeout);
        }
    }

    /** One conditional update, so two instances never claim the same order. */
    private Order claimAndPublish() {
        Instant cutoff = Instant.now().minus(timeout);
        Query query = Query.query(Criteria.where("status")
                        .in(OrderStatus.PENDING.name(), OrderStatus.FUNDS_RESERVED.name())
                .and("createdAt").lt(cutoff)
                .and("cancellationRequestedAt").is(null));
        Order order = mongo.findAndModify(query,
                new Update().set("cancellationRequestedAt", Instant.now()),
                FindAndModifyOptions.options().returnNew(true), Order.class);
        if (order == null) {
            return null;
        }
        outbox.publish(Events.AGG_ORDER, order.getId().toString(),
                Events.ORDER_CANCELLATION_REQUESTED,
                new Events.OrderCancellationRequested(order.getId(), "TIMEOUT", Instant.now()));
        return order;
    }

    long countStuck() {
        List<String> nonTerminal = List.of(OrderStatus.PENDING.name(), OrderStatus.FUNDS_RESERVED.name(),
                OrderStatus.PAYMENT_AUTHORIZED.name(), OrderStatus.LEDGER_POSTED.name());
        return mongo.count(Query.query(Criteria.where("status").in(nonTerminal)
                .and("createdAt").lt(Instant.now().minus(stuckAfter))), Order.class);
    }
}
