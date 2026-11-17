package com.paymentledger.wallet.domain;

import com.paymentledger.common.api.ApiException;
import com.paymentledger.common.events.Events;
import com.paymentledger.common.outbox.OutboxPublisher;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class WalletService {

    private static final Logger log = LoggerFactory.getLogger(WalletService.class);

    private final MongoTemplate mongo;
    private final OutboxPublisher outbox;
    private final TransactionTemplate tx;

    public WalletService(MongoTemplate mongo, OutboxPublisher outbox, TransactionTemplate tx) {
        this.mongo = mongo;
        this.outbox = outbox;
        this.tx = tx;
    }

    public Wallet create(String ownerId, String currency) {
        return mongo.insert(new Wallet(UUID.randomUUID(), ownerId, currency, 0L, 0L));
    }

    public Wallet get(UUID walletId) {
        Wallet wallet = mongo.findById(walletId, Wallet.class);
        if (wallet == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "no wallet " + walletId);
        }
        return wallet;
    }

    public List<Wallet> all() {
        return mongo.find(new Query().limit(100), Wallet.class);
    }

    public List<Hold> holds(Hold.Status status) {
        return mongo.find(Query.query(Criteria.where("status").is(status.name())).limit(500),
                Hold.class);
    }

    public TopUp topUp(UUID walletId, long amountMinor, String idempotencyKey) {
        Wallet wallet = get(walletId);
        TopUp existing = mongo.findOne(
                Query.query(Criteria.where("idempotencyKey").is(idempotencyKey)), TopUp.class);
        if (existing != null) {
            return existing; // the unique index, not the API layer, is the authority
        }
        return tx.execute(status -> {
            TopUp topUp = mongo.insert(new TopUp(UUID.randomUUID(), walletId, idempotencyKey,
                    amountMinor, Instant.now()));
            mongo.updateFirst(Query.query(Criteria.where("_id").is(walletId)),
                    new Update().inc("balanceMinor", amountMinor), Wallet.class);
            outbox.publish(Events.AGG_WALLET, walletId.toString(), Events.WALLET_TOPPED_UP,
                    new Events.WalletToppedUp(walletId, topUp.id(), amountMinor, wallet.currency(),
                            Instant.now()));
            return topUp;
        });
    }

    /** Reserve funds for an order, or reject. Runs inside the consumer's transaction. */
    public void placeHold(UUID orderId, UUID walletId, String merchantId, long amountMinor,
                          String currency) {
        if (mongo.exists(Query.query(Criteria.where("orderId").is(orderId)), Hold.class)) {
            log.info("hold for order {} already exists, nothing to do", orderId);
            return;
        }
        Wallet wallet = mongo.findById(walletId, Wallet.class);
        if (wallet == null) {
            reject(orderId, walletId, "WALLET_NOT_FOUND");
            return;
        }
        if (!wallet.currency().equals(currency)) {
            reject(orderId, walletId, "CURRENCY_MISMATCH");
            return;
        }
        Wallet reserved = mongo.findAndModify(availableAtLeast(walletId, amountMinor),
                new Update().inc("heldMinor", amountMinor),
                FindAndModifyOptions.options().returnNew(true), Wallet.class);
        if (reserved == null) {
            reject(orderId, walletId, "INSUFFICIENT_FUNDS");
            return;
        }
        Hold hold = mongo.insert(new Hold(UUID.randomUUID(), walletId, orderId, amountMinor,
                Hold.Status.RESERVED, Instant.now(), Instant.now()));
        outbox.publish(Events.AGG_WALLET, orderId.toString(), Events.FUNDS_RESERVED,
                new Events.FundsReserved(orderId, hold.id(), walletId, merchantId, amountMinor,
                        currency, Instant.now()));
    }

    public void capture(UUID orderId) {
        Hold hold = claimHold(orderId, Hold.Status.CAPTURED);
        if (hold == null) {
            return;
        }
        mongo.updateFirst(Query.query(Criteria.where("_id").is(hold.walletId())),
                new Update().inc("balanceMinor", -hold.amountMinor()).inc("heldMinor", -hold.amountMinor()),
                Wallet.class);
        outbox.publish(Events.AGG_WALLET, orderId.toString(), Events.FUNDS_CAPTURED,
                new Events.FundsCaptured(orderId, hold.walletId(), hold.amountMinor(), Instant.now()));
    }

    public void release(UUID orderId) {
        Hold hold = claimHold(orderId, Hold.Status.RELEASED);
        if (hold == null) {
            return;
        }
        mongo.updateFirst(Query.query(Criteria.where("_id").is(hold.walletId())),
                new Update().inc("heldMinor", -hold.amountMinor()), Wallet.class);
        outbox.publish(Events.AGG_WALLET, orderId.toString(), Events.FUNDS_RELEASED,
                new Events.FundsReleased(orderId, hold.id(), Instant.now()));
    }

    /** Conditional status change, so capture and release can never run twice on one hold. */
    private Hold claimHold(UUID orderId, Hold.Status target) {
        Hold hold = mongo.findAndModify(
                Query.query(Criteria.where("orderId").is(orderId)
                        .and("status").is(Hold.Status.RESERVED.name())),
                new Update().set("status", target.name()).set("updatedAt", Instant.now()),
                Hold.class);
        if (hold == null) {
            log.info("no RESERVED hold for order {}, skipping {}", orderId, target);
        }
        return hold;
    }

    private void reject(UUID orderId, UUID walletId, String reason) {
        outbox.publish(Events.AGG_WALLET, orderId.toString(), Events.FUNDS_REJECTED,
                new Events.FundsRejected(orderId, walletId, reason, Instant.now()));
    }

    /** { _id, $expr: { $gte: [ { $subtract: [balance, held] }, amount ] } } */
    private Query availableAtLeast(UUID walletId, long amountMinor) {
        Document available = new Document("$subtract", List.of("$balanceMinor", "$heldMinor"));
        return new BasicQuery(new Document("_id", walletId)
                .append("$expr", new Document("$gte", List.of(available, amountMinor))));
    }
}
