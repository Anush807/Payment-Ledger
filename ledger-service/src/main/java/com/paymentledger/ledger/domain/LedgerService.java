package com.paymentledger.ledger.domain;

import com.paymentledger.common.api.ApiException;
import com.paymentledger.common.events.Events;
import com.paymentledger.common.outbox.OutboxPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class LedgerService {

    private static final Logger log = LoggerFactory.getLogger(LedgerService.class);

    private final MongoTemplate mongo;
    private final OutboxPublisher outbox;
    private final int feeBps;

    public LedgerService(MongoTemplate mongo, OutboxPublisher outbox,
                         @Value("${ledger.fee-bps:200}") int feeBps) {
        this.mongo = mongo;
        this.outbox = outbox;
        this.feeBps = feeBps;
    }

    public void postPurchase(Events.PaymentAuthorized e) {
        JournalEntry journal = JournalBuilder.purchase(e.orderId(), e.walletId(), e.merchantId(),
                e.amountMinor(), e.currency(), feeBps);
        if (alreadyPosted(journal.sourceRef())) {
            return;
        }
        post(journal);
        outbox.publish(Events.AGG_LEDGER, e.orderId().toString(), Events.LEDGER_ENTRY_POSTED,
                new Events.LedgerEntryPosted(e.orderId(), journal.id(), Instant.now()));
    }

    public void postTopUp(Events.WalletToppedUp e) {
        JournalEntry journal = JournalBuilder.topUp(e.topUpId(), e.walletId(), e.amountMinor(),
                e.currency());
        if (alreadyPosted(journal.sourceRef())) {
            return;
        }
        post(journal); // a top-up is terminal: nobody downstream waits for it
    }

    /** Insert the journal and $inc every affected account in one transaction. */
    private void post(JournalEntry journal) {
        mongo.insert(journal);
        for (Posting posting : journal.postings()) {
            String field = posting.direction() == Posting.Direction.DEBIT
                    ? "debitTotalMinor" : "creditTotalMinor";
            mongo.upsert(Query.query(Criteria.where("_id").is(posting.account())),
                    new Update().inc(field, posting.amountMinor())
                            .setOnInsert("type", Accounts.typeOf(posting.account()).name())
                            .setOnInsert("currency", journal.currency()),
                    Account.class);
        }
        log.info("posted journal {} ({})", journal.id(), journal.sourceRef());
    }

    private boolean alreadyPosted(String sourceRef) {
        boolean posted = mongo.exists(Query.query(Criteria.where("sourceRef").is(sourceRef)),
                JournalEntry.class);
        if (posted) {
            log.info("journal for {} already posted", sourceRef);
        }
        return posted;
    }

    public Account account(String code) {
        Account account = mongo.findById(code, Account.class);
        if (account == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "no account " + code);
        }
        return account;
    }

    public List<JournalEntry> journals(String sourceRef) {
        Query query = sourceRef == null ? new Query()
                : Query.query(Criteria.where("sourceRef").is(sourceRef));
        return mongo.find(query.limit(100), JournalEntry.class);
    }

    /** Invariant 2 is checked per wallet through the APIs; here we expose the ledger side. */
    public long customerBalance(UUID walletId) {
        Account account = mongo.findById(Accounts.customer(walletId), Account.class);
        return account == null ? 0L : account.balanceMinor();
    }
}
