package com.paymentledger.ledger.domain;

import com.paymentledger.common.metrics.SagaMetrics;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Invariant 1: every journal balances and total debits = total credits, cross-checked against the
 * accounts totals. Invariants 2-4 span other services' databases, so chaos/verify.sh checks those
 * through the APIs instead of reading foreign databases.
 */
@Component
public class InvariantChecker {

    private static final Logger log = LoggerFactory.getLogger(InvariantChecker.class);

    private final MongoTemplate mongo;
    private volatile InvariantReport last = InvariantReport.pending();

    public InvariantChecker(MongoTemplate mongo, SagaMetrics metrics) {
        this.mongo = mongo;
        metrics.registerGauge("ledger_invariant_violations", () -> last.violations());
    }

    public InvariantReport last() {
        return last;
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 5_000)
    public InvariantReport check() {
        Long unbalancedCount = mongo.getCollection("journal_entries").aggregate(List.of(
                new Document("$unwind", "$postings"),
                new Document("$group", new Document("_id", "$_id").append("net",
                        new Document("$sum", signedAmount()))),
                new Document("$match", new Document("net", new Document("$ne", 0L))),
                new Document("$count", "unbalanced")
        )).map(d -> ((Number) d.get("unbalanced")).longValue()).first();
        long unbalanced = unbalancedCount == null ? 0L : unbalancedCount;

        Document journalTotals = mongo.getCollection("journal_entries").aggregate(List.of(
                new Document("$unwind", "$postings"),
                new Document("$group", new Document("_id", "$postings.direction")
                        .append("total", new Document("$sum", "$postings.amountMinor")))
        )).into(new java.util.ArrayList<>()).stream()
                .reduce(new Document("DEBIT", 0L).append("CREDIT", 0L),
                        (acc, d) -> acc.append(d.getString("_id"),
                                ((Number) d.get("total")).longValue()));

        Document accountTotals = mongo.getCollection("accounts").aggregate(List.of(
                new Document("$group", new Document("_id", null)
                        .append("debit", new Document("$sum", "$debitTotalMinor"))
                        .append("credit", new Document("$sum", "$creditTotalMinor")))
        )).first();

        InvariantReport report = new InvariantReport(true, unbalanced,
                number(journalTotals, "DEBIT"), number(journalTotals, "CREDIT"),
                number(accountTotals, "debit"), number(accountTotals, "credit"), Instant.now());
        report = new InvariantReport(report.violations() == 0, report.unbalancedJournals(),
                report.journalDebitTotalMinor(), report.journalCreditTotalMinor(),
                report.accountDebitTotalMinor(), report.accountCreditTotalMinor(), Instant.now());
        last = report;
        if (!report.ok()) {
            log.error("LEDGER INVARIANT VIOLATION: {}", report);
        }
        return report;
    }

    private static Document signedAmount() {
        return new Document("$cond", List.of(
                new Document("$eq", List.of("$postings.direction", "DEBIT")),
                "$postings.amountMinor",
                new Document("$multiply", List.of("$postings.amountMinor", -1))));
    }

    private static long number(Document doc, String field) {
        if (doc == null || doc.get(field) == null) {
            return 0L;
        }
        return ((Number) doc.get(field)).longValue();
    }
}
