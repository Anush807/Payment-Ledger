package com.paymentledger.ledger.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Refuses to build an unbalanced journal. The invariant job double-checks anyway. */
public final class JournalBuilder {

    private final String sourceRef;
    private final String description;
    private final String currency;
    private final List<Posting> postings = new ArrayList<>();

    private JournalBuilder(String sourceRef, String description, String currency) {
        this.sourceRef = sourceRef;
        this.description = description;
        this.currency = currency;
    }

    public static JournalBuilder forSource(String sourceRef, String description, String currency) {
        return new JournalBuilder(sourceRef, description, currency);
    }

    public JournalBuilder debit(String account, long amountMinor) {
        return add(Posting.debit(account, amountMinor));
    }

    public JournalBuilder credit(String account, long amountMinor) {
        return add(Posting.credit(account, amountMinor));
    }

    private JournalBuilder add(Posting posting) {
        if (posting.amountMinor() <= 0) {
            throw new IllegalArgumentException("posting amount must be positive: " + posting);
        }
        postings.add(posting);
        return this;
    }

    public JournalEntry build() {
        if (postings.isEmpty()) {
            throw new IllegalStateException("journal " + sourceRef + " has no postings");
        }
        long debits = sum(Posting.Direction.DEBIT);
        long credits = sum(Posting.Direction.CREDIT);
        if (debits != credits) {
            throw new IllegalStateException("journal " + sourceRef + " is unbalanced: debits "
                    + debits + " != credits " + credits);
        }
        return new JournalEntry(UUID.randomUUID(), sourceRef, description, currency, Instant.now(),
                List.copyOf(postings));
    }

    private long sum(Posting.Direction direction) {
        return postings.stream().filter(p -> p.direction() == direction)
                .mapToLong(Posting::amountMinor).sum();
    }

    /** Fee is integer arithmetic, rounded down; the merchant gets the remainder, so it balances. */
    public static long feeMinor(long amountMinor, int feeBps) {
        return amountMinor * feeBps / 10_000;
    }

    public static JournalEntry purchase(UUID orderId, UUID walletId, String merchantId,
                                        long amountMinor, String currency, int feeBps) {
        long fee = feeMinor(amountMinor, feeBps);
        JournalBuilder builder = forSource("order:" + orderId, "Order " + orderId, currency)
                .debit(Accounts.customer(walletId), amountMinor);
        if (fee > 0) {
            builder.credit(Accounts.FEE_REVENUE, fee);
        }
        return builder.credit(Accounts.merchant(merchantId), amountMinor - fee).build();
    }

    public static JournalEntry topUp(UUID topUpId, UUID walletId, long amountMinor, String currency) {
        return forSource("topup:" + topUpId, "Top-up " + topUpId, currency)
                .debit(Accounts.CASH, amountMinor)
                .credit(Accounts.customer(walletId), amountMinor)
                .build();
    }
}
