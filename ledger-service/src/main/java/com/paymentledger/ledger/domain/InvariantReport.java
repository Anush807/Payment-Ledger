package com.paymentledger.ledger.domain;

import java.time.Instant;

public record InvariantReport(boolean ok, long unbalancedJournals, long journalDebitTotalMinor,
                              long journalCreditTotalMinor, long accountDebitTotalMinor,
                              long accountCreditTotalMinor, Instant checkedAt) {

    public static InvariantReport pending() {
        return new InvariantReport(true, 0, 0, 0, 0, 0, null);
    }

    public long violations() {
        return unbalancedJournals
                + (journalDebitTotalMinor == journalCreditTotalMinor ? 0 : 1)
                + (journalDebitTotalMinor == accountDebitTotalMinor
                   && journalCreditTotalMinor == accountCreditTotalMinor ? 0 : 1);
    }
}
