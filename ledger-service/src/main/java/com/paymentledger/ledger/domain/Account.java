package com.paymentledger.ledger.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/** Running totals, $inc-ed in the same transaction as the journal insert. */
@Document("accounts")
public record Account(@Id String id, Type type, String currency, long debitTotalMinor,
                      long creditTotalMinor) {

    public enum Type {ASSET, LIABILITY, REVENUE}

    /** Liabilities and revenue are credit-normal; assets are debit-normal. */
    public long balanceMinor() {
        return type == Type.ASSET ? debitTotalMinor - creditTotalMinor
                : creditTotalMinor - debitTotalMinor;
    }
}
