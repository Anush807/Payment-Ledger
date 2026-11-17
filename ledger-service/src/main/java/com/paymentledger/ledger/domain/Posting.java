package com.paymentledger.ledger.domain;

public record Posting(String account, Direction direction, long amountMinor) {

    public enum Direction {DEBIT, CREDIT}

    public static Posting debit(String account, long amountMinor) {
        return new Posting(account, Direction.DEBIT, amountMinor);
    }

    public static Posting credit(String account, long amountMinor) {
        return new Posting(account, Direction.CREDIT, amountMinor);
    }
}
