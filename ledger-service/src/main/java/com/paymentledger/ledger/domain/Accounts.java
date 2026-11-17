package com.paymentledger.ledger.domain;

import java.util.UUID;

/** Account codes and the account type each prefix implies. */
public final class Accounts {

    public static final String CASH = "CASH";
    public static final String FEE_REVENUE = "FEE_REVENUE";

    private Accounts() {}

    public static String customer(UUID walletId) {
        return "CUSTOMER:" + walletId;
    }

    public static String merchant(String merchantId) {
        return "MERCHANT:" + merchantId;
    }

    public static Account.Type typeOf(String code) {
        if (code.startsWith("CUSTOMER:") || code.startsWith("MERCHANT:")) {
            return Account.Type.LIABILITY;
        }
        return CASH.equals(code) ? Account.Type.ASSET : Account.Type.REVENUE;
    }
}
