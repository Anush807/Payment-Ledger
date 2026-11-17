package com.paymentledger.wallet.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.UUID;

/** Never read-modify-written: every change is a conditional $inc. */
@Document("wallets")
public record Wallet(@Id UUID id, String ownerId, String currency, long balanceMinor, long heldMinor) {

    public long availableMinor() {
        return balanceMinor - heldMinor;
    }
}
