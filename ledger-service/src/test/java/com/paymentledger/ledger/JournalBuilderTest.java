package com.paymentledger.ledger;

import com.paymentledger.ledger.domain.JournalBuilder;
import com.paymentledger.ledger.domain.JournalEntry;
import com.paymentledger.ledger.domain.Posting;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JournalBuilderTest {

    @Test
    void refusesAnUnbalancedJournal() {
        assertThatThrownBy(() -> JournalBuilder.forSource("order:x", "x", "INR")
                .debit("CUSTOMER:a", 100L)
                .credit("MERCHANT:m", 99L)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unbalanced");
    }

    @Test
    void refusesANonPositivePosting() {
        assertThatThrownBy(() -> JournalBuilder.forSource("order:x", "x", "INR").debit("CASH", 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({
            "49900, 200, 998, 48902",
            "100, 200, 2, 98",
            "49, 200, 0, 49",      // fee rounds down to nothing, merchant gets it all
            "1, 200, 0, 1",
            "123457, 200, 2469, 120988"
    })
    void feeRoundsDownAndTheMerchantGetsTheRemainder(long amount, int feeBps, long expectedFee,
                                                     long expectedMerchant) {
        assertThat(JournalBuilder.feeMinor(amount, feeBps)).isEqualTo(expectedFee);

        JournalEntry journal = JournalBuilder.purchase(UUID.randomUUID(), UUID.randomUUID(), "m-42",
                amount, "INR", feeBps);

        assertThat(sum(journal, Posting.Direction.DEBIT))
                .isEqualTo(sum(journal, Posting.Direction.CREDIT))
                .isEqualTo(amount);
        assertThat(journal.postings()).filteredOn(p -> p.account().equals("MERCHANT:m-42"))
                .singleElement()
                .satisfies(p -> assertThat(p.amountMinor()).isEqualTo(expectedMerchant));
    }

    @Test
    void aTopUpDebitsCashAndCreditsTheCustomer() {
        UUID walletId = UUID.randomUUID();
        JournalEntry journal = JournalBuilder.topUp(UUID.randomUUID(), walletId, 100000L, "INR");

        assertThat(journal.postings()).containsExactly(
                Posting.debit("CASH", 100000L),
                Posting.credit("CUSTOMER:" + walletId, 100000L));
        assertThat(journal.sourceRef()).startsWith("topup:");
    }

    private static long sum(JournalEntry journal, Posting.Direction direction) {
        return journal.postings().stream().filter(p -> p.direction() == direction)
                .mapToLong(Posting::amountMinor).sum();
    }
}
