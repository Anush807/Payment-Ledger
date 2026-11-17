package com.paymentledger.ledger.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Postings are embedded, so a journal is written atomically and a partial one cannot exist. */
@Document("journal_entries")
public record JournalEntry(@Id UUID id, String sourceRef, String description, String currency,
                           Instant postedAt, List<Posting> postings) {
}
