package com.paymentledger.ledger.api;

import com.paymentledger.ledger.domain.Account;
import com.paymentledger.ledger.domain.InvariantChecker;
import com.paymentledger.ledger.domain.InvariantReport;
import com.paymentledger.ledger.domain.JournalEntry;
import com.paymentledger.ledger.domain.LedgerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/ledger")
public class LedgerController {

    private final LedgerService ledger;
    private final InvariantChecker invariants;

    public LedgerController(LedgerService ledger, InvariantChecker invariants) {
        this.ledger = ledger;
        this.invariants = invariants;
    }

    @GetMapping("/accounts/{code}")
    public Account account(@PathVariable String code) {
        return ledger.account(code);
    }

    @GetMapping("/journals")
    public List<JournalEntry> journals(@RequestParam(required = false) String sourceRef) {
        return ledger.journals(sourceRef);
    }

    @GetMapping("/invariants")
    public InvariantReport invariants(@RequestParam(defaultValue = "false") boolean recheck) {
        return recheck ? invariants.check() : invariants.last();
    }
}
