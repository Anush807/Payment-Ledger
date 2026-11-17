package com.paymentledger.wallet.api;

import com.paymentledger.common.idempotency.IdempotencyService;
import com.paymentledger.wallet.domain.Hold;
import com.paymentledger.wallet.domain.TopUp;
import com.paymentledger.wallet.domain.Wallet;
import com.paymentledger.wallet.domain.WalletService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/wallets")
public class WalletController {

    private final WalletService wallets;
    private final IdempotencyService idempotency;

    public WalletController(WalletService wallets, IdempotencyService idempotency) {
        this.wallets = wallets;
        this.idempotency = idempotency;
    }

    public record CreateWalletRequest(@NotBlank String ownerId,
                                      @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {}

    public record TopUpRequest(@Min(1) long amountMinor) {}

    public record WalletResponse(UUID walletId, String ownerId, String currency, long balanceMinor,
                                 long heldMinor, long availableMinor) {
        static WalletResponse of(Wallet w) {
            return new WalletResponse(w.id(), w.ownerId(), w.currency(), w.balanceMinor(),
                    w.heldMinor(), w.availableMinor());
        }
    }

    public record TopUpResponse(UUID topUpId, UUID walletId, long amountMinor) {
        static TopUpResponse of(TopUp t) {
            return new TopUpResponse(t.id(), t.walletId(), t.amountMinor());
        }
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public WalletResponse create(@Valid @RequestBody CreateWalletRequest request) {
        return WalletResponse.of(wallets.create(request.ownerId(), request.currency()));
    }

    @PostMapping("/{id}/top-ups")
    @ResponseStatus(HttpStatus.CREATED)
    public TopUpResponse topUp(@PathVariable UUID id,
                               @RequestHeader("Idempotency-Key") String key,
                               @Valid @RequestBody TopUpRequest request) {
        return idempotency.execute(key, request, TopUpResponse.class,
                () -> TopUpResponse.of(wallets.topUp(id, request.amountMinor(), key)));
    }

    /** For chaos/verify.sh: invariant 2 compares these against the ledger. */
    @GetMapping
    public List<WalletResponse> list() {
        return wallets.all().stream().map(WalletResponse::of).toList();
    }

    /** For chaos/verify.sh: invariant 3 checks no RESERVED hold belongs to a terminal order. */
    @GetMapping("/holds")
    public List<Hold> holds(@RequestParam(defaultValue = "RESERVED") Hold.Status status) {
        return wallets.holds(status);
    }

    @GetMapping("/{id}")
    public WalletResponse get(@PathVariable UUID id) {
        return WalletResponse.of(wallets.get(id));
    }
}
