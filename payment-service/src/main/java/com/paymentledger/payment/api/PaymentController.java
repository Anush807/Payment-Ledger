package com.paymentledger.payment.api;

import com.paymentledger.payment.domain.Payment;
import com.paymentledger.payment.domain.PaymentService;
import com.paymentledger.payment.domain.SimulatedProcessor;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class PaymentController {

    private final PaymentService payments;
    private final SimulatedProcessor processor;

    public PaymentController(PaymentService payments, SimulatedProcessor processor) {
        this.payments = payments;
        this.processor = processor;
    }

    public record ProcessorSettingsRequest(
            @DecimalMin("0.0") @DecimalMax("1.0") double declineRate,
            @Min(0) long latencyMs,
            @DecimalMin("0.0") @DecimalMax("1.0") double failRate) {}

    @GetMapping("/payments/{orderId}")
    public Payment get(@PathVariable UUID orderId) {
        return payments.get(orderId);
    }

    @GetMapping("/admin/processor")
    public SimulatedProcessor.Settings settings() {
        return processor.settings();
    }

    @PutMapping("/admin/processor")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void configure(@Valid @RequestBody ProcessorSettingsRequest request) {
        processor.configure(new SimulatedProcessor.Settings(request.declineRate(),
                request.latencyMs(), request.failRate()));
    }
}
