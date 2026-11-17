package com.paymentledger.notification.api;

import com.paymentledger.notification.domain.FakeProvider;
import com.paymentledger.notification.domain.Notification;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class NotificationController {

    private final MongoTemplate mongo;
    private final FakeProvider provider;

    public NotificationController(MongoTemplate mongo, FakeProvider provider) {
        this.mongo = mongo;
        this.provider = provider;
    }

    public record ProviderSettingsRequest(@DecimalMin("0.0") @DecimalMax("1.0") double failRate) {}

    @GetMapping("/notifications")
    public List<Notification> list(@RequestParam(required = false) UUID orderId) {
        Query query = orderId == null ? new Query()
                : Query.query(Criteria.where("orderId").is(orderId));
        return mongo.find(query.limit(100), Notification.class);
    }

    @PutMapping("/admin/provider")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void configure(@Valid @RequestBody ProviderSettingsRequest request) {
        provider.setFailRate(request.failRate());
    }
}
