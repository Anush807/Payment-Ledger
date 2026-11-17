package com.paymentledger.common.dlt;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/admin/dlt")
public class DeadLetterController {

    private final DeadLetterService service;
    private final org.springframework.data.mongodb.core.MongoTemplate mongo;

    public DeadLetterController(DeadLetterService service,
                                org.springframework.data.mongodb.core.MongoTemplate mongo) {
        this.service = service;
        this.mongo = mongo;
    }

    @org.springframework.web.bind.annotation.GetMapping
    public List<DeadLetter> list() {
        return mongo.findAll(DeadLetter.class);
    }

    @PostMapping("/{id}/replay")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void replay(@PathVariable UUID id) {
        service.replay(id);
    }
}
