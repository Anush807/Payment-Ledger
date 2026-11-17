package com.paymentledger.common.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentledger.common.api.ApiException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.function.Supplier;

/** Replaying a key with the same body returns the stored response; a different body is 422. */
@Component
public class IdempotencyService {

    private final MongoTemplate mongo;
    private final ObjectMapper mapper;

    public IdempotencyService(MongoTemplate mongo, ObjectMapper mapper) {
        this.mongo = mongo;
        this.mapper = mapper;
    }

    @SuppressWarnings("unchecked")
    public <T> T execute(String key, Object request, Class<T> responseType, Supplier<T> action) {
        String hash = hash(request);
        IdempotencyRecord existing = mongo.findById(key, IdempotencyRecord.class);
        if (existing != null) {
            return replay(existing, hash, responseType);
        }
        T response = action.get();
        try {
            mongo.insert(new IdempotencyRecord(key, hash,
                    mapper.convertValue(response, Map.class), Instant.now()));
        } catch (DuplicateKeyException raced) {
            return replay(mongo.findById(key, IdempotencyRecord.class), hash, responseType);
        }
        return response;
    }

    private <T> T replay(IdempotencyRecord record, String hash, Class<T> responseType) {
        if (record == null) {
            throw new ApiException(HttpStatus.CONFLICT, "Idempotency key is being processed, retry");
        }
        if (!record.requestHash().equals(hash)) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_ENTITY,
                    "Idempotency-Key was already used with a different request body");
        }
        return mapper.convertValue(record.response(), responseType);
    }

    private String hash(Object request) {
        try {
            byte[] json = mapper.writeValueAsBytes(request);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(json));
        } catch (NoSuchAlgorithmException | com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
