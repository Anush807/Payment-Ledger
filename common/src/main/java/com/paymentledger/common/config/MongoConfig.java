package com.paymentledger.common.config;

import com.mongodb.client.MongoClient;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** Multi-document transactions for every write that touches a business document plus its outbox. */
@Configuration
@EntityScan(basePackages = "com.paymentledger")
public class MongoConfig {

    @Bean
    MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory factory) {
        return new MongoTransactionManager(factory);
    }

    @Bean
    TransactionTemplate transactionTemplate(MongoTransactionManager txManager) {
        return new TransactionTemplate(txManager);
    }

    /** Mongock needs the template before the rest of the context; expose the client explicitly. */
    @Bean
    MongoTemplate mongoTemplate(MongoDatabaseFactory factory,
                                org.springframework.data.mongodb.core.convert.MappingMongoConverter converter) {
        return new MongoTemplate(factory, converter);
    }

    @Bean
    MongoHealthNote mongoHealthNote(MongoClient client) {
        return new MongoHealthNote(client);
    }

    /** Marker bean so the client is eagerly created and a bad URI fails fast at startup. */
    public record MongoHealthNote(MongoClient client) {}
}
