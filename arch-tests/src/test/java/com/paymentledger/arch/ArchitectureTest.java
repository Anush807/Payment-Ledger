package com.paymentledger.arch;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

class ArchitectureTest {

    private static JavaClasses classes;

    @BeforeAll
    static void importClasses() {
        classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.paymentledger");
    }

    @Test
    void businessCodeNeverPublishesToKafka() {
        noClasses().that().resideOutsideOfPackage("com.paymentledger.common..")
                .should().dependOnClassesThat().haveFullyQualifiedName(
                        "org.springframework.kafka.core.KafkaTemplate")
                .because("every outgoing event is an outbox document in the same transaction")
                .check(classes);
    }

    @Test
    void servicesDoNotImportEachOther() {
        for (String service : new String[]{"order", "wallet", "payment", "ledger", "notification"}) {
            noClasses().that().resideInAPackage("com.paymentledger." + service + "..")
                    .should().dependOnClassesThat().resideInAnyPackage(otherServices(service))
                    .because("services share nothing but Kafka topics and the common module")
                    .check(classes);
        }
    }

    private static String[] otherServices(String service) {
        return java.util.Arrays.stream(new String[]{"order", "wallet", "payment", "ledger", "notification"})
                .filter(s -> !s.equals(service))
                .map(s -> "com.paymentledger." + s + "..")
                .toArray(String[]::new);
    }
}
