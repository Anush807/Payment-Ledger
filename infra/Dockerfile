# One image for all five services; SERVICE_JAR picks which jar runs.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY common/pom.xml common/
COPY order-service/pom.xml order-service/
COPY wallet-service/pom.xml wallet-service/
COPY payment-service/pom.xml payment-service/
COPY ledger-service/pom.xml ledger-service/
COPY notification-service/pom.xml notification-service/
COPY arch-tests/pom.xml arch-tests/
RUN mvn -B -q dependency:go-offline -DskipTests || true
COPY . .
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/*-service/target/*-service-*.jar /app/jars/
ENV PORT=8080
ENTRYPOINT ["sh", "-c", "exec java -jar /app/jars/${SERVICE_JAR}-1.0.0-SNAPSHOT.jar"]
