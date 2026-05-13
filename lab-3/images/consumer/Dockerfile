ARG FLINK_TAG

FROM maven:3-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src src
RUN mvn -B package -DskipTests

FROM apache/flink:${FLINK_TAG}
COPY --from=builder /build/target/consumer-*.jar /opt/flink/usrlib/consumer.jar
