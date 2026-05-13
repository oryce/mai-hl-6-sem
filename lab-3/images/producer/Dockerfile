FROM maven:3-eclipse-temurin-26 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src src
RUN mvn -B package -DskipTests

FROM eclipse-temurin:26
WORKDIR /app
COPY --from=builder /build/target/producer-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]