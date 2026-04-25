FROM docker.io/maven:3.9-eclipse-temurin-26 AS builder

WORKDIR /app

# Copy pom.xml and download dependencies first to leverage Docker caching
COPY pom.xml .
RUN mvn dependency:resolve

# Now copy the source code and build the application
COPY src ./src
RUN mvn package -DskipTests

# Stage 2: Runtime optimized for CRaC
FROM docker.io/eclipse-temurin:26-jre

WORKDIR /app

# Copy built jar
COPY --from=builder /app/target/SiriAnalyzer-0.0.1-SNAPSHOT.jar ./app.jar


COPY docker/entrypoint.sh .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
