# Stage 1: Build
FROM docker.io/eclipse-temurin:26-jdk-alpine AS builder

WORKDIR /app

# Copy everything at once
COPY . .

# Build without using or saving any cache
RUN ./gradlew bootJar -x test --no-daemon --no-build-cache --no-configuration-cache

# Stage 2: Runtime
FROM docker.io/eclipse-temurin:26-jre-alpine

WORKDIR /app

# Copy the generated JAR
COPY --from=builder /app/build/libs/app.jar ./app.jar
COPY docker/entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
