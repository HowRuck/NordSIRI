# syntax=docker/dockerfile:1
# Stage 1: Build
FROM docker.io/gradle:9.4.1-jdk26 AS builder

WORKDIR /app

# Enable Gradle caching and configuration cache
ENV GRADLE_USER_HOME=/app/.gradle

# Copy only files for dependency resolution
COPY build.gradle settings.gradle ./

# Optimization: Use BuildKit cache mount for the .gradle folder
# This persists your dependencies across builds on the same machine
RUN --mount=type=cache,target=/app/.gradle \
    gradle build --no-daemon --configuration-cache > /dev/null 2>&1 || true

# Copy source and build the JAR
COPY src ./src
RUN --mount=type=cache,target=/app/.gradle \
    gradle bootJar -x test --no-daemon --configuration-cache

# Stage 2: Minimal Runtime
# Using Alpine to reduce attack surface and image size
FROM docker.io/eclipse-temurin:26-jre-alpine

# Security: Run as a non-root user
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

WORKDIR /app

# Gradle output is in build/libs/
COPY --from=builder /app/build/libs/SiriAnalyzer-0.0.1-SNAPSHOT.jar ./app.jar
COPY docker/entrypoint.sh .

# Note: In Alpine, ensure your entrypoint.sh uses #!/bin/sh (not /bin/bash)
ENTRYPOINT ["./entrypoint.sh"]
