<div align="center">
  <h1>GTFSynq</h1>

  <img alt="Spring" src="https://img.shields.io/badge/spring-%236DB33F.svg?style=for-the-badge&logo=spring&logoColor=white" />
  <img alt="Java" src="https://img.shields.io/badge/java-%23ED8B00.svg?style=for-the-badge&logo=openjdk&logoColor=white" />
  <img alt="Gradle" src="https://img.shields.io/badge/gradle-02303A.svg?style=for-the-badge&logo=gradle&logoColor=white" />
  <img alt="TimescaleDB" src="https://img.shields.io/badge/timescaledb-36764?style=for-the-badge&logo=Timescale&logoColor=black&color=%23E6EE8A" />
  <img alt="Apache Kafka" src="https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka" />
  <img alt="Zed" src="https://img.shields.io/badge/zed-084CCF.svg?style=for-the-badge&logo=zedindustries&logoColor=white" />
</div>

<br />

GTFSynq is a modern Spring Boot transit data platform for ingesting, processing, storing, and analyzing **GTFS-RT (General Transit Feed Specification - Real-Time)** feeds and **GTFS CSV** static feed data

It is built with **Gradle**, runs on **Java 26**, and is designed to work with **Kafka** and **TimescaleDB** for real-time transit data processing and time-series storage

## Features

- **Real-time processing**: ingest and process GTFS-RT feeds with low latency
- **Static feed support**: model GTFS CSV data alongside realtime transit updates
- **Time-series storage**: store transit data efficiently in TimescaleDB
- **Kafka integration**: stream transit payloads through Apache Kafka
- **Protobuf support**: encode and decode GTFS-RT messages efficiently
- **Observability**: Spring Boot Actuator with Prometheus metrics
- **Docker-based runtime**: run the full stack with a single Compose command

## Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| Backend | Spring Boot 4 | Application framework |
| Build tool | Gradle | Build, test, and packaging |
| Language | Java 26 | Application language |
| Streaming | Apache Kafka | Event streaming and transport |
| Database | TimescaleDB | Time-series PostgreSQL storage |
| Serialization | Protobuf 4 | GTFS-RT message encoding |
| Monitoring | Spring Actuator + Prometheus | Health and metrics |

## Prerequisites

To run the project locally, you need:

- **Java 26**
- **Gradle Wrapper**  
  The repository includes a Gradle wrapper, so use `./gradlew` for all build and run commands.
- **Docker** and **Docker Compose** if you want to run Kafka and TimescaleDB in containers.

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/evogel/GTFSynq.git
cd GTFSynq
```

### 2. Build the application with Gradle

```bash
./gradlew clean bootJar
```

This creates the executable application JAR at:

```bash
build/libs/app.jar
```

### 3. Run the application locally

If Kafka and TimescaleDB are available on your machine, start the app with:

```bash
./gradlew bootRun
```

The application listens on:

- Application: `http://localhost:8888`
- Actuator: `http://localhost:8888/actuator`
- Prometheus metrics: `http://localhost:8888/actuator/prometheus`

## Running the Full Stack with Docker

The repository includes a Docker Compose setup that starts:

- the application
- Kafka
- TimescaleDB

To build and start everything:

```bash
docker compose -f docker/docker-compose.yaml up -d --build
```

The app is exposed on:

```bash
http://localhost:8888
```

### Stopping the stack

```bash
docker compose -f docker/docker-compose.yaml down
```

### Viewing logs

```bash
docker compose -f docker/docker-compose.yaml logs -f app
```

## Local Development Flow

A typical local development setup looks like this:

1. Start infrastructure services with Docker Compose.
2. Run the app with `./gradlew bootRun`.
3. Make changes in `src/main/java`.
4. Re-run `./gradlew test` and `./gradlew bootJar` as needed.

If you want to run only the infrastructure containers and keep the app on your host machine, start Kafka and TimescaleDB separately using the same Compose file, then run the app locally with `./gradlew`.

## Project Structure

```text
GTFSynq/
├── build.gradle
├── settings.gradle
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── entrypoint.sh
├── modules/
│   ├── shared/
│   ├── ingest-app/
│   ├── store-app/
│   └── api-app/
├── docs/
│   └── architecture.md
├── src/
│   ├── main/
│   │   ├── java/
│   │   ├── proto/
│   │   └── resources/
│   │       ├── application.yaml
│   │       └── db/migration/
│   └── test/
└── README.md
```

## Modules

### `modules/shared`
Contains code reused by multiple apps:

- GTFS domain models and DTOs
- protobuf-generated types
- message envelope encoding/decoding
- hashing utilities
- off-heap state store utilities
- GTFS formatting helpers

### `modules/ingest-app`
Responsible for getting data into Kafka:

- scheduled GTFS-RT polling
- native GTFS-RT parsing
- Kafka publishing
- feed/source configuration

### `modules/store-app`
Responsible for getting data out of Kafka and into PostgreSQL/TimescaleDB:

- Kafka Streams consumer
- deduplication
- batch persistence
- Flyway migrations
- JDBC-based repository writes

### `modules/api-app`
Reserved for the REST API layer:

- Spring Boot application entry point
- PostgreSQL-backed read access
- HTTP endpoints and query models

## Runtime Flow

```text
GTFS-RT / GTFS CSV sources
        ↓
   ingest-app
        ↓
      Kafka
        ↓
    store-app
        ↓
 PostgreSQL / TimescaleDB
        ↓
      api-app
```

## Configuration

Key runtime configuration lives in `src/main/resources/application.yaml`.

Important defaults:

- Application port: `8888`
- Kafka bootstrap server: `localhost:9092`
- PostgreSQL / TimescaleDB: `localhost:5432`
- Database name: `gtfsynq`

When running through Docker Compose, these values are overridden with container hostnames.

## Database Migrations

Database schema migrations are managed with Flyway and are located in:

```text
src/main/resources/db/migration
```

Migrations are applied automatically on startup when Flyway is enabled.

## Build and Test Commands

### Run tests

```bash
./gradlew test
```

### Build the application JAR

```bash
./gradlew bootJar
```

### Clean and build from scratch

```bash
./gradlew clean build
```

### Run the app from Gradle

```bash
./gradlew bootRun
```

## Docker Image Build

The Docker image is built in two stages:

1. Build the application with the Gradle wrapper inside a Gradle/JDK 26 container.
2. Copy the generated JAR into a lightweight Java 26 runtime image.

This means the container image always reflects the current Gradle build output from `build/libs/app.jar`.

## Monitoring

The application exposes Actuator endpoints for health and metrics.

Useful endpoints:

- `GET /actuator`
- `GET /actuator/health`
- `GET /actuator/prometheus`

## Educational Project

GTFSynq is primarily intended for educational and exploratory use. It is a good place to experiment with:

- modern Spring Boot development
- real-time transit feed ingestion
- Kafka streaming
- Flyway database migrations
- TimescaleDB time-series modeling
- Protobuf-based transport formats

## License

This project is licensed under the terms of the repository's `LICENSE` file.
