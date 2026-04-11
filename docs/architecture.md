# GTFSynq Architecture

## Overview
GTFSynq is a Spring Boot application for processing GTFS (General Transit Feed Specification) data with Kafka integration.

## Layered Architecture

### 1. Application Layer
**Location**: `org.example.gtfsynq.application`

Responsibilities:
- Entry point (SiriAnalyzerApplication)
- Application services (orchestration)
- DTOs (Data Transfer Objects)
- Exception handling
- REST controllers

### 2. Domain Layer
**Location**: `org.example.gtfsynq.domain`

Responsibilities:
- Business logic
- Domain models
- Domain services
- Repository interfaces

### 3. Infrastructure Layer
**Location**: `org.example.gtfsynq.infrastructure`

Responsibilities:
- External integrations (Kafka, databases)
- Configuration
- Protobuf serialization
- Persistence implementations

## Package Structure

```
src/
├── main/
│   ├── java/org/example/gtfsynq/
│   │   ├── application/      # Application layer
│   │   ├── domain/           # Domain layer
│   │   ├── infrastructure/   # Infrastructure layer
│   │   └── util/             # Utilities
│   └── resources/
└── test/                    # Test packages mirror main structure
```

## Key Components

### Kafka Integration
- **Location**: `infrastructure.kafka`
- Handles GTFS data streaming to Kafka topics
- Configurable through application properties

### Protobuf
- **Location**: `infrastructure.protobuf`
- Protocol Buffers for efficient data serialization
- Generated from `.proto` files

### Services
- **Location**: `application.service`
- Application services for business logic orchestration
- Separated from domain services

## Build & Configuration

- **Build Tool**: Maven
- **Spring Boot**: 4.0
- **Java**: 25
- **Configuration**: `application.yaml`


## Dependencies

- Spring Boot Web MVC
- Spring Kafka
- Protocol Buffers
- Lombok
- Micrometer (Prometheus)
