<div align="center">
   <h1>GTFSynq</h1> 

   A modern, high-performance application for processing, storing and analyzing *GTFS-RT (General Transit Feed Specification - Real-Time)* data

   ![Spring](https://img.shields.io/badge/spring-%236DB33F.svg?style=for-the-badge&logo=spring&logoColor=white)
   ![Java](https://img.shields.io/badge/java-%23ED8B00.svg?style=for-the-badge&logo=openjdk&logoColor=white)
   ![TimescaleDB](https://img.shields.io/badge/timescaledb-36764?style=for-the-badge&logo=Timescale&logoColor=black&color=%23E6EE8A)
   ![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
   ![Zed](https://img.shields.io/badge/zed-084CCF.svg?style=for-the-badge&logo=zedindustries&logoColor=white)

</div>


## ✨ **Features**

- **Real-time Processing**: Ingest and process GTFS-RT feeds with low latency.
- **Time-series Storage**: Optimized storage for transit data using TimescaleDB hypertables.
- **Kafka Integration**: Scalable message streaming with Apache Kafka.
- **Protobuf Support**: Efficient serialization/deserialization of GTFS-RT messages.
- **Metrics & Monitoring**: Built-in Actuator endpoints with Prometheus support.

## 🛠️ **Getting Started**

### Prerequisites
- Java 26
- Docker & Docker Compose
- Maven 3.9

### Quick Start
1. **Clone the repository**:
   ```bash
   git clone https://github.com/evogel/GTFSynq.git
   cd GTFSynq
   ```

2. **Fast setup with full Docker build** (recommended):
   ```bash
   docker compose -f docker/docker-compose.yaml up -d
   ```
   This builds the application image and starts all services (Kafka, TimescaleDB, app).

   Access the API at `http://localhost:8888`.

3. **Or manual local development**:
   - Start infrastructure:
     ```bash
     docker-compose up -d
     ```
   - Build and run:
     ```bash
     mvn spring-boot:run
     ```
     or use Zed's built-in Maven support.

   Access the API:
   - Application: `http://localhost:8080`
   - Actuator: `http://localhost:8080/actuator`
   - Prometheus Metrics: `http://localhost:8080/actuator/prometheus`

## 📦 **Project Structure**

```
GTFSynq/
├── src/                  # Java source code
│   ├── main/             # Application code
│   └── test/             # Test cases
├── docker/               # Docker configurations
│   ├── Dockerfile        # Multi-stage build
│   ├── docker-compose.yaml # Full stack with app image build
│   └── entrypoint.sh     # Container entrypoint
├── docker-compose.yaml   # Service orchestration (infrastructure only)
├── pom.xml               # Maven build configuration
└── run.sh                # Convenience startup script
```

## 🎯 **Use Cases**

- Real-time transit data visualization.
- Transit delay analysis and predictions.
- Historical transit data storage and querying.
- GTFS-RT feed validation and monitoring.
- Transit agency integration.

## 📝 **Note**

> **🎓 Educational Project**: GTFSynq is primarily intended for educational purposes and skill development. It serves as a platform to explore modern Java development, Spring Boot 4, time-series databases, and real-time data processing. While functional, the code may not follow all production-grade architectural patterns or performance optimizations.

## 🚀 **Tech Stack**

| Component | Technology | Purpose |
|-----------|------------|---------|
| **IDE** | [Zed](https://zed.dev) | Ultra-fast, collaborative code editor |
| **Backend** | [Spring Boot 4](https://spring.io/projects/spring-boot) | Modern Java framework |
| **Database** | [TimescaleDB](https://www.timescale.com) | Time-series optimized PostgreSQL for transit data |
| **Streaming** | [Apache Kafka 4](https://kafka.apache.org) | Distributed event streaming platform |
| **Protocol Buffers** | Protobuf 4 | Efficient data serialization |
| **Language** | Java 26 | Latest LTS with cutting-edge features |
