<div align="center">
   <h1>GTFSynq</h1> 

   A modern, high-performance application for processing, storing and analyzing *GTFS-RT (General Transit Feed Specification - Real-Time)* data

   [![Spring Boot](https://img.shields.io/badge/Spring_Boot-4-6DB33F?logo=spring-boot&logoColor=white)](https://spring.io/projects/spring-boot)
   [![Java](https://img.shields.io/badge/Java-25-oracle?logo=java&logoColor=white)](https://www.java.com)
   [![TimescaleDB](https://img.shields.io/badge/TimescaleDB-PostgreSQL-468B97?logo=timescale&logoColor=white)](https://www.timescale.com)
   [![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?logo=apache-kafka&logoColor=white)](https://kafka.apache.org)
   [![Zed](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/zed-industries/zed/main/assets/badge/v0.json)](https://zed.dev)

</div>


## ✨ **Features**

- **Real-time Processing**: Ingest and process GTFS-RT feeds with low latency.
- **Time-series Storage**: Optimized storage for transit data using TimescaleDB hypertables.
- **Kafka Integration**: Scalable message streaming with Apache Kafka.
- **Protobuf Support**: Efficient serialization/deserialization of GTFS-RT messages.
- **Metrics & Monitoring**: Built-in Actuator endpoints with Prometheus support.
- **Native Compilation**: GraalVM native image support for fast startup and low memory footprint.


## 🛠️ **Getting Started**

[![Spring Boot](https://img.shields.io/badge/Spring_Boot-4-6DB33F?logo=spring-boot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Java](https://img.shields.io/badge/Java-25-oracle?logo=java&logoColor=white)](https://www.java.com)
[![TimescaleDB](https://img.shields.io/badge/TimescaleDB-PostgreSQL-468B97?logo=timescale&logoColor=white)](https://www.timescale.com)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?logo=apache-kafka&logoColor=white)](https://kafka.apache.org)
[![Zed](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/zed-industries/zed/main/assets/badge/v0.json)](https://zed.dev)

### Prerequisites
- Java 25+.
- Docker & Docker Compose.
- Maven 3.9+.

### Quick Start
1. **Clone the repository**:
   ```bash
   git clone https://github.com/evogel/GTFSynq.git
   cd GTFSynq
   ```

2. **Start the infrastructure**:
   ```bash
   docker-compose up -d
   ```

3. **Build and run**:
   ```bash
   ./run.sh
   ```
   or use Zed's built-in Maven support.

4. **Access the API**:
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
│   └── entrypoint.sh     # Container entrypoint
├── docker-compose.yaml   # Service orchestration
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
| **Backend** | [Spring Boot 4](https://spring.io/projects/spring-boot) | Modern Java framework with GraalVM native support |
| **Database** | [TimescaleDB](https://www.timescale.com) | Time-series optimized PostgreSQL for transit data |
| **Streaming** | [Apache Kafka 4](https://kafka.apache.org) | Distributed event streaming platform |
| **Protocol Buffers** | Protobuf 4 | Efficient data serialization |
| **Language** | Java 25 | Latest LTS with cutting-edge features |
