# Real-Time Fraud Detection Pipeline

**Kafka • Flink 2.0 • PyFlink • ML • Docker • Grafana • PostgreSQL**


## Overview

Real-time fraud detection system that processes thousands of bank transactions per second and detects fraudulent ones instantly under 100 milliseconds. Built entirely with free open-source tools running inside Docker containers.

## Architecture

```
Docker Container: Producer (Faker)
        |
        v
Apache Kafka  (message streaming - port 9092)
        |
        v
Apache Flink 2.0  (real-time fraud detection - port 8081)
        |              |
        v              v
PostgreSQL         Console Output
(fraud alerts)     (live terminal)
        |
        v
Grafana Dashboard  (live charts - port 3000)
```

## Tech Stack

| Tool | Purpose |
|------|---------|
| Docker | Container orchestration |
| Apache Kafka | Message streaming |
| Apache Flink 2.0 | Real-time stream processing |
| PyFlink | Python API for Flink |
| scikit-learn | ML fraud detection model |
| PostgreSQL 16 | Fraud alerts storage |
| Grafana | Live monitoring dashboard |
| Faker | Synthetic transaction data |

## Project Structure

```
fraud-detection-project/
├── docker-compose.yml          # Starts all 7 containers
├── Dockerfile                  # Custom Flink image with Python + ML
├── Dockerfile.producer         # Producer container image
├── init.sql                    # Auto-creates PostgreSQL table
├── write_config.py             # Regenerates docker-compose.yml
├── producer.py                 # Local producer (for testing)
├── consumer.py                 # Local consumer (for testing)
├── train_model.py              # Trains ML fraud model
├── fraud_model.pkl             # Saved trained model
├── .gitignore
├── README.md
└── flink-jobs/
    ├── producer.py             # Dockerized producer (Kafka retry)
    ├── train_model.py          # Model training (container copy)
    ├── fraud_detection_job.py  # Basic fraud detection (console)
    └── fraud_detection_v2.py   # Full pipeline (+ PostgreSQL)
```

## Quick Start

```powershell
# Step 1: Train the ML model (one-time)
py -3.14 train_model.py

# Step 2: Build and start all containers
docker compose up -d --build

# Step 3: Wait ~30 seconds for services to initialize, then submit Flink job
docker exec -it flink-jobmanager python /opt/flink/jobs/fraud_detection_v2.py

# Step 4: Open dashboards
# Flink UI:  http://localhost:8081
# Grafana:   http://localhost:3000  (admin / admin)
```

The producer starts automatically inside Docker and feeds transactions to Kafka.  
Flink processes them with the ML model and saves fraud alerts to PostgreSQL.  
Grafana visualizes everything in real time.

## Daily Commands

```powershell
# Start
cd C:\projects\fraud-detection-project
docker compose up -d

# Stop
docker compose down

# Rebuild after code changes
docker compose up -d --build

# View producer logs
docker logs -f fraud-producer

# View Flink job output
docker logs -f flink-taskmanager

# Check fraud alerts in PostgreSQL
docker exec -it postgres psql -U admin -d frauddb -c "SELECT * FROM fraud_alerts ORDER BY detected_at DESC LIMIT 10;"

# Fix broken network
docker compose down --remove-orphans
docker network prune -f
docker compose up -d
```

## Kafka Listener Configuration

| Listener | Address | Used By |
|----------|---------|---------|
| INTERNAL | `kafka:29092` | Flink jobs, containerized producer |
| EXTERNAL | `localhost:9092` | Host machine scripts (`producer.py`, `consumer.py`) |

## Dashboards

- **Flink UI**: http://localhost:8081
- **Grafana**: http://localhost:3000 (admin/admin)
