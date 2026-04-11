# Real-Time Enterprise Fraud Detection Pipeline

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/amrutatarage/fraud-detection-project)


https://github.com/user-attachments/assets/507df2d8-2b0f-4df5-96cb-22e5a5a50ae2


**Kafka • Flink 2.0 • PyFlink • ML • Docker • Grafana • PostgreSQL**

## 🚀 Experience it Live (Zero Setup)
Don't want to install Docker locally? Click the **Open in GitHub Codespaces** badge above to launch a free cloud environment. 
Once the browser-based VS Code loads, open the terminal and run:
```powershell
./start_pipeline.ps1
```

---

# 🏗️ Technical Architecture & Manual Deployment Guide

This guide provides the complete manual execution sequence to understand how data flows through the pipeline from model training to real-time visualization.

### Phase 1: Environment Setup
If running locally, clone the repository and navigate to the project root. If using **GitHub Codespaces**, the environment is automatically prepared for you.

```bash
git clone https://github.com/amrutatarage/fraud-detection-project.git
cd fraud-detection-project
```

### Phase 2: Train the ML Model
Before starting the pipeline, we must train the intelligence. This script generates a synthetic dataset, trains a Logistic Regression model, and serializes it to `fraud_model.pkl`.

```bash
python train_model.py
```
> [!NOTE]
> The model achieves **~93.60% accuracy** and is used by the Flink job for real-time inference.

### Phase 3: Boot Infrastructure
Launch the containerized stack (Zookeeper, Kafka, Flink JobManager, Flink TaskManager, Postgres, and Grafana).

```bash
docker compose up -d
```
> [!IMPORTANT]
> **Wait ~30 seconds** for all services to complete their health checks and for Kafka brokers to initialize.

### Phase 4: Start Data Generation
Open a **NEW terminal window**. We need to start the transaction stream. This script mimics thousands of users making bank transactions every second.

```bash
python3 producer.py
```
*Keep this terminal running to maintain the data flow.*

### Phase 5: Submit the Flink Job
Open a **THIRD terminal window**. Now, we deploy the PyFlink detection logic into the Flink cluster. This job consumes the Kafka stream, applies the ML model, and saves fraud alerts into Postgres.

```bash
docker exec -it flink-jobmanager flink run --python /opt/flink/jobs/fraud_detection_v2.py
```

### Phase 6: View Live Dashboard
The pipeline is now fully operational! Navigate to the Grafana dashboard to view the fraud alerts in real-time.

*   **URL:** `http://localhost:3000`
*   **Username:** `admin`
*   **Password:** `admin`

### Phase 7: Teardown
Once you are finished with the demo, gracefully stop and remove all containers.

```bash
docker compose down
```
