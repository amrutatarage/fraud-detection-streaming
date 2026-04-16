# Real-Time Enterprise Fraud Detection Pipeline

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/amrutatarage/fraud-detection-project)

https://github.com/user-attachments/assets/507df2d8-2b0f-4df5-96cb-22e5a5a50ae2

**Kafka • Flink 2.0 • PyFlink • ML • Docker • Grafana • PostgreSQL**

---

## 🚀 Deployment Guide

This pipeline is fully containerized. You can run it instantly in the cloud, or locally on your own machine.

### Option A: Run in GitHub Codespaces (Cloud / Linux)
If you clicked the **Open in Codespaces** badge above, you are running a Linux environment. Please follow these 7 manual execution phases to see how the architecture connects:

#### Phase 1: Environment Setup
The GitHub Codespace environment is automatically prepared for you. Open the terminal at the bottom of the screen.

#### Phase 2: Train the ML Model
Before starting the pipeline, we must train the intelligence. This script generates a synthetic dataset, trains a Logistic Regression model (~93.60% accuracy), and serializes it to `fraud_model.pkl`.
```bash
python train_model.py
```

#### Phase 3: Boot Infrastructure
Launch the containerized stack (Zookeeper, Kafka, Flink JobManager, Flink TaskManager, Postgres, and Grafana).
```bash
docker compose up -d
```
> [!IMPORTANT]
> **Wait ~30 seconds** for all services to complete their health checks and for Kafka brokers to initialize.

#### Phase 4: Start Data Generation
Open a **NEW terminal window** (click the `+` icon in the terminal). Install the required libraries and start the transaction stream.
```bash
pip install faker kafka-python
python3 producer.py
```
*Keep this terminal running to maintain the data flow.*

#### Phase 5: Submit the Flink Job
Open a **THIRD terminal window**. Deploy the PyFlink detection logic into the Flink cluster. This job consumes the Kafka stream, applies the ML model, and saves fraud alerts into Postgres.
```bash
docker exec -it flink-jobmanager flink run --python /opt/flink/jobs/fraud_detection_v2.py
```

#### Phase 6: View Live Dashboard
The pipeline is now fully operational! Navigate to the Grafana dashboard to view the fraud alerts in real-time.
* **URL:** `http://localhost:3000` (or click the "Open in Browser" pop-up in Codespaces)
* **Username:** `admin`
* **Password:** `admin`

#### Phase 7: Teardown
Once you are finished with the demo, gracefully stop and remove all containers.
```bash
docker compose down
```

---

### Option B: Run Locally (Windows / Docker Desktop)
If you prefer to run this locally on a Windows machine with Docker Desktop installed, you can use the automated PowerShell deployment script.

**1. Clone the repository:**
```bash
git clone [https://github.com/amrutatarage/fraud-detection-project.git](https://github.com/amrutatarage/fraud-detection-project.git)
cd fraud-detection-project
```

**2. Run the automated boot script:**
```powershell
.\start_pipeline.ps1
```
*(Once running, navigate to `http://localhost:3000` to view the Grafana dashboard. Login: `admin` / `admin`).*