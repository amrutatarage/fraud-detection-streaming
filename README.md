\# Real-Time Fraud Detection System



This project simulates a real-time fraud detection system using:



\- Apache Kafka (streaming)

\- Python (producer + consumer)

\- Machine Learning (RandomForest)



\## How it works



Producer → Kafka → Consumer → Fraud Detection



\## Run



1\. Start Kafka:

docker-compose up -d



2\. Run producer:

python producer.py



3\. Run consumer:

python consumer.py

