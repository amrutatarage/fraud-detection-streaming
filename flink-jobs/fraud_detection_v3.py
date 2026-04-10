import json
import joblib
import numpy as np
import psycopg2
import requests
import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor
from pyflink.common.typeinfo import Types

# Load ML model from mounted volume
bundle = joblib.load('/opt/flink/jobs/fraud_model.pkl')
model = bundle['model']
scaler = bundle['scaler']

class FraudDetectorProcess(KeyedProcessFunction):
    """
    Stateful processing function to track a user's 10-minute sliding window average 
    and identify high-velocity or high-ratio spending anomalies.
    """
    def __init__(self):
        self.history_state = None

    def open(self, runtime_context: RuntimeContext):
        # We use ListState to store [amount, timestamp] of recent transactions bounds
        descriptor = ListStateDescriptor('spending_history', Types.STRING())
        self.history_state = runtime_context.get_list_state(descriptor)

    def process_element(self, json_str, ctx: 'KeyedProcessFunction.Context'):
        try:
            txn = json.loads(json_str)
            amount = txn['amount']
            country = txn['country']
            user_id = txn['user_id']
            lat = txn.get('lat', 0.0)
            lon = txn.get('lon', 0.0)
            fraud_reason = txn.get('fraud_reason', 'None')
            txn_time = datetime.datetime.utcnow().timestamp()
            
            # 1. Retrieve historical spending for this user
            history = list(self.history_state.get())
            if not history:
                history = []
                
            # 2. Filter history to only the last 10 minutes (600 seconds)
            valid_history = []
            total_spent = 0.0
            
            for item in history:
                hist_amount, hist_time = json.loads(item)
                if txn_time - hist_time <= 600:
                    valid_history.append(item)
                    total_spent += hist_amount
            
            # 3. Calculate 10-minute sliding average
            avg_spending = 0.0
            if len(valid_history) > 0:
                avg_spending = total_spent / len(valid_history)
                
            # 4. ML Feature Override: Is this swipe >300% of historical average?
            # We must have at least 2 prior swipes in the window to establish a baseline average
            is_anomaly = 1 if (len(valid_history) >= 2 and avg_spending > 0 and amount > avg_spending * 3.0) else 0
            
            # 5. Extract core features and predict
            country_risk = 2 if country in ['RU', 'NG', 'UA'] else 0
            features = scaler.transform([[amount, is_anomaly, country_risk]])
            prediction = model.predict(features)[0]

            # 6. Update user's history state
            valid_history.append(json.dumps([amount, txn_time]))
            self.history_state.update(valid_history)
            
            status_msg = f"[NORMAL] {user_id} | ${amount} | Avg:${avg_spending:.2f}"

            # Detect fraud either by ML prediction or if the producer heuristic flagged explicitly
            if prediction == 1 or fraud_reason != "None":
                # Synchronous Database Insertion
                conn = psycopg2.connect(
                    host='postgres', database='frauddb',
                    user='admin', password='password'
                )
                cur = conn.cursor()
                cur.execute(
                    'INSERT INTO fraud_alerts (user_id, amount, country, lat, lon, fraud_reason) VALUES (%s, %s, %s, %s, %s, %s)',
                    (user_id, amount, country, lat, lon, fraud_reason)
                )
                conn.commit()
                conn.close()
                status_msg = f"[FRAUD SAVED] {user_id} | ${amount} | {country} | Reason: {fraud_reason} | (+{int((amount/avg_spending)*100)}% spike)" if avg_spending > 0 else f"[FRAUD SAVED] {user_id} | ${amount} | {country} | Reason: {fraud_reason}"
                
                # 7. Asynchronous Alerting (Mock Webhook) for High Confidence Fraud
                if amount > 15000:
                    webhook_payload = {
                        "alert": "HIGH CONFIDENCE FRAUD",
                        "user": user_id,
                        "amount": amount,
                        "location": {"lat": lat, "lon": lon},
                        "historical_avg": avg_spending
                    }
                    try:
                        # Attempt to post to a mock URL with a strict timeout so the streaming pipeline doesn't block
                        requests.post("http://localhost:9999/mock-webhook", json=webhook_payload, timeout=0.1)
                    except requests.exceptions.RequestException:
                        # Fallback to stdout log for demonstration
                        print(f"WEBHOOK TRIGGERED (Simulated Async): {json.dumps(webhook_payload)}")
            
            return status_msg

        except Exception as e:
            return f'Error: {str(e)}'

env = StreamExecutionEnvironment.get_execution_environment()

# Kafka Source strictly compatible with Flink 1.20.0 Builder API
source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("transactions") \
    .set_group_id("fraud-v3") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

# Execute Stateful Processing
results = stream \
    .key_by(lambda json_str: json.loads(json_str)['user_id'], key_type=Types.STRING()) \
    .process(FraudDetectorProcess(), output_type=Types.STRING())

results.print()

env.execute('Advanced Fraud Detection V3 - Sliding Window & Geospatial')