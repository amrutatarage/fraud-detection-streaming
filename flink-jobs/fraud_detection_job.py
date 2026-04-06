import json
import joblib
import numpy as np
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common import WatermarkStrategy

# Load ML model (mounted into container at this path)
bundle = joblib.load('/opt/flink/jobs/fraud_model.pkl')
model = bundle['model']
scaler = bundle['scaler']

def detect_fraud(transaction_json):
    try:
        txn = json.loads(transaction_json)
        amount = txn['amount']
        country_risk = 2 if txn['country'] in ['RU', 'NG', 'UA'] else 0
        velocity = 1  # simplified
        features = scaler.transform([[amount, velocity, country_risk]])
        prediction = model.predict(features)[0]
        status = 'FRAUD' if prediction == 1 else 'NORMAL'
        return f'[{status}] {txn["user_id"]} | ${amount} | {txn["country"]}'
    except Exception as e:
        return f'Error: {str(e)}'

env = StreamExecutionEnvironment.get_execution_environment()

source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("transactions") \
    .set_group_id("flink-fraud-detector") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
results = stream.map(detect_fraud)
results.print()

env.execute('Real-Time Fraud Detection')