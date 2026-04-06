import json, joblib, numpy as np
import psycopg2
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common import WatermarkStrategy

bundle = joblib.load('/opt/flink/jobs/fraud_model.pkl')
model = bundle['model']
scaler = bundle['scaler']

def detect_and_save(transaction_json):
    try:
        txn = json.loads(transaction_json)
        amount = txn['amount']
        country_risk = 2 if txn['country'] in ['RU', 'NG', 'UA'] else 0
        features = scaler.transform([[amount, 1, country_risk]])
        prediction = model.predict(features)[0]

        if prediction == 1:
            conn = psycopg2.connect(
                host='postgres', database='frauddb',
                user='admin', password='password'
            )
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO fraud_alerts (user_id, amount, country) VALUES (%s, %s, %s)',
                (txn['user_id'], amount, txn['country'])
            )
            conn.commit()
            conn.close()
            return f'[FRAUD SAVED] {txn["user_id"]} | ${amount} | {txn["country"]}'

        return f'[NORMAL] {txn["user_id"]} | ${amount}'
    except Exception as e:
        return f'Error: {str(e)}'

env = StreamExecutionEnvironment.get_execution_environment()
source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("transactions") \
    .set_group_id("fraud-v2") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()
stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
stream.map(detect_and_save).print()
env.execute('Fraud Detection V2')
