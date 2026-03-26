import json
from kafka import KafkaConsumer
from collections import defaultdict
import time
import joblib
model = joblib.load("fraud_model.pkl")
import pandas as pd



consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

print("Listening for transactions...\n")

# Track user behavior
user_last_time = {}
user_last_country = {}
user_txn_count = defaultdict(int)

for message in consumer:
    txn = message.value

    user = txn['user_id']
    amount = txn['amount']
    country = txn['country']
    now = time.time()

    # 👇 ADD ML LOGIC HERE

    country_flag = 0 if country == "IN" else 1

    rapid_flag = 0
    if user in user_last_time:
        if now - user_last_time[user] < 2:
            rapid_flag = 1

    features = pd.DataFrame(
    [[amount, country_flag, rapid_flag]],
    columns=["amount", "country", "rapid"]
)

    prediction = model.predict(features)[0]

    if prediction == 1:
        print(f"🤖 ML FRAUD DETECTED: ${amount} | {user}")
    else:
        print(f"✅ NORMAL: ${amount}")


    # Update tracking
    user_last_time[user] = now
    user_last_country[user] = country