import json, time, random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# Retry loop — wait for Kafka to be ready
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:29092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=10, # Batching for high throughput
            batch_size=16384
        )
        print("Connected to Kafka ✅")
        break
    except Exception:
        print("Waiting for Kafka...")
        time.sleep(5)

MERCHANTS = ['grocery','restaurant','electronics','pharmacy',
             'gas_station','airline','hotel','online_retail','atm','casino']

# Base coordinates for countries (lat, lon)
COUNTRY_COORDS = {
    'RU': (60.0, 90.0),
    'NG': (9.0, 8.0),
    'UA': (48.0, 31.0),
    'IN': (20.0, 77.0),
    'US': (37.0, -95.0)
}

card_testing_users = {}
travel_anomaly_users = {}

def generate_transaction():
    user_id = 'USER_' + str(random.randint(1, 200)).zfill(4)
    
    # Default Normal Transaction
    amount = round(random.uniform(10, 500), 2)
    country = 'IN'
    merchant = random.choice(MERCHANTS)
    fraud_reason = "None"
    is_fraud = False

    # 1. Card Testing Scenario (3 tiny txns then huge spike)
    if random.random() < 0.01 or user_id in card_testing_users:
        if user_id not in card_testing_users:
            card_testing_users[user_id] = 0
            
        step = card_testing_users[user_id]
        if step < 3:
            amount = 1.00
            fraud_reason = "Card Testing Setup"
            card_testing_users[user_id] += 1
            is_fraud = False
        else:
            amount = round(random.uniform(5000, 20000), 2)
            fraud_reason = "Card Testing Spike"
            is_fraud = True
            del card_testing_users[user_id]
            
    # 2. Travel Velocity Anomaly Scenario (US then RU instantly)
    elif random.random() < 0.01 or user_id in travel_anomaly_users:
        if user_id not in travel_anomaly_users:
            country = 'US'
            merchant = 'hotel'
            fraud_reason = "Travel Setup"
            travel_anomaly_users[user_id] = True
            is_fraud = False
        else:
            country = 'RU'
            merchant = 'atm'
            fraud_reason = "Travel Velocity Anomaly"
            is_fraud = True
            del travel_anomaly_users[user_id]
            
    # 3. High Value Unknown Merchant (Standard Fraud)
    elif random.random() < 0.02:
        amount = round(random.uniform(5000, 20000), 2)
        country = random.choice(['RU', 'NG', 'UA'])
        merchant = random.choice(['casino', 'atm', 'online_retail'])
        fraud_reason = "High Value Unknown Merchant"
        is_fraud = True
        
    base_lat, base_lon = COUNTRY_COORDS.get(country, COUNTRY_COORDS['IN'])
    lat = round(base_lat + random.uniform(-3.0, 3.0), 4)
    lon = round(base_lon + random.uniform(-3.0, 3.0), 4)

    return {
        'transaction_id': fake.uuid4(),
        'user_id': user_id,
        'amount': amount,
        'currency': 'USD',
        'merchant_type': merchant,
        'country': country,
        'lat': lat,
        'lon': lon,
        'fraud_reason': fraud_reason,
        'card_last4': str(random.randint(1000, 9999)),
        'timestamp': datetime.utcnow().isoformat(),
        'is_fraud_label': is_fraud
    }

print('Sending transactions at ~500 TPS... Press Ctrl+C to stop.')
count = 0
start_time = time.time()
TARGET_TPS = 500

while True:
    txn = generate_transaction()
    producer.send('transactions',
                  key=txn['user_id'].encode('utf-8'),
                  value=txn)
    count += 1
    
    # Print every 100th to not overwhelm stdout, saving CPU
    if count % 100 == 0:
        status = 'FRAUD' if txn['is_fraud_label'] else 'OK'
        print(f'[{count}] {txn["user_id"]} | ${txn["amount"]} | {txn["country"]} | {txn["fraud_reason"]} | {status}')
        
    # Rate Limiter
    elapsed = time.time() - start_time
    expected_time = count / TARGET_TPS
    if expected_time > elapsed:
        time.sleep(expected_time - elapsed)