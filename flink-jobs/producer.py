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
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka ✅")
        break
    except Exception:
        print("Waiting for Kafka...")
        time.sleep(5)

FRAUD_PROBABILITY = 0.05
MERCHANTS = ['grocery','restaurant','electronics','pharmacy',
             'gas_station','airline','hotel','online_retail','atm','casino']

# Base coordinates for countries (lat, lon)
COUNTRY_COORDS = {
    'RU': (60.0, 90.0),
    'NG': (9.0, 8.0),
    'UA': (48.0, 31.0),
    'IN': (20.0, 77.0)
}

def generate_transaction():
    user_id = 'USER_' + str(random.randint(1, 200)).zfill(4)
    is_fraud = random.random() < FRAUD_PROBABILITY
    if is_fraud:
        amount = round(random.uniform(5000, 20000), 2)
        country = random.choice(['RU', 'NG', 'UA'])
        merchant = random.choice(['casino', 'atm', 'online_retail'])
    else:
        amount = round(random.uniform(10, 500), 2)
        country = 'IN'
        merchant = random.choice(MERCHANTS)
        
    # Geospatial data mapping
    base_lat, base_lon = COUNTRY_COORDS[country]
    # Add some random scatter to the coordinates for visual spread
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
        'card_last4': str(random.randint(1000, 9999)),
        'timestamp': datetime.utcnow().isoformat(),
        'is_fraud_label': is_fraud
    }

print('Sending transactions... Press Ctrl+C to stop.')
count = 0
while True:
    txn = generate_transaction()
    producer.send('transactions',
                  key=txn['user_id'].encode('utf-8'),
                  value=txn)
    count += 1
    status = 'FRAUD' if txn['is_fraud_label'] else 'OK'
    print(f'[{count}] {txn["user_id"]} | ${txn["amount"]} | {txn["country"]} | lat:{txn["lat"]}, lon:{txn["lon"]} | {status}')
    time.sleep(0.5)