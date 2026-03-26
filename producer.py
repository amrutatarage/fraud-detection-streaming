import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

FRAUD_PROBABILITY = 0.05
MERCHANTS = ['grocery','restaurant','electronics','pharmacy','gas_station','airline','hotel','online_retail','atm','casino']

def generate_transaction():
    user_id = 'USER_' + str(random.randint(1, 10)).zfill(4)
    is_fraud = random.random() < FRAUD_PROBABILITY
    if is_fraud:
        amount = round(random.uniform(5000, 20000), 2)
        country = random.choice(['RU', 'NG', 'UA'])
        merchant = random.choice(['casino', 'atm', 'online_retail'])
    else:
        amount = round(random.uniform(10, 500), 2)
        country = random.choice(['IN', 'RU', 'NG'])
        merchant = random.choice(MERCHANTS)
    return {
        'transaction_id': fake.uuid4(),
        'user_id': user_id,
        'amount': amount,
        'currency': 'USD',
        'merchant_type': merchant,
        'country': country,
        'card_last4': str(random.randint(1000, 9999)),
        'timestamp': datetime.utcnow().isoformat(),
        'is_fraud_label': is_fraud
    }

print('Sending transactions to Kafka... Press Ctrl+C to stop.')
count = 0
while True:
    txn = generate_transaction()
    producer.send('transactions', key=txn['user_id'].encode('utf-8'), value=txn)
    count += 1
    status = 'FRAUD' if txn['is_fraud_label'] else 'OK'
    print('[' + str(count) + '] ' + txn['user_id'] + ' | $' + str(txn['amount']) + ' | ' + txn['country'] + ' | ' + status)
    time.sleep(0.5)
