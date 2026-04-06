import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='test-consumer'
)

print('Reading from Kafka... Press Ctrl+C to stop.')
for msg in consumer:
    txn = msg.value
    status = 'FRAUD' if txn['is_fraud_label'] else 'OK'
    print(f'{txn["user_id"]} | ${txn["amount"]} | {txn["country"]} | {status}')