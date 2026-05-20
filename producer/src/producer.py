import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Set the target broker (internal K8s service)
KAFKA_BROKER = "kafka-service:9092"

print(f"🚀 Attempting to connect to Kafka at {KAFKA_BROKER}...")

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Successfully connected to Cluster Kafka!")
        break
    except Exception as e:
        print(f"❌ Connection failed: {e}. Retrying in 5s...")
        time.sleep(5)

def generate_event():
    return {
        "event_id": random.randint(10000, 99999),
        "user_id": random.randint(1, 1000),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "timestamp": int(time.time())
    }

print("🚀 Starting data ingestion stream...")
while True:
    event = generate_event()
    producer.send('transactions', event)
    print(f"Sent event: {event}")
    time.sleep(10)