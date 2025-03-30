import json
import random
import time
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "localhost:9092"  # Use the correct broker address
KAFKA_TOPIC = "topic1"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize the JSON messages
)

# Simulate sending messages with the required structure
def generate_message():
    return {
        "id": random.randint(1, 1000),
        "name": f"User{random.randint(1, 100)}",
        "email": f"user{random.randint(1, 100)}@example.com"
    }

# Send simulated messages to Kafka every 1 second
try:
    while True:
        message = generate_message()
        print(f"Sending message: {message}")
        producer.send(KAFKA_TOPIC, value=message)
        time.sleep(1)  # Simulate a delay of 1 second between messages
except KeyboardInterrupt:
    print("Terminating producer.")
finally:
    producer.close()
