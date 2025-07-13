from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_vehicle_data():
    return {
        "vehicle_id": random.choice(["V101", "V102", "V103"]),
        "speed": random.randint(20, 120),
        "engine_temp": round(random.uniform(70, 100), 2),
        "latitude": round(random.uniform(12.90, 13.10), 6),
        "longitude": round(random.uniform(77.50, 77.70), 6),
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }

try:
    while True:
        data = generate_vehicle_data()
        producer.send('vehicle-topic', value=data)
        print(f"Sent: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Stopped producing messages.")
