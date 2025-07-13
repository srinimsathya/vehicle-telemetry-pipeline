from kafka import KafkaConsumer
import json
import os

consumer = KafkaConsumer(
    'vehicle-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='vehicle-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    data = message.value
    print("Received:", data)

    folder_path = 'data'
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, 'vehicle_data_current.json')

    with open(file_path, 'a') as f:
        f.write(json.dumps(data) + '\n')
