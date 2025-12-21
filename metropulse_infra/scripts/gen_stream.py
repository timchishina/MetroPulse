import json
import time
import random
import uuid
from kafka import KafkaProducer
from datetime import datetime

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'vehicle_positions'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


active_vehicles = [
    # Buses on route 1 (ID 1001-1005)
    {"vehicle_id": 1001, "route_number": "105", "lat": 55.75, "lon": 37.61, "speed": 40},
    {"vehicle_id": 1002, "route_number": "105", "lat": 55.76, "lon": 37.62, "speed": 35},

    # Tram on route 2 (ID 1006-1010)
    {"vehicle_id": 1006, "route_number": "17", "lat": 55.80, "lon": 37.50, "speed": 20},

    # Metro on route 3 (ID 1011...)
    {"vehicle_id": 1011, "route_number": "404", "lat": 55.70, "lon": 37.40, "speed": 60},
]

print(f"Starting stream generator to topic '{TOPIC_NAME}'...")
print("Press Ctrl+C to stop.")

try:
    while True:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for v in active_vehicles:
            # Simulate coordinate change (movement)
            v["lat"] += random.uniform(-0.0005, 0.0005)
            v["lon"] += random.uniform(-0.0005, 0.0005)

            # Change speed
            v["speed"] = max(0, min(80, v["speed"] + random.randint(-5, 5)))

            event = {
                "event_id": str(uuid.uuid4()),
                "vehicle_id": v["vehicle_id"],
                "route_number": v["route_number"],
                "event_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "latitude": round(v["lat"], 6),
                "longitude": round(v["lon"], 6),
                "speed_kmh": v["speed"],
                "passengers_estimated": random.randint(5, 50)
            }

            producer.send(TOPIC_NAME, value=event)
            print(f"Sent: {event}")

        producer.flush()
        time.sleep(5) 

except KeyboardInterrupt:
    print("Stopping generator...")
    producer.close()
