import pandas as pd
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
from minio import Minio
import io
import os

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "mp_minio:9000")
ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "raw-data"

print(f"Connecting to MinIO at: {MINIO_ENDPOINT}...")

client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

fake = Faker('ru_RU')

def upload_df_to_minio(df, object_name):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype('datetime64[us]')

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    client.put_object(
        BUCKET_NAME,
        object_name,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes
    )
    print(f"Uploaded {object_name} (Rows: {len(df)})")

def generate_data():
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    print("--- Start data generation for new schema ---")

    # Users
    # Fields: user_id, name, email, city, created_at
    print("Generating Users...")
    users = []
    cities = ['Москва', 'Санкт-Петербург', 'Казань', 'Екатеринбург', 'Новосибирск']
    for i in range(1, 201): # 200 users
        users.append({
            "user_id": i,
            "name": fake.name(),
            "email": fake.email(),
            "city": random.choice(cities),
            "created_at": fake.date_time_between(start_date='-2y', end_date='now')
        })
    df_users = pd.DataFrame(users)
    upload_df_to_minio(df_users, "users/users.parquet")

    # Routes
    # Fields: route_id, route_number, vehicle_type, base_fare
    print("Generating Routes...")
    routes_data = [
        {"route_id": 1, "route_number": "105", "vehicle_type": "bus", "base_fare": 50},
        {"route_id": 2, "route_number": "17", "vehicle_type": "tram", "base_fare": 45},
        {"route_id": 3, "route_number": "А-72", "vehicle_type": "metro", "base_fare": 60},
        {"route_id": 4, "route_number": "404", "vehicle_type": "bus", "base_fare": 55},
    ]
    df_routes = pd.DataFrame(routes_data)
    upload_df_to_minio(df_routes, "routes/routes.parquet")

    # Vehicles
    # Fields: vehicle_id, route_id, license_plate, capacity
    print("Generating Vehicles...")
    vehicles = []
    vehicle_id_counter = 100
    route_vehicle_map = {}

    for r in routes_data:
        route_vehicle_map[r['route_id']] = []
        for _ in range(5):
            vehicle_id_counter += 1
            v_id = vehicle_id_counter
            vehicles.append({
                "vehicle_id": v_id,
                "route_id": r['route_id'],
                "license_plate": fake.license_plate(),
                "capacity": random.randint(40, 120)
            })
            route_vehicle_map[r['route_id']].append(v_id)

    df_vehicles = pd.DataFrame(vehicles)
    upload_df_to_minio(df_vehicles, "vehicles/vehicles.parquet")

    # Rides
    # Fields: ride_id (uuid), user_id, route_id, vehicle_id, start_time, end_time, fare_amount
    print("Generating Rides & Payments...")
    rides = []
    payments = []

    start_date = datetime.now() - timedelta(days=30)

    for _ in range(2000):
        # Base data
        trip_start = start_date + timedelta(minutes=random.randint(0, 43200))
        duration = random.randint(5, 90)
        trip_end = trip_start + timedelta(minutes=duration)

        user = random.choice(users)
        route = random.choice(routes_data)
        vehicle_id = random.choice(route_vehicle_map[route['route_id']])

        ride_uuid = str(uuid.uuid4())

        rides.append({
            "ride_id": ride_uuid,
            "user_id": user['user_id'],
            "route_id": route['route_id'],
            "vehicle_id": vehicle_id,
            "start_time": trip_start,
            "end_time": trip_end,
            "fare_amount": route['base_fare'],
            "ride_duration_sec": duration * 60
        })

        # Payments
        # Fields: payment_id (uuid), ride_id, user_id, amount, method, status, time
        if random.random() > 0.05: # 95% successful payments
            status = "success"
        else:
            status = "failed"

        payments.append({
            "payment_id": str(uuid.uuid4()),
            "ride_id": ride_uuid,
            "user_id": user['user_id'],
            "amount": route['base_fare'],
            "payment_method": random.choice(["card", "sbp", "wallet"]),
            "status": status,
            "created_at": trip_end + timedelta(seconds=random.randint(1, 60))
        })

    df_rides = pd.DataFrame(rides)
    # Partition by date for beauty, like real Data Lake
    upload_df_to_minio(df_rides, f"rides/data.parquet")

    df_payments = pd.DataFrame(payments)
    upload_df_to_minio(df_payments, "payments/payments.parquet")

    print("!!! Generation Complete !!!")

if __name__ == "__main__":
    generate_data()
