-- USERS
CREATE TABLE IF NOT EXISTS stg_users (
    user_id     INT,
    name        VARCHAR,
    email       VARCHAR,
    created_at  TIMESTAMP,
    city        VARCHAR
);

-- ROUTES
CREATE TABLE IF NOT EXISTS stg_routes (
    route_id        INT,
    route_number    VARCHAR,
    vehicle_type    VARCHAR,
    base_fare       NUMERIC(10,2)
);

-- VEHICLES
CREATE TABLE IF NOT EXISTS stg_vehicles (
    vehicle_id      INT,
    route_id        INT,
    license_plate   VARCHAR,
    capacity        INT
);

-- RIDES
CREATE TABLE IF NOT EXISTS stg_rides (
    ride_id         UUID,
    user_id         INT,
    route_id        INT,
    vehicle_id      INT,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    fare_amount     NUMERIC(10,2)
);

-- PAYMENTS
CREATE TABLE IF NOT EXISTS stg_payments (
    payment_id      UUID,
    ride_id         UUID,
    user_id         INT,
    amount          NUMERIC(10,2),
    payment_method  VARCHAR,
    status          VARCHAR,
    created_at      TIMESTAMP
);

-- vehicle_positions из Kafka (сырые события)
CREATE TABLE IF NOT EXISTS stg_vehicle_positions (
    event_id            UUID,
    vehicle_id          INT,
    route_number        VARCHAR,
    event_time          TIMESTAMP,
    latitude            NUMERIC(9,6),
    longitude           NUMERIC(9,6),
    speed_kmh           NUMERIC(6,2),
    passengers_estimated INT
);