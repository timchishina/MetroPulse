-- Факт поездки
CREATE TABLE IF NOT EXISTS fact_ride (
    ride_id_nk          UUID PRIMARY KEY,   -- натуральный ключ из OLTP
    user_sk             BIGINT NOT NULL,
    route_sk            BIGINT NOT NULL,
    vehicle_sk          BIGINT,
    start_time_sk       BIGINT NOT NULL,
    end_time_sk         BIGINT,
    fare_amount         NUMERIC(10,2),
    ride_duration_sec   INT,
    load_dt             TIMESTAMP DEFAULT NOW()
);

ALTER TABLE fact_ride
  ADD CONSTRAINT fk_fact_ride_user
  FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk);

ALTER TABLE fact_ride
  ADD CONSTRAINT fk_fact_ride_route
  FOREIGN KEY (route_sk) REFERENCES dim_route(route_sk);

ALTER TABLE fact_ride
  ADD CONSTRAINT fk_fact_ride_vehicle
  FOREIGN KEY (vehicle_sk) REFERENCES dim_vehicle(vehicle_sk);

ALTER TABLE fact_ride
  ADD CONSTRAINT fk_fact_ride_start_time
  FOREIGN KEY (start_time_sk) REFERENCES dim_datetime(time_sk);

ALTER TABLE fact_ride
  ADD CONSTRAINT fk_fact_ride_end_time
  FOREIGN KEY (end_time_sk) REFERENCES dim_datetime(time_sk);


-- Факт платежа
CREATE TABLE IF NOT EXISTS fact_payment (
    payment_id_nk   UUID PRIMARY KEY,
    user_sk         BIGINT NOT NULL,
    ride_id_nk      UUID,          -- связь с fact_ride как дегереративный ключ
    time_sk         BIGINT NOT NULL,
    amount          NUMERIC(10,2),
    payment_method  VARCHAR,
    status          VARCHAR,
    load_dt         TIMESTAMP DEFAULT NOW()
);

ALTER TABLE fact_payment
  ADD CONSTRAINT fk_fact_payment_user
  FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk);

ALTER TABLE fact_payment
  ADD CONSTRAINT fk_fact_payment_time
  FOREIGN KEY (time_sk) REFERENCES dim_datetime(time_sk);


-- Факт движения ТС (агрегаты по окну)
CREATE TABLE IF NOT EXISTS fact_vehicle_movement (
    id                      BIGSERIAL PRIMARY KEY,
    vehicle_sk              BIGINT NOT NULL,
    route_sk                BIGINT NOT NULL,
    time_sk                 BIGINT NOT NULL,   -- начало агрегатного окна
    avg_speed_kmh           NUMERIC(6,2),
    max_speed_kmh           NUMERIC(6,2),
    avg_passengers_estimated NUMERIC(10,2),
    events_count            INT,
    load_dt                 TIMESTAMP DEFAULT NOW()
);

ALTER TABLE fact_vehicle_movement
  ADD CONSTRAINT fk_fvm_vehicle
  FOREIGN KEY (vehicle_sk) REFERENCES dim_vehicle(vehicle_sk);

ALTER TABLE fact_vehicle_movement
  ADD CONSTRAINT fk_fvm_route
  FOREIGN KEY (route_sk) REFERENCES dim_route(route_sk);

ALTER TABLE fact_vehicle_movement
  ADD CONSTRAINT fk_fvm_time
  FOREIGN KEY (time_sk) REFERENCES dim_datetime(time_sk);