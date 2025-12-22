CREATE DATABASE IF NOT EXISTS metropulse;

CREATE TABLE IF NOT EXISTS metropulse.mart_route_hour (
    dt Date,
    hour UInt8,
    route_number String,

    ride_cnt UInt64,
    avg_fare Float64,
    avg_duration_sec Float64,

    avg_speed_kmh Float64,
    max_speed_kmh Float64,
    avg_passengers_estimated Float64,
    movement_events_cnt UInt64,

    load_ts DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, hour, route_number)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS metropulse.mart_city_day (
    dt Date,
    city String,

    rides_cnt UInt64,
    users_distinct UInt64,
    rides_revenue_nominal Float64,

    payments_success_cnt UInt64,
    payments_success_amount Float64,
    payments_failed_cnt UInt64,

    load_ts DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, city)
SETTINGS index_granularity = 8192;


CREATE VIEW IF NOT EXISTS metropulse.v_route_hour AS
SELECT
  dt,
  hour,
  route_number,
  ride_cnt,
  avg_fare,
  avg_duration_sec,
  avg_speed_kmh,
  max_speed_kmh,
  avg_passengers_estimated,
  movement_events_cnt,
  load_ts
FROM metropulse.mart_route_hour;
