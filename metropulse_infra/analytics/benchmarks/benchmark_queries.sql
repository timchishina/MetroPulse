
EXPLAIN (ANALYZE, BUFFERS)
SELECT
  dd.date AS dt,
  dd.hour AS hour,
  dr.route_number,
  COUNT(*) AS ride_cnt,
  AVG(fr.fare_amount) AS avg_fare,
  AVG(fr.ride_duration_sec) AS avg_duration_sec
FROM fact_ride fr
JOIN dim_datetime dd ON dd.time_sk = fr.start_time_sk
JOIN dim_route dr ON dr.route_sk = fr.route_sk
WHERE dd.date >= CURRENT_DATE - INTERVAL '14 days'
GROUP BY 1,2,3
ORDER BY dt, hour, route_number;

SELECT
  dt,
  hour,
  route_number,
  ride_cnt,
  avg_fare,
  avg_duration_sec
FROM metropulse.mart_route_hour
WHERE dt >= today() - 14
ORDER BY dt, hour, route_number;

EXPLAIN (ANALYZE, BUFFERS)
SELECT
  dd.date AS dt,
  dd.hour AS hour,
  dr.route_number,
  AVG(fvm.avg_speed_kmh) AS avg_speed_kmh,
  MAX(fvm.max_speed_kmh) AS max_speed_kmh,
  SUM(fvm.events_count) AS events
FROM fact_vehicle_movement fvm
JOIN dim_datetime dd ON dd.time_sk = fvm.time_sk
JOIN dim_route dr ON dr.route_sk = fvm.route_sk
WHERE dd.date >= CURRENT_DATE - INTERVAL '7 days'
  AND dd.hour IN (7,8,9,17,18,19)
GROUP BY 1,2,3
ORDER BY avg_speed_kmh DESC
LIMIT 20;

SELECT
  dt,
  hour,
  route_number,
  avg_speed_kmh,
  max_speed_kmh,
  movement_events_cnt AS events
FROM metropulse.mart_route_hour
WHERE dt >= today() - 7
  AND hour IN (7,8,9,17,18,19)
ORDER BY avg_speed_kmh DESC
LIMIT 20;

EXPLAIN (ANALYZE, BUFFERS)
SELECT
  dd.date AS dt,
  du.city,
  COUNT(*) FILTER (WHERE fp.status='success') AS payments_success_cnt,
  SUM(fp.amount) FILTER (WHERE fp.status='success') AS payments_success_amount,
  COUNT(*) FILTER (WHERE fp.status!='success') AS payments_failed_cnt
FROM fact_payment fp
JOIN dim_datetime dd ON dd.time_sk = fp.time_sk
JOIN dim_user du ON du.user_sk = fp.user_sk
WHERE dd.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1,2
ORDER BY dt, city;

SELECT
  dt,
  city,
  payments_success_cnt,
  payments_success_amount,
  payments_failed_cnt,
  if(payments_success_cnt + payments_failed_cnt = 0, 0,
     payments_success_cnt / (payments_success_cnt + payments_failed_cnt)) AS payment_success_rate
FROM metropulse.mart_city_day
WHERE dt >= today() - 30
ORDER BY dt, city;

EXPLAIN (ANALYZE, BUFFERS)
WITH rides_h AS (
  SELECT
    dd.date AS dt,
    dd.hour AS hour,
    dr.route_number,
    COUNT(*) AS ride_cnt
  FROM fact_ride fr
  JOIN dim_datetime dd ON dd.time_sk = fr.start_time_sk
  JOIN dim_route dr ON dr.route_sk = fr.route_sk
  WHERE dd.date >= CURRENT_DATE - INTERVAL '7 days'
  GROUP BY 1,2,3
),
spd_h AS (
  SELECT
    dd.date AS dt,
    dd.hour AS hour,
    dr.route_number,
    AVG(fvm.avg_speed_kmh) AS avg_speed_kmh
  FROM fact_vehicle_movement fvm
  JOIN dim_datetime dd ON dd.time_sk = fvm.time_sk
  JOIN dim_route dr ON dr.route_sk = fvm.route_sk
  WHERE dd.date >= CURRENT_DATE - INTERVAL '7 days'
  GROUP BY 1,2,3
)
SELECT
  r.dt,
  r.hour,
  r.route_number,
  r.ride_cnt,
  s.avg_speed_kmh
FROM rides_h r
JOIN spd_h s USING (dt, hour, route_number)
WHERE r.ride_cnt >= 20
ORDER BY r.ride_cnt DESC, s.avg_speed_kmh ASC
LIMIT 50;

SELECT
  dt,
  hour,
  route_number,
  ride_cnt,
  avg_speed_kmh
FROM metropulse.mart_route_hour
WHERE dt >= today() - 7
  AND ride_cnt >= 20
ORDER BY ride_cnt DESC, avg_speed_kmh ASC
LIMIT 50;

