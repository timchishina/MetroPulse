-- Время/дата
CREATE TABLE IF NOT EXISTS dim_datetime (
    time_sk     BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMP NOT NULL,
    date        DATE NOT NULL,
    year        INT,
    month       INT,
    day         INT,
    hour        INT,
    minute      INT,
    day_of_week INT,
    is_weekend  BOOLEAN
);

CREATE UNIQUE INDEX ux_dim_datetime_ts ON dim_datetime(ts);


-- Пользователи
CREATE TABLE IF NOT EXISTS dim_user (
    user_sk       BIGSERIAL PRIMARY KEY,
    user_id_nk    INT NOT NULL,
    name          VARCHAR,
    email         VARCHAR,
    city          VARCHAR,
    created_at    TIMESTAMP
);

CREATE UNIQUE INDEX ux_dim_user_nk ON dim_user(user_id_nk);


-- Маршруты (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_route (
    route_sk      BIGSERIAL PRIMARY KEY,
    route_id_nk   INT NOT NULL,
    route_number  VARCHAR,
    vehicle_type  VARCHAR,
    base_fare     NUMERIC(10,2),
    valid_from    TIMESTAMP NOT NULL,
    valid_to      TIMESTAMP NOT NULL,
    is_current    BOOLEAN NOT NULL
);

CREATE INDEX ix_dim_route_nk ON dim_route(route_id_nk);
CREATE INDEX ix_dim_route_current ON dim_route(route_id_nk, is_current);


-- Транспортные средства
CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_sk      BIGSERIAL PRIMARY KEY,
    vehicle_id_nk   INT NOT NULL,
    route_id_nk     INT,
    license_plate   VARCHAR,
    capacity        INT
);

CREATE UNIQUE INDEX ux_dim_vehicle_nk ON dim_vehicle(vehicle_id_nk);