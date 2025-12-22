"""
MetroPulse - Batch ETL (Task #3)
--------------------------------
Reads raw Parquet from MinIO (S3) and loads Core DWH (PostgreSQL):
- dim_datetime (Type 1, incremental insert)
- dim_user (Type 1, full refresh)
- dim_vehicle (Type 1, full refresh)
- dim_route (SCD Type 2, incremental)
- fact_ride (full refresh)
- fact_payment (full refresh)

How to run (example inside docker network):
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3 \
  etl_batch.py --run_ts "2025-12-22T12:00:00"

Note: for SCD2 demo, change routes.parquet (base_fare/vehicle_type/route_number) between runs
and re-run the job with a later --run_ts.
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_batch

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


FAR_FUTURE = "9999-12-31 23:59:59"
FAR_PAST = "1900-01-01 00:00:00"


def build_spark(app_name: str, s3_endpoint: str, s3_access_key: str, s3_secret_key: str) -> SparkSession:
    """
    Creates SparkSession with S3A (MinIO) configs.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        # S3A configs
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Better defaults for small demo
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark


def read_parquet(spark: SparkSession, s3a_path: str) -> DataFrame:
    return spark.read.parquet(s3a_path)


def jdbc_url(pg_host: str, pg_port: int, pg_db: str) -> str:
    return f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"


def read_table(spark: SparkSession, url: str, props: dict, table: str) -> DataFrame:
    return spark.read.jdbc(url=url, table=table, properties=props)


def write_overwrite(df: DataFrame, url: str, props: dict, table: str) -> None:
    # Truncate keeps schema & FKs; works on Postgres with .option("truncate", "true")
    (df.write
       .mode("overwrite")
       .option("truncate", "true")
       .jdbc(url=url, table=table, properties=props)
    )


def write_append(df: DataFrame, url: str, props: dict, table: str) -> None:
    (df.write
       .mode("append")
       .jdbc(url=url, table=table, properties=props)
    )


def clean_users(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("user_id").cast("int").alias("user_id"),
            F.col("name").cast("string").alias("name"),
            F.col("email").cast("string").alias("email"),
            F.col("city").cast("string").alias("city"),
            F.to_timestamp("created_at").alias("created_at"),
        )
        .dropna(subset=["user_id"])
        .dropDuplicates(["user_id"])
    )


def clean_routes(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("route_id").cast("int").alias("route_id"),
            F.col("route_number").cast("string").alias("route_number"),
            F.col("vehicle_type").cast("string").alias("vehicle_type"),
            F.col("base_fare").cast("decimal(10,2)").alias("base_fare"),
        )
        .dropna(subset=["route_id"])
        .dropDuplicates(["route_id"])
    )


def clean_vehicles(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("vehicle_id").cast("int").alias("vehicle_id"),
            F.col("route_id").cast("int").alias("route_id"),
            F.col("license_plate").cast("string").alias("license_plate"),
            F.col("capacity").cast("int").alias("capacity"),
        )
        .dropna(subset=["vehicle_id"])
        .dropDuplicates(["vehicle_id"])
    )


def clean_rides(df: DataFrame) -> DataFrame:
    # generator may already have ride_duration_sec
    with_dur = df
    if "ride_duration_sec" not in df.columns:
        with_dur = with_dur.withColumn(
            "ride_duration_sec",
            (F.col("end_time").cast("long") - F.col("start_time").cast("long"))
        )

    return (
        with_dur.select(
            F.col("ride_id").cast("string").alias("ride_id"),
            F.col("user_id").cast("int").alias("user_id"),
            F.col("route_id").cast("int").alias("route_id"),
            F.col("vehicle_id").cast("int").alias("vehicle_id"),
            F.to_timestamp("start_time").alias("start_time"),
            F.to_timestamp("end_time").alias("end_time"),
            F.col("fare_amount").cast("decimal(10,2)").alias("fare_amount"),
            F.col("ride_duration_sec").cast("int").alias("ride_duration_sec"),
        )
        .dropna(subset=["ride_id", "user_id", "route_id", "start_time"])
        .dropDuplicates(["ride_id"])
    )


def clean_payments(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("payment_id").cast("string").alias("payment_id"),
            F.col("ride_id").cast("string").alias("ride_id"),
            F.col("user_id").cast("int").alias("user_id"),
            F.col("amount").cast("decimal(10,2)").alias("amount"),
            F.col("payment_method").cast("string").alias("payment_method"),
            F.col("status").cast("string").alias("status"),
            F.to_timestamp("created_at").alias("created_at"),
        )
        .dropna(subset=["payment_id", "user_id", "created_at"])
        .dropDuplicates(["payment_id"])
    )


def build_dim_datetime(spark: SparkSession, users: DataFrame, rides: DataFrame, payments: DataFrame) -> DataFrame:
    """
    Build dim_datetime rows from all timestamps we know about.
    (We keep only distinct timestamps down to second-level granularity.)
    """
    ts_df = (
        users.select(F.col("created_at").alias("ts"))
        .unionByName(rides.select(F.col("start_time").alias("ts")))
        .unionByName(rides.select(F.col("end_time").alias("ts")))
        .unionByName(payments.select(F.col("created_at").alias("ts")))
        .where(F.col("ts").isNotNull())
        .withColumn("ts", F.date_trunc("second", F.col("ts")))
        .dropDuplicates(["ts"])
    )

    # Spark: dayofweek -> 1=Sunday..7=Saturday
    dim = (
        ts_df
        .withColumn("date", F.to_date("ts"))
        .withColumn("year", F.year("ts"))
        .withColumn("month", F.month("ts"))
        .withColumn("day", F.dayofmonth("ts"))
        .withColumn("hour", F.hour("ts"))
        .withColumn("minute", F.minute("ts"))
        .withColumn("day_of_week", F.dayofweek("ts"))
        .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
        .select("ts", "date", "year", "month", "day", "hour", "minute", "day_of_week", "is_weekend")
    )
    return dim


def ensure_dim_datetime(
    spark: SparkSession,
    dim_datetime_new: DataFrame,
    pg_url: str,
    pg_props: dict,
) -> None:
    """
    Insert only missing timestamps (because dim_datetime has UNIQUE(ts)).
    """
    try:
        existing = read_table(spark, pg_url, pg_props, "dim_datetime").select("ts")
        to_insert = dim_datetime_new.join(existing, on="ts", how="left_anti")
    except Exception:
        # if table is empty/unavailable, insert all
        to_insert = dim_datetime_new

    if to_insert.rdd.isEmpty():
        print("dim_datetime: no new timestamps to insert")
        return

    write_append(to_insert, pg_url, pg_props, "dim_datetime")
    print(f"dim_datetime: inserted {to_insert.count()} rows")


def scd2_apply_dim_route(
    routes_in: DataFrame,
    pg_conn_dsn: str,
    pg_url: str,
    pg_props: dict,
    spark: SparkSession,
    run_ts: datetime,
) -> None:
    """
    Apply SCD Type 2 on dim_route:
    - Natural key: route_id_nk
    - Compare attributes: route_number, vehicle_type, base_fare
    Rules:
      * If route is new -> insert (valid_from=FAR_PAST, valid_to=FAR_FUTURE, is_current=true)
      * If route changed vs current -> close current (valid_to=run_ts, is_current=false) + insert new (valid_from=run_ts)
      * If unchanged -> do nothing
    """
    incoming = routes_in.select(
        F.col("route_id").alias("route_id_nk"),
        "route_number",
        "vehicle_type",
        "base_fare"
    ).dropDuplicates(["route_id_nk"])

    # Load only current routes from DB into Spark
    try:
        current = (
            read_table(spark, pg_url, pg_props, "dim_route")
            .where(F.col("is_current") == True)
            .select(
                F.col("route_sk"),
                F.col("route_id_nk"),
                F.col("route_number").alias("cur_route_number"),
                F.col("vehicle_type").alias("cur_vehicle_type"),
                F.col("base_fare").alias("cur_base_fare"),
            )
        )
    except Exception:
        current = spark.createDataFrame([], schema=T.StructType([
            T.StructField("route_sk", T.LongType(), True),
            T.StructField("route_id_nk", T.IntegerType(), True),
            T.StructField("cur_route_number", T.StringType(), True),
            T.StructField("cur_vehicle_type", T.StringType(), True),
            T.StructField("cur_base_fare", T.DecimalType(10,2), True),
        ]))

    joined = incoming.join(current, on="route_id_nk", how="left")

    # Identify new routes (no current record)
    new_routes = joined.where(F.col("route_sk").isNull()).select(
        "route_id_nk", "route_number", "vehicle_type", "base_fare"
    )

    # Identify changed routes
    changed = joined.where(
        F.col("route_sk").isNotNull() &
        (
            (F.col("route_number") != F.col("cur_route_number")) |
            (F.col("vehicle_type") != F.col("cur_vehicle_type")) |
            (F.col("base_fare") != F.col("cur_base_fare"))
        )
    ).select(
        F.col("route_sk").alias("cur_route_sk"),
        "route_id_nk", "route_number", "vehicle_type", "base_fare"
    )

    new_count = new_routes.count()
    chg_count = changed.count()
    print(f"dim_route SCD2: new={new_count}, changed={chg_count}")

    if new_count == 0 and chg_count == 0:
        return

    # Collect small changed/new sets to driver for psycopg2 (ok for demo масштаба)
    new_rows = [r.asDict() for r in new_routes.collect()]
    chg_rows = [r.asDict() for r in changed.collect()]

    run_ts_str = run_ts.strftime("%Y-%m-%d %H:%M:%S")

    with psycopg2.connect(pg_conn_dsn) as conn:
        with conn.cursor() as cur:
            # Close old versions for changed routes
            if chg_rows:
                execute_batch(
                    cur,
                    """
                    UPDATE dim_route
                       SET valid_to = %(valid_to)s,
                           is_current = FALSE
                     WHERE route_sk = %(cur_route_sk)s
                       AND is_current = TRUE
                    """,
                    [
                        {"valid_to": run_ts_str, "cur_route_sk": row["cur_route_sk"]}
                        for row in chg_rows
                    ],
                    page_size=500,
                )

            # Insert new routes (initial history from FAR_PAST)
            if new_rows:
                execute_batch(
                    cur,
                    """
                    INSERT INTO dim_route (route_id_nk, route_number, vehicle_type, base_fare, valid_from, valid_to, is_current)
                    VALUES (%(route_id_nk)s, %(route_number)s, %(vehicle_type)s, %(base_fare)s, %(valid_from)s, %(valid_to)s, TRUE)
                    """,
                    [
                        {
                            "route_id_nk": row["route_id_nk"],
                            "route_number": row["route_number"],
                            "vehicle_type": row["vehicle_type"],
                            "base_fare": float(row["base_fare"]) if row["base_fare"] is not None else None,
                            "valid_from": FAR_PAST,
                            "valid_to": FAR_FUTURE,
                        }
                        for row in new_rows
                    ],
                    page_size=500,
                )

            # Insert new versions for changed routes
            if chg_rows:
                execute_batch(
                    cur,
                    """
                    INSERT INTO dim_route (route_id_nk, route_number, vehicle_type, base_fare, valid_from, valid_to, is_current)
                    VALUES (%(route_id_nk)s, %(route_number)s, %(vehicle_type)s, %(base_fare)s, %(valid_from)s, %(valid_to)s, TRUE)
                    """,
                    [
                        {
                            "route_id_nk": row["route_id_nk"],
                            "route_number": row["route_number"],
                            "vehicle_type": row["vehicle_type"],
                            "base_fare": float(row["base_fare"]) if row["base_fare"] is not None else None,
                            "valid_from": run_ts_str,
                            "valid_to": FAR_FUTURE,
                        }
                        for row in chg_rows
                    ],
                    page_size=500,
                )

        conn.commit()


def build_fact_ride(
    rides: DataFrame,
    dim_user: DataFrame,
    dim_vehicle: DataFrame,
    dim_route: DataFrame,
    dim_datetime: DataFrame,
) -> DataFrame:
    """
    Maps natural keys to surrogate keys and creates fact_ride rows.
    SCD2 join on route_id_nk with validity range by start_time.
    """
    r = rides.alias("r")
    u = dim_user.select("user_sk", F.col("user_id_nk").alias("user_id")).alias("u")
    v = dim_vehicle.select("vehicle_sk", F.col("vehicle_id_nk").alias("vehicle_id")).alias("v")
    dt = dim_datetime.select("time_sk", "ts").alias("dt")
    dr = dim_route.select(
        "route_sk",
        "route_id_nk",
        F.col("valid_from").cast("timestamp").alias("valid_from"),
        F.col("valid_to").cast("timestamp").alias("valid_to"),
    ).alias("dr")

    # datetime keys
    rides_dt = (
        r.join(dt, F.date_trunc("second", F.col("r.start_time")) == F.col("dt.ts"), "left")
         .withColumnRenamed("time_sk", "start_time_sk")
         .drop("ts")
         .join(dt, F.date_trunc("second", F.col("r.end_time")) == F.col("dt.ts"), "left")
         .withColumnRenamed("time_sk", "end_time_sk")
         .drop("ts")
    )

    # SCD2 route join
    rides_route = rides_dt.join(
        dr,
        (F.col("r.route_id") == F.col("dr.route_id_nk")) &
        (F.col("r.start_time") >= F.col("dr.valid_from")) &
        (F.col("r.start_time") < F.col("dr.valid_to")),
        "left"
    )

    out = (
        rides_route
        .join(u, on=F.col("r.user_id") == F.col("u.user_id"), how="left")
        .join(v, on=F.col("r.vehicle_id") == F.col("v.vehicle_id"), how="left")
        .select(
            F.col("r.ride_id").cast("string").alias("ride_id_nk"),
            F.col("u.user_sk").cast("long").alias("user_sk"),
            F.col("dr.route_sk").cast("long").alias("route_sk"),
            F.col("v.vehicle_sk").cast("long").alias("vehicle_sk"),
            F.col("start_time_sk").cast("long").alias("start_time_sk"),
            F.col("end_time_sk").cast("long").alias("end_time_sk"),
            F.col("r.fare_amount").alias("fare_amount"),
            F.col("r.ride_duration_sec").cast("int").alias("ride_duration_sec"),
        )
    )

    # Basic referential sanity: drop rows that cannot be linked
    out = out.dropna(subset=["ride_id_nk", "user_sk", "route_sk", "start_time_sk"]).dropDuplicates(["ride_id_nk"])
    return out


def build_fact_payment(
    payments: DataFrame,
    dim_user: DataFrame,
    dim_datetime: DataFrame,
) -> DataFrame:
    p = payments.alias("p")
    u = dim_user.select("user_sk", F.col("user_id_nk").alias("user_id")).alias("u")
    dt = dim_datetime.select("time_sk", "ts").alias("dt")

    out = (
        p.join(u, on=F.col("p.user_id") == F.col("u.user_id"), how="left")
         .join(dt, F.date_trunc("second", F.col("p.created_at")) == F.col("dt.ts"), "left")
         .select(
             F.col("p.payment_id").cast("string").alias("payment_id_nk"),
             F.col("u.user_sk").cast("long").alias("user_sk"),
             F.col("p.ride_id").cast("string").alias("ride_id_nk"),
             F.col("dt.time_sk").cast("long").alias("time_sk"),
             F.col("p.amount").alias("amount"),
             F.col("p.payment_method").alias("payment_method"),
             F.col("p.status").alias("status"),
         )
    )

    out = out.dropna(subset=["payment_id_nk", "user_sk", "time_sk"]).dropDuplicates(["payment_id_nk"])
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_ts", default=None, help="Batch run timestamp, ISO8601 (e.g. 2025-12-22T12:00:00). Defaults to now().")
    args = parser.parse_args()

    run_ts = datetime.fromisoformat(args.run_ts) if args.run_ts else datetime.utcnow()

    # S3 (MinIO) config
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")  # inside docker network
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
    s3_bucket = os.getenv("S3_BUCKET", "raw-data")

    # Postgres config
    pg_host = os.getenv("PG_HOST", "postgres")  # inside docker network
    pg_port = int(os.getenv("PG_PORT", "5432"))
    pg_db = os.getenv("PG_DB", "metropulse_dwh")
    pg_user = os.getenv("PG_USER", "user")
    pg_password = os.getenv("PG_PASSWORD", "password")

    pg_url = jdbc_url(pg_host, pg_port, pg_db)
    pg_props = {"user": pg_user, "password": pg_password, "driver": "org.postgresql.Driver"}
    pg_conn_dsn = f"host={pg_host} port={pg_port} dbname={pg_db} user={pg_user} password={pg_password}"

    spark = build_spark("metropulse-batch-etl", s3_endpoint, s3_access_key, s3_secret_key)

    # --------- Extract (MinIO) ---------
    users_raw = read_parquet(spark, f"s3a://{s3_bucket}/users/users.parquet")
    routes_raw = read_parquet(spark, f"s3a://{s3_bucket}/routes/routes.parquet")
    vehicles_raw = read_parquet(spark, f"s3a://{s3_bucket}/vehicles/vehicles.parquet")
    rides_raw = read_parquet(spark, f"s3a://{s3_bucket}/rides/data.parquet")
    payments_raw = read_parquet(spark, f"s3a://{s3_bucket}/payments/payments.parquet")

    # --------- Transform/Clean ---------
    users = clean_users(users_raw)
    routes = clean_routes(routes_raw)
    vehicles = clean_vehicles(vehicles_raw)
    rides = clean_rides(rides_raw)
    payments = clean_payments(payments_raw)

    # --------- Load dimensions ---------
    # dim_datetime incremental insert
    dim_dt_new = build_dim_datetime(spark, users, rides, payments)
    ensure_dim_datetime(spark, dim_dt_new, pg_url, pg_props)

    # dim_user full refresh
    dim_user_out = (
        users.select(
            F.col("user_id").alias("user_id_nk"),
            "name", "email", "city", "created_at"
        )
    )
    write_overwrite(dim_user_out, pg_url, pg_props, "dim_user")
    print(f"dim_user: loaded {dim_user_out.count()} rows (full refresh)")

    # dim_vehicle full refresh
    dim_vehicle_out = (
        vehicles.select(
            F.col("vehicle_id").alias("vehicle_id_nk"),
            F.col("route_id").alias("route_id_nk"),
            "license_plate", "capacity"
        )
    )
    write_overwrite(dim_vehicle_out, pg_url, pg_props, "dim_vehicle")
    print(f"dim_vehicle: loaded {dim_vehicle_out.count()} rows (full refresh)")

    # dim_route SCD2 incremental
    scd2_apply_dim_route(routes, pg_conn_dsn, pg_url, pg_props, spark, run_ts)

    # --------- Reload dims for key mapping ---------
    dim_user_db = read_table(spark, pg_url, pg_props, "dim_user")
    dim_vehicle_db = read_table(spark, pg_url, pg_props, "dim_vehicle")
    dim_route_db = read_table(spark, pg_url, pg_props, "dim_route")
    dim_datetime_db = read_table(spark, pg_url, pg_props, "dim_datetime")

    # --------- Facts ---------
    fact_ride = build_fact_ride(rides, dim_user_db, dim_vehicle_db, dim_route_db, dim_datetime_db)
    write_overwrite(fact_ride, pg_url, pg_props, "fact_ride")
    print(f"fact_ride: loaded {fact_ride.count()} rows (full refresh)")

    fact_payment = build_fact_payment(payments, dim_user_db, dim_datetime_db)
    write_overwrite(fact_payment, pg_url, pg_props, "fact_payment")
    print(f"fact_payment: loaded {fact_payment.count()} rows (full refresh)")

    spark.stop()
    print("ETL finished OK.")


if __name__ == "__main__":
    main()
