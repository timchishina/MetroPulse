from __future__ import annotations

import argparse
import os
import sys
import gc
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_batch

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

def build_spark(app_name):
    print(f">> Initializing Spark: {app_name}")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.extraJavaOptions", "-Djava.security.egd=file:/dev/./urandom")
        .config("spark.executor.extraJavaOptions", "-Djava.security.egd=file:/dev/./urandom")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .getOrCreate()
    )

def cleanup(spark, df=None):
    if df: df.unpersist()
    spark.catalog.clearCache()
    gc.collect()

def jdbc_url(pg_host, pg_port, pg_db):
    return f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}?sslmode=disable"

def write_append(df, url, props, table):
    print(f"   -> [DB WRITE] Appending to: {table}")
    df.write.mode("append").jdbc(url=url, table=table, properties=props)

def reset_warehouse(pg_dsn):
    """
    Cleans tables (Fact -> Dim) using CASCADE to handle Foreign Keys.
    """
    print("\n>> [INIT] Cleaning Warehouse (Truncating Tables)...")
    try:
        with psycopg2.connect(pg_dsn + " sslmode=disable") as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    TRUNCATE TABLE fact_ride, fact_payment CASCADE;
                    TRUNCATE TABLE dim_user, dim_vehicle CASCADE;
                """)
        print("   -> Warehouse cleaned successfully.")
    except Exception as e:
        print(f"   !!! Warning during cleanup: {e}") 

def clean_users(df):
    return df.select(
        F.col("user_id").cast("int"),
        F.col("name").cast("string"),
        F.col("email").cast("string"),
        F.col("city").cast("string"),
        F.to_timestamp("created_at").alias("created_at")
    ).dropna(subset=["user_id"]).dropDuplicates(["user_id"])

def clean_routes(df):
    return df.select(
        F.col("route_id").cast("int"),
        F.col("route_number").cast("string"),
        F.col("vehicle_type").cast("string"),
        F.col("base_fare").cast("decimal(10,2)")
    ).dropna(subset=["route_id"]).dropDuplicates(["route_id"])

def clean_vehicles(df):
    return df.select(
        F.col("vehicle_id").cast("int"),
        F.col("route_id").cast("int"),
        F.col("license_plate").cast("string"),
        F.col("capacity").cast("int")
    ).dropna(subset=["vehicle_id"]).dropDuplicates(["vehicle_id"])

def clean_rides(df):
    if "ride_duration_sec" not in df.columns:
        df = df.withColumn("ride_duration_sec", (F.col("end_time").cast("long") - F.col("start_time").cast("long")))
    return df.select(
        F.col("ride_id").cast("string"),
        F.col("user_id").cast("int"),
        F.col("route_id").cast("int"),
        F.col("vehicle_id").cast("int"),
        F.to_timestamp("start_time").alias("start_time"),
        F.to_timestamp("end_time").alias("end_time"),
        F.col("fare_amount").cast("decimal(10,2)"),
        F.col("ride_duration_sec").cast("int")
    ).dropna(subset=["ride_id", "user_id", "route_id", "start_time"]).dropDuplicates(["ride_id"])

def clean_payments(df):
    return df.select(
        F.col("payment_id").cast("string"),
        F.col("ride_id").cast("string"),
        F.col("user_id").cast("int"),
        F.col("amount").cast("decimal(10,2)"),
        F.col("payment_method").cast("string"),
        F.col("status").cast("string"),
        F.to_timestamp("created_at").alias("created_at")
    ).dropna(subset=["payment_id", "user_id", "created_at"]).dropDuplicates(["payment_id"])

def build_dim_datetime(users, rides, payments):
    print("   -> Logic: Union timestamps...")
    ts_df = (
        users.select(F.col("created_at").alias("ts"))
        .union(rides.select(F.col("start_time").alias("ts")))
        .union(rides.select(F.col("end_time").alias("ts")))
        .union(payments.select(F.col("created_at").alias("ts")))
        .where(F.col("ts").isNotNull())
        .withColumn("ts", F.date_trunc("second", F.col("ts")))
        .dropDuplicates(["ts"])
    )
    return ts_df.select(
        "ts", F.to_date("ts").alias("date"), F.year("ts").alias("year"),
        F.month("ts").alias("month"), F.dayofmonth("ts").alias("day"),
        F.hour("ts").alias("hour"), F.minute("ts").alias("minute"),
        F.dayofweek("ts").alias("day_of_week"),
        F.col("day_of_week").isin([1, 7]).alias("is_weekend")
    )

def ensure_dim_datetime(spark, dim_datetime_new, pg_url, props):
    try:
        existing = spark.read.jdbc(url=pg_url, table="dim_datetime", properties=props).select("ts")
        new_rows = dim_datetime_new.join(existing, on="ts", how="left_anti")
    except:
        new_rows = dim_datetime_new 

    if new_rows.limit(1).count() > 0:
        write_append(new_rows, pg_url, props, "dim_datetime")
    else:
        print("   -> dim_datetime: No new rows.")

def scd2_apply_dim_route(routes, pg_dsn, pg_url, props, spark, run_ts):
    print("   -> Logic: SCD2 Routes...")
    routes_in = routes.select(F.col("route_id").alias("route_id_nk"), "route_number", "vehicle_type", "base_fare").dropDuplicates(["route_id_nk"])
    
    try:
        current = spark.read.jdbc(url=pg_url, table="dim_route", properties=props).where("is_current = true") \
            .select("route_sk", "route_id_nk", F.col("route_number").alias("c_num"), F.col("vehicle_type").alias("c_type"), F.col("base_fare").alias("c_fare"))
    except:
        current = spark.createDataFrame([], schema="route_sk long, route_id_nk int, c_num string, c_type string, c_fare decimal(10,2)")

    joined = routes_in.join(current, on="route_id_nk", how="left")
    new_recs = joined.where(F.col("route_sk").isNull())
    changed_recs = joined.where(F.col("route_sk").isNotNull() & (
        (F.col("route_number") != F.col("c_num")) | (F.col("vehicle_type") != F.col("c_type")) | (F.col("base_fare") != F.col("c_fare"))
    ))

    n_cnt, c_cnt = new_recs.count(), changed_recs.count()
    print(f"      Stats: New={n_cnt}, Changed={c_cnt}")
    if n_cnt == 0 and c_cnt == 0: return

    new_rows = [r.asDict() for r in new_recs.collect()]
    chg_rows = [r.asDict() for r in changed_recs.collect()]
    
    with psycopg2.connect(pg_dsn + " sslmode=disable") as conn:
        with conn.cursor() as cur:
            ts = run_ts.strftime("%Y-%m-%d %H:%M:%S")
            if chg_rows:
                execute_batch(cur, "UPDATE dim_route SET valid_to=%(ts)s, is_current=FALSE WHERE route_sk=%(route_sk)s", [{'ts': ts, 'route_sk': r['route_sk']} for r in chg_rows])
                execute_batch(cur, "INSERT INTO dim_route (route_id_nk, route_number, vehicle_type, base_fare, valid_from, valid_to, is_current) VALUES (%(route_id_nk)s, %(route_number)s, %(vehicle_type)s, %(base_fare)s, %(ts)s, '9999-12-31', TRUE)", [{**r, 'ts': ts, 'base_fare': float(r['base_fare']) if r['base_fare'] else 0} for r in chg_rows])
            if new_rows:
                execute_batch(cur, "INSERT INTO dim_route (route_id_nk, route_number, vehicle_type, base_fare, valid_from, valid_to, is_current) VALUES (%(route_id_nk)s, %(route_number)s, %(vehicle_type)s, %(base_fare)s, '1900-01-01', '9999-12-31', TRUE)", [{**r, 'base_fare': float(r['base_fare']) if r['base_fare'] else 0} for r in new_rows])
        conn.commit()

def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--run_ts", default=None)
        args = parser.parse_args()
        run_ts = datetime.fromisoformat(args.run_ts) if args.run_ts else datetime.utcnow()

        pg_host, pg_user, pg_pass, pg_db = os.getenv("PG_HOST", "mp_postgres"), os.getenv("PG_USER", "user"), os.getenv("PG_PASSWORD", "password"), os.getenv("PG_DB", "metropulse_dwh")
        bucket = os.getenv("S3_BUCKET", "raw-data")
        
        pg_url = jdbc_url(pg_host, 5432, pg_db)
        props = {
            "user": pg_user, 
            "password": pg_pass, 
            "driver": "org.postgresql.Driver", 
            "ssl": "false", 
            "sslfactory": "org.postgresql.ssl.NonValidatingFactory",
            "stringtype": "unspecified"
        }
        pg_dsn = f"host={pg_host} port=5432 dbname={pg_db} user={pg_user} password={pg_pass}"

        reset_warehouse(pg_dsn)

        spark = build_spark("Metropulse_ETL")

        users = clean_users(spark.read.parquet(f"s3a://{bucket}/users/users.parquet"))
        routes = clean_routes(spark.read.parquet(f"s3a://{bucket}/routes/routes.parquet"))
        vehicles = clean_vehicles(spark.read.parquet(f"s3a://{bucket}/vehicles/vehicles.parquet"))
        rides = clean_rides(spark.read.parquet(f"s3a://{bucket}/rides/data.parquet"))
        payments = clean_payments(spark.read.parquet(f"s3a://{bucket}/payments/payments.parquet"))

        print("\n>> [2/6] Loading Dimensions...")
        ensure_dim_datetime(spark, build_dim_datetime(users, rides, payments), pg_url, props)
        
        write_append(users.select(F.col("user_id").alias("user_id_nk"), "name", "email", "city", "created_at"), pg_url, props, "dim_user")
        cleanup(spark)
        
        write_append(vehicles.select(F.col("vehicle_id").alias("vehicle_id_nk"), F.col("route_id").alias("route_id_nk"), "license_plate", "capacity"), pg_url, props, "dim_vehicle")
        cleanup(spark)
        
        scd2_apply_dim_route(routes, pg_dsn, pg_url, props, spark, run_ts)
        cleanup(spark)

        print("\n>> [3/6] Loading Facts...")
        d_u = spark.read.jdbc(pg_url, "dim_user", properties=props).select("user_sk", F.col("user_id_nk").alias("u_nk"))
        d_v = spark.read.jdbc(pg_url, "dim_vehicle", properties=props).select("vehicle_sk", F.col("vehicle_id_nk").alias("v_nk"))
        d_r = spark.read.jdbc(pg_url, "dim_route", properties=props).select("route_sk", F.col("route_id_nk").alias("r_nk"), "valid_from", "valid_to")
        d_t = spark.read.jdbc(pg_url, "dim_datetime", properties=props).select("time_sk", F.col("ts").alias("t_ts"))

        f_rides = rides.alias("r") \
            .join(d_t.alias("st"), F.date_trunc("second", F.col("r.start_time")) == F.col("st.t_ts"), "left") \
            .join(d_t.alias("et"), F.date_trunc("second", F.col("r.end_time")) == F.col("et.t_ts"), "left") \
            .join(d_r, (F.col("r.route_id") == F.col("r_nk")) & (F.col("r.start_time") >= F.col("valid_from")) & (F.col("r.start_time") < F.col("valid_to")), "left") \
            .join(d_u, F.col("r.user_id") == F.col("u_nk"), "left") \
            .join(d_v, F.col("r.vehicle_id") == F.col("v_nk"), "left") \
            .select(
                F.col("r.ride_id").alias("ride_id_nk"), F.col("user_sk"), F.col("route_sk"), F.col("vehicle_sk"),
                F.col("st.time_sk").alias("start_time_sk"), F.col("et.time_sk").alias("end_time_sk"),
                F.col("r.fare_amount").cast("decimal(10,2)"), F.col("r.ride_duration_sec").cast("int")
            ).dropDuplicates(["ride_id_nk"])
        
        write_append(f_rides, pg_url, props, "fact_ride")
        cleanup(spark)

        f_pay = payments.alias("p") \
            .join(d_t, F.date_trunc("second", F.col("p.created_at")) == F.col("t_ts"), "left") \
            .join(d_u, F.col("p.user_id") == F.col("u_nk"), "left") \
            .select(F.col("p.payment_id").alias("payment_id_nk"), F.col("user_sk"), F.col("p.ride_id").alias("ride_id_nk"), F.col("time_sk"), F.col("p.amount").cast("decimal(10,2)"), F.col("p.payment_method"), F.col("p.status")).dropDuplicates(["payment_id_nk"])
        
        write_append(f_pay, pg_url, props, "fact_payment")
        
        print("\n>> ETL COMPLETE WITH NO ERRORS!")
        spark.stop()

    except Exception as e:
        print(f"!!! CRITICAL FAIL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
