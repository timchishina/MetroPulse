from __future__ import annotations

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def jdbc_read(spark: SparkSession, url: str, table: str, props: dict):
    return (
        spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .options(**{k: str(v) for k, v in props.items()})
        .load()
    )


def jdbc_write(df, url: str, table: str, props: dict, mode: str = "overwrite"):
    (
        df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .options(**{k: str(v) for k, v in props.items()})
        .mode(mode)
        .save()
    )


def mart_route_hour(spark: SparkSession, pg_url: str, pg_props: dict):

    fact_ride = jdbc_read(spark, pg_url, "fact_ride", pg_props)
    dim_route = jdbc_read(spark, pg_url, "dim_route", pg_props).select(
        "route_sk", "route_id_nk", "route_number", "vehicle_type", "is_current"
    )
    dim_dt = jdbc_read(spark, pg_url, "dim_datetime", pg_props).select(
        F.col("time_sk"), F.col("date").alias("dt"), F.col("hour").alias("hh")
    )

    rides_h = (
        fact_ride
        .join(dim_route, on="route_sk", how="left")
        .join(dim_dt.withColumnRenamed("time_sk", "start_time_sk"), on="start_time_sk", how="left")
        .groupBy("dt", "hh", "route_number")
        .agg(
            F.count(F.lit(1)).cast("long").alias("ride_cnt"),
            F.avg(F.col("fare_amount").cast("double")).alias("avg_fare"),
            F.avg(F.col("ride_duration_sec").cast("double")).alias("avg_duration_sec"),
        )
    )

    fvm = jdbc_read(spark, pg_url, "fact_vehicle_movement", pg_props)

    movement_h = (
        fvm
        .join(dim_route, on="route_sk", how="left")
        .join(dim_dt.withColumnRenamed("time_sk", "time_sk_fvm"), fvm.time_sk == F.col("time_sk_fvm"), how="left")
        .groupBy(F.col("dt"), F.col("hh"), F.col("route_number"))
        .agg(
            F.avg(F.col("avg_speed_kmh").cast("double")).alias("avg_speed_kmh"),
            F.max(F.col("max_speed_kmh").cast("double")).alias("max_speed_kmh"),
            F.avg(F.col("avg_passengers_estimated").cast("double")).alias("avg_passengers_estimated"),
            F.sum(F.col("events_count").cast("long")).alias("movement_events_cnt"),
        )
    )

    mart = (
        rides_h.alias("r")
        .join(
            movement_h.alias("m"),
            on=[F.col("r.dt") == F.col("m.dt"), F.col("r.hh") == F.col("m.hh"), F.col("r.route_number") == F.col("m.route_number")],
            how="full"
        )
        .select(
            F.coalesce(F.col("r.dt"), F.col("m.dt")).alias("dt"),
            F.coalesce(F.col("r.hh"), F.col("m.hh")).cast("int").alias("hour"),
            F.coalesce(F.col("r.route_number"), F.col("m.route_number")).alias("route_number"),
            F.col("r.ride_cnt"),
            F.col("r.avg_fare"),
            F.col("r.avg_duration_sec"),
            F.col("m.avg_speed_kmh"),
            F.col("m.max_speed_kmh"),
            F.col("m.avg_passengers_estimated"),
            F.col("m.movement_events_cnt"),
            F.current_timestamp().alias("load_ts"),
        )
        .fillna({
            "ride_cnt": 0,
            "movement_events_cnt": 0,
        })
    )

    return mart


def mart_city_day(spark: SparkSession, pg_url: str, pg_props: dict):

    dim_user = jdbc_read(spark, pg_url, "dim_user", pg_props).select("user_sk", "city")
    dim_dt = jdbc_read(spark, pg_url, "dim_datetime", pg_props).select("time_sk", F.col("date").alias("dt"))

    fact_ride = jdbc_read(spark, pg_url, "fact_ride", pg_props).select(
        "user_sk", "start_time_sk", F.col("fare_amount").cast("double").alias("fare_amount")
    )

    rides = (
        fact_ride
        .join(dim_user, on="user_sk", how="left")
        .join(dim_dt.withColumnRenamed("time_sk", "start_time_sk"), on="start_time_sk", how="left")
        .groupBy("dt", "city")
        .agg(
            F.count(F.lit(1)).cast("long").alias("rides_cnt"),
            F.countDistinct("user_sk").cast("long").alias("users_distinct"),
            F.sum("fare_amount").alias("rides_revenue_nominal"),
        )
    )

    fact_payment = jdbc_read(spark, pg_url, "fact_payment", pg_props).select(
        "user_sk", "time_sk", F.col("amount").cast("double").alias("amount"), "status"
    )

    pay = (
        fact_payment
        .join(dim_user, on="user_sk", how="left")
        .join(dim_dt, on="time_sk", how="left")
        .groupBy("dt", "city")
        .agg(
            F.sum(F.when(F.col("status") == F.lit("success"), 1).otherwise(0)).cast("long").alias("payments_success_cnt"),
            F.sum(F.when(F.col("status") == F.lit("success"), F.col("amount")).otherwise(F.lit(0.0))).alias("payments_success_amount"),
            F.sum(F.when(F.col("status") != F.lit("success"), 1).otherwise(0)).cast("long").alias("payments_failed_cnt"),
        )
    )

    mart = (
        rides.alias("r")
        .join(pay.alias("p"), on=[F.col("r.dt") == F.col("p.dt"), F.col("r.city") == F.col("p.city")], how="full")
        .select(
            F.coalesce(F.col("r.dt"), F.col("p.dt")).alias("dt"),
            F.coalesce(F.col("r.city"), F.col("p.city")).alias("city"),
            F.col("r.rides_cnt"),
            F.col("r.users_distinct"),
            F.col("r.rides_revenue_nominal"),
            F.col("p.payments_success_cnt"),
            F.col("p.payments_success_amount"),
            F.col("p.payments_failed_cnt"),
            F.current_timestamp().alias("load_ts"),
        )
        .fillna({
            "rides_cnt": 0,
            "users_distinct": 0,
            "rides_revenue_nominal": 0.0,
            "payments_success_cnt": 0,
            "payments_success_amount": 0.0,
            "payments_failed_cnt": 0,
        })
    )

    return mart


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"))
    parser.add_argument("--pg-port", default=os.getenv("PG_PORT", "5432"))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "metropulse_dwh"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "user"))
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASSWORD", "password"))

    parser.add_argument("--ch-host", default=os.getenv("CH_HOST", "localhost"))
    parser.add_argument("--ch-port", default=os.getenv("CH_PORT", "8123"))  
    parser.add_argument("--ch-db", default=os.getenv("CH_DB", "metropulse"))
    parser.add_argument("--ch-user", default=os.getenv("CH_USER", "default"))
    parser.add_argument("--ch-pass", default=os.getenv("CH_PASSWORD", ""))

    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")

    args = parser.parse_args()

    spark = build_spark("metropulse-clickhouse-marts")

    pg_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
    pg_props = {
        "user": args.pg_user,
        "password": args.pg_pass,
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }

    ch_url = f"jdbc:clickhouse://{args.ch_host}:{args.ch_port}/{args.ch_db}"
    ch_props = {
        "user": args.ch_user,
        "password": args.ch_pass,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "socket_timeout": "600000",
        "connect_timeout": "20000",
    }

    m1 = mart_route_hour(spark, pg_url, pg_props)
    m2 = mart_city_day(spark, pg_url, pg_props)

    jdbc_write(m1, ch_url, "metropulse.mart_route_hour", ch_props, mode=args.mode)
    jdbc_write(m2, ch_url, "metropulse.mart_city_day", ch_props, mode=args.mode)

    print("Marts loaded to ClickHouse.")
    spark.stop()


if __name__ == "__main__":
    main()
