import polars as pl
from pymongo import MongoClient
from loguru import logger
import os

# Connection Details
MONGO_URI = "mongodb://admin:secret@localhost:27017/"
DB_NAME = "chicago_transit"
PARQUET_FILE = "data/processed/silver_trips.parquet"

def aggregate_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Load clean data from Parquet for high-performance aggregation
    if not os.path.exists(PARQUET_FILE):
        logger.error(f"Parquet file {PARQUET_FILE} not found. Ensure clean.py has been executed.")
        return

    logger.info(f"Loading data from {PARQUET_FILE}...")
    df = pl.read_parquet(PARQUET_FILE)
    logger.info(f"Loaded {len(df)} rows for aggregation.")

    # --- Aggregation 1: Traffic Trends by Hour ---
    # Analyzing peak hours for trips and fare averages
    logger.info("Computing hourly traffic stats...")
    df_hourly = (
        df.with_columns(
            pl.col("trip_start_timestamp").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S.%f").dt.hour().alias("hour")
        )
        .group_by("hour")
        .agg([
            pl.len().alias("total_trips"),
            pl.col("fare").mean().round(2).alias("avg_fare"),
            pl.col("duration_min").mean().round(2).alias("avg_duration")
        ])
        .sort("hour")
    )
    
    # Save Hourly Stats to Gold Layer
    db["gold_hourly_stats"].delete_many({})
    db["gold_hourly_stats"].insert_many(df_hourly.to_dicts())

    # --- Aggregation 2: Top Pickup Areas ---
    # Identifying high-density community areas
    logger.info("Computing top pickup locations...")
    df_areas = (
        df.group_by("pickup_community_area")
        .agg([
            pl.len().alias("trip_count"),
            pl.col("fare").mean().round(2).alias("avg_fare")
        ])
        .sort("trip_count", descending=True)
        .head(10) # Top 10 areas only
    )

    # Save Area Stats to Gold Layer
    db["gold_area_stats"].delete_many({})
    db["gold_area_stats"].insert_many(df_areas.to_dicts())

    # --- Aggregation 3: Payment Method Analysis ---
    logger.info("Computing payment method distribution...")
    df_payment = (
        df.group_by("payment_type")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )

    # Save Payment Stats to Gold Layer
    db["gold_payment_stats"].delete_many({})
    db["gold_payment_stats"].insert_many(df_payment.to_dicts())

    logger.success("Gold Layer aggregations complete. Insights saved to MongoDB.")

if __name__ == "__main__":
    aggregate_data()