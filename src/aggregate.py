import polars as pl
from pymongo import MongoClient
import logging
import os

# --- LOGGING SETUP ---
logger = logging.getLogger("ChicagoTransitPipeline")

# --- CONFIGURATION ---
MONGO_URI = "mongodb://admin:secret@localhost:27017/"
DB_NAME = "chicago_transit"
PARQUET_FILE = "data/processed/silver_trips.parquet"

def aggregate_gold_metrics():
    """
    Reads clean Silver data (Parquet), performs business aggregations,
    and saves results to the Gold Layer (MongoDB).
    """
    if not os.path.exists(PARQUET_FILE):
        logger.error(f"Silver Parquet file not found: {PARQUET_FILE}. Run Step 2 first.")
        return

    logger.info("Loading Silver Layer data from Parquet...")
    df = pl.read_parquet(PARQUET_FILE)
    logger.info(f"Loaded {len(df)} rows for aggregation.")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # ----------------------------------------------------------------
    # AGGREGATION 1: HOURLY TRAFFIC PATTERNS
    # ----------------------------------------------------------------
    logger.info("Computing Hourly Traffic Stats...")
    
    # CRITICAL FIX: Explicitly convert String to Datetime before extracting .dt.hour()
    # This prevents the "InvalidOperationError: hour operation not supported for dtype str"
    hourly_stats = (
        df.with_columns(
            pl.col("trip_start_timestamp")
            .str.to_datetime(strict=False) # Convert String -> Datetime
            .dt.hour() # Extract Hour
            .alias("hour")
        )
        .group_by("hour")
        .agg([
            pl.len().alias("trip_count"), # Match dashboard expected column name
            pl.col("fare").mean().round(2).alias("avg_fare"),
            pl.col("duration_min").mean().round(2).alias("avg_duration")
        ])
        .sort("hour")
    )
    
    # Save to MongoDB
    col_hourly = db["gold_hourly_stats"]
    col_hourly.delete_many({})
    col_hourly.insert_many(hourly_stats.to_dicts())
    logger.info("Saved 'gold_hourly_stats' to MongoDB.")

    # ----------------------------------------------------------------
    # AGGREGATION 2: PAYMENT TYPE PREFERENCES
    # ----------------------------------------------------------------
    logger.info("Computing Payment Type Stats...")
    
    payment_stats = (
        df.group_by("payment_type")
        .agg([
            pl.len().alias("count"),
            pl.col("trip_total").mean().round(2).alias("avg_cost")
        ])
        .sort("count", descending=True)
    )
    
    # Save to MongoDB
    col_pay = db["gold_payment_stats"]
    col_pay.delete_many({})
    col_pay.insert_many(payment_stats.to_dicts())
    logger.info("Saved 'gold_payment_stats' to MongoDB.")

    # ----------------------------------------------------------------
    # AGGREGATION 3: TOP PICKUP AREAS
    # ----------------------------------------------------------------
    logger.info("Computing Top Pickup Areas...")
    
    area_stats = (
        df.group_by("pickup_community_area")
        .agg([
            pl.len().alias("trip_count"),
            pl.col("fare").mean().round(2).alias("avg_fare")
        ])
        .sort("trip_count", descending=True)
        .head(10) # Top 10 areas only
    )
    
    # Save to MongoDB
    col_area = db["gold_area_stats"]
    col_area.delete_many({})
    col_area.insert_many(area_stats.to_dicts())
    logger.info("Saved 'gold_area_stats' to MongoDB.")
    
    logger.info(" Gold Layer Aggregations Complete.")

if __name__ == "__main__":
    aggregate_gold_metrics()