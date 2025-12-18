import polars as pl
from pymongo import MongoClient
import logging
import os

# --- LOGGING SETUP ---
logger = logging.getLogger("ChicagoTransitPipeline")

# --- CONFIGURATION ---
# Load from environment or use default for local testing
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:secret@localhost:27017/")
DB_NAME = "chicago_transit"
SOURCE_COLLECTION = "raw_trips"
TARGET_COLLECTION = "silver_trips"
PARQUET_PATH = "data/processed/silver_trips.parquet"

# --- RENAMED TO MATCH PIPELINE ---
def clean_and_load_silver():
    """
    Fetches raw data, cleans it using Polars, and saves to Silver Layer.
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # 1. Fetch Data
    logger.info("Fetching raw data from Bronze layer (MongoDB)...")
    data = list(db[SOURCE_COLLECTION].find({}, {"_id": 0}))
    
    if not data:
        logger.warning("No data found in raw_trips! Skipping Silver cleaning.")
        return

    df = pl.DataFrame(data)
    
    # Define schema columns to keep
    keep_cols = [
        "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp",
        "trip_seconds", "trip_miles", "fare", "tips", "tolls", "extras",
        "trip_total", "payment_type", "company", 
        "pickup_community_area", "dropoff_community_area"
    ]
    
    # Filter dataset to strictly include defined columns
    df = df.select([c for c in keep_cols if c in df.columns])

    logger.info(f"Processing {len(df)} rows with strict cleaning rules...")

    # 2. Apply Polars Cleaning Logic
    df_clean = (
        df
        # Enforce type consistency for numerical fields
        .with_columns([
            pl.col("trip_seconds").cast(pl.Float64),
            pl.col("trip_miles").cast(pl.Float64),
            pl.col("fare").cast(pl.Float64),
            pl.col("tips").cast(pl.Float64).fill_null(0.0),   
            pl.col("tolls").cast(pl.Float64).fill_null(0.0), 
            pl.col("extras").cast(pl.Float64).fill_null(0.0),
            pl.col("trip_total").cast(pl.Float64)
        ])
        
        # Apply filters
        .unique(subset=["trip_id"])           # Deduplicate by Trip ID
        .filter(pl.col("trip_seconds") > 60)  # Remove short trips (< 1 min)
        .filter(pl.col("trip_miles") > 0)     # Remove zero-distance trips
        .filter(pl.col("fare") >= 0)          # Ensure fare is non-negative
        
        # Drop rows with missing location data
        .drop_nulls(subset=["pickup_community_area", "dropoff_community_area"])
        
        # Feature Engineering: Calculate duration in minutes
        .with_columns(
            (pl.col("trip_seconds") / 60).round(2).alias("duration_min")
        )
    )

    final_count = len(df_clean)
    logger.info(f"Final Cleaned Count: {final_count} rows")

    # 3. Save to Parquet (for the Gold Step)
    os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)
    df_clean.write_parquet(PARQUET_PATH)
    logger.info(f"Saved parquet file to {PARQUET_PATH}")

    # 4. Update MongoDB Silver Collection
    target_col = db[TARGET_COLLECTION]
    target_col.delete_many({})
    if final_count > 0:
        target_col.insert_many(df_clean.to_dicts())
        logger.info("Updated MongoDB 'silver_trips' collection.")

if __name__ == "__main__":
    clean_and_load_silver()