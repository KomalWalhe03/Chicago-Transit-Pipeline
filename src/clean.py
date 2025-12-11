import polars as pl
from pymongo import MongoClient
from loguru import logger
import os

# Connection Details
MONGO_URI = "mongodb://admin:secret@localhost:27017/"
DB_NAME = "chicago_transit"
SOURCE_COLLECTION = "raw_trips"
TARGET_COLLECTION = "silver_trips"

def clean_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Fetch raw data from Bronze layer
    logger.info("Fetching raw data...")
    data = list(db[SOURCE_COLLECTION].find({}, {"_id": 0}))
    df = pl.DataFrame(data)
    
    # Define schema for validation
    keep_cols = [
        "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp",
        "trip_seconds", "trip_miles", "fare", "tips", "tolls", "extras",
        "trip_total", "payment_type", "company", 
        "pickup_community_area", "dropoff_community_area"
    ]
    
    # Filter dataset to strictly include defined columns
    df = df.select([c for c in keep_cols if c in df.columns])

    logger.info(f"Processing {len(df)} rows with strict cleaning...")

    df_clean = (
        df
        # Enforce type consistency for numerical fields
        .with_columns([
            pl.col("trip_seconds").cast(pl.Float64),
            pl.col("trip_miles").cast(pl.Float64),
            pl.col("fare").cast(pl.Float64),
            pl.col("tips").cast(pl.Float64).fill_null(0.0),   # Fill missing tips with 0
            pl.col("tolls").cast(pl.Float64).fill_null(0.0),  # Fill missing tolls with 0
            pl.col("extras").cast(pl.Float64).fill_null(0.0),
            pl.col("trip_total").cast(pl.Float64)
        ])
        
        # Apply cleaning logic
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
    logger.success(f"Final Cleaned Count: {final_count} rows")

    # Save snapshot to Parquet for efficient storage
    os.makedirs("data/processed", exist_ok=True)
    parquet_path = "data/processed/silver_trips.parquet"
    df_clean.write_parquet(parquet_path)
    logger.info(f"Saved parquet file to {parquet_path} (Size: {os.path.getsize(parquet_path)/1024**2:.2f} MB)")

    # Update Silver Layer in MongoDB
    target_col = db[TARGET_COLLECTION]
    target_col.delete_many({})
    if final_count > 0:
        target_col.insert_many(df_clean.to_dicts())
        logger.success("Updated MongoDB 'silver_trips' collection.")

if __name__ == "__main__":
    clean_data()