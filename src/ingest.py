import polars as pl
from pymongo import MongoClient
from loguru import logger
import os
import time

# Connection Details
MONGO_URI = "mongodb://admin:secret@localhost:27017/"
DB_NAME = "chicago_transit"
COLLECTION_NAME = "raw_trips"

# --- RENAMED FUNCTION TO MATCH PIPELINE ---
def ingest_raw_data(file_path):
    """
    Ingests the CSV data into MongoDB (Bronze Layer) using Polars.
    Args:
        file_path (str): Path to the downloaded CSV file.
    """
    # Verify raw data file exists
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} not found! Ensure download_data.py has been executed.")
        return

    # Establish MongoDB connection
    logger.info("Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Load data efficiently using Polars
    logger.info(f"Reading {file_path}...")
    start_time = time.time()
    
    # Read CSV and drop rows with missing Trip IDs
    try:
        df = pl.read_csv(file_path, ignore_errors=True).drop_nulls(subset=["trip_id"])
        
        # Convert DataFrame to dictionary format for MongoDB insertion
        logger.info("Converting to JSON format...")
        records = df.to_dicts()
        
        # Batch insert records into MongoDB
        count = len(records)
        logger.info(f"Inserting {count} rows into MongoDB...")
        
        if records:
            collection.delete_many({}) # Ensure idempotency by clearing existing collection
            collection.insert_many(records)
        
        duration = time.time() - start_time
        logger.success(f"Successfully ingested {count} rows in {duration:.2f} seconds!")
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise e

if __name__ == "__main__":
    # Test locally
    ingest_raw_data("data/raw_data.csv")