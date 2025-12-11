import polars as pl
from pymongo import MongoClient
from loguru import logger
import os
import time

# Connection Details
MONGO_URI = "mongodb://admin:secret@localhost:27017/"
DB_NAME = "chicago_transit"
COLLECTION_NAME = "raw_trips"
DATA_FILE = "data/raw_data.csv"

def ingest_data():
    # Verify raw data file exists
    if not os.path.exists(DATA_FILE):
        logger.error(f"File {DATA_FILE} not found! Ensure download_data.py has been executed.")
        return

    # Establish MongoDB connection
    logger.info("Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Load data efficiently using Polars
    logger.info(f"Reading {DATA_FILE}...")
    start_time = time.time()
    
    # Read CSV and drop rows with missing Trip IDs
    df = pl.read_csv(DATA_FILE, ignore_errors=True).drop_nulls(subset=["trip_id"])
    
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

if __name__ == "__main__":
    ingest_data()