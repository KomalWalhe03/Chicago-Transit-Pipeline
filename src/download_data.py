import requests
import os
from loguru import logger

# Constants
DATASET_ID = "wrvz-psew"
BASE_URL = f"https://data.cityofchicago.org/resource/{DATASET_ID}.csv"
LIMIT = 800000  # Define row limit for dataset
OUTPUT_FILE = "data/raw_data.csv"

def fetch_data():
    os.makedirs("data", exist_ok=True)
    
    # Configure API parameters for latest data
    # Fetching in descending order to get the most recent records
    params = {
        "$limit": LIMIT,
        "$order": "trip_start_timestamp DESC" 
    }

    logger.info(f"Downloading {LIMIT} rows (Latest Data)...")
    
    try:
        # Stream the download to handle large file sizes efficiently
        with requests.get(BASE_URL, params=params, stream=True) as r:
            r.raise_for_status()
            
            with open(OUTPUT_FILE, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)
        
        # Validate downloaded file size
        size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
        if size_mb < 1:
            logger.error(f"FAIL: File is too small ({size_mb:.2f} MB).")
            # Inspect response content on failure to identify API errors
            with open(OUTPUT_FILE, 'r') as f:
                print("API ERROR MESSAGE:", f.read(200)) 
        else:
            logger.success(f"Success! Downloaded {size_mb:.2f} MB to {OUTPUT_FILE}")
            
    except Exception as e:
        logger.error(f"Network Failed: {e}")

if __name__ == "__main__":
    fetch_data()