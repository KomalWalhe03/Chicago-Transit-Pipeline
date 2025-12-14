import requests
import os
from loguru import logger

# Constants
DATASET_ID = "wrvz-psew"
BASE_URL = f"https://data.cityofchicago.org/resource/{DATASET_ID}.csv"
LIMIT = 800000  # Define row limit for dataset
OUTPUT_FILE = "data/raw_data.csv"

# --- RENAMED FUNCTION TO MATCH PIPELINE ---
def download_dataset():
    """
    Fetches the latest dataset from the Chicago Data Portal API.
    Returns:
        str: The file path of the downloaded CSV, or None if failed.
    """
    os.makedirs("data", exist_ok=True)
    
    # Configure API parameters for latest data
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
            return None # Return None on failure
        else:
            logger.success(f"Success! Downloaded {size_mb:.2f} MB to {OUTPUT_FILE}")
            return OUTPUT_FILE # --- CRITICAL: Return the path to the pipeline ---
            
    except Exception as e:
        logger.error(f"Network Failed: {e}")
        return None

if __name__ == "__main__":
    download_dataset()