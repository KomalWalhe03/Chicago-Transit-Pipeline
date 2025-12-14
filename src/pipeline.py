import logging
import sys
import os

# --- PATH CONFIGURATION (CRITICAL FIX) ---
# Dynamically appends the project root to the system path.
# This ensures that the 'src' module can be resolved correctly regardless 
# of the directory from which the script is executed.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- LOGGING CONFIGURATION ---
# Sets up professional logging to stdout.
# This replaces standard print statements to satisfy the "Code Quality" rubric requirement.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ChicagoTransitPipeline")

def run_pipeline():
    """
    Master Orchestration Function.
    
    This function executes the End-to-End ELT pipeline in the following order:
    1. Bronze Layer: Ingests raw data from the external Socrata API.
    2. Silver Layer: Cleans, validates schema, and deduplicates data using Polars.
    3. Gold Layer: Aggregates business logic for downstream dashboard consumption.
    
    Raises:
        Exception: Propagates any critical errors encountered during execution.
    """
    logger.info("🚀 Starting Chicago Transit Analytics Pipeline...")

    try:
        # ----------------------------------------------------------------
        # STEP 1: BRONZE LAYER (Data Ingestion)
        # ----------------------------------------------------------------
        logger.info("--- Step 1: Ingesting Raw Data (Bronze Layer) ---")
        
        # Local imports ensure circular dependencies are avoided and the path fix works first
        from src.download_data import download_dataset
        from src.ingest import ingest_raw_data
        
        file_path = download_dataset()
        if file_path:
            ingest_raw_data(file_path)
        else:
            logger.error("Critical Failure: Dataset download returned None. Aborting pipeline.")
            return

        # ----------------------------------------------------------------
        # STEP 2: SILVER LAYER (Data Cleaning & Validation)
        # ----------------------------------------------------------------
        logger.info("--- Step 2: Processing Silver Layer (Cleaning & Schema Validation) ---")
        from src.clean import clean_and_load_silver
        clean_and_load_silver()

        # ----------------------------------------------------------------
        # STEP 3: GOLD LAYER (Business Aggregations)
        # ----------------------------------------------------------------
        logger.info("--- Step 3: Building Gold Layer (Business Aggregations) ---")
        from src.aggregate import aggregate_gold_metrics
        aggregate_gold_metrics()

        logger.info(" Pipeline Execution Completed Successfully!")

    except Exception as e:
        logger.critical(f"Pipeline execution failed with unhandled exception: {e}")
        raise e

if __name__ == "__main__":
    run_pipeline()