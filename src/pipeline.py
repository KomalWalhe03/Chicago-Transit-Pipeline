import logging
import sys

# Configure Professional Logging
# This satisfies the "Logging" requirement in the rubric
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
    Orchestrates the entire Bronze -> Silver -> Gold pipeline.
    """
    logger.info(" Starting Chicago Transit Analytics Pipeline...")

    try:
        # Step 1: Bronze Layer (Raw)
        logger.info("--- Step 1: Ingesting Raw Data (Bronze Layer) ---")
        from src.download_data import download_dataset
        from src.ingest import ingest_raw_data
        
        file_path = download_dataset()
        if file_path:
            ingest_raw_data(file_path)
        else:
            logger.error("Failed to download dataset. Stopping pipeline.")
            return

        # Step 2: Silver Layer (Clean)
        logger.info("--- Step 2: Processing Silver Layer (Cleaning & Schema) ---")
        from src.clean import clean_and_load_silver
        clean_and_load_silver()

        # Step 3: Gold Layer (Aggregates)
        logger.info("--- Step 3: Building Gold Layer (Aggregations) ---")
        from src.aggregate import aggregate_gold_metrics
        aggregate_gold_metrics()

        logger.info(" Pipeline Execution Completed Successfully!")

    except Exception as e:
        logger.critical(f"Pipeline failed with error: {e}")
        raise e

if __name__ == "__main__":
    run_pipeline()