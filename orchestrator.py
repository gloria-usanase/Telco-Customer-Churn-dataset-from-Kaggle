#!/usr/bin/env python3
"""
Pipeline Orchestrator
Simple script-based orchestration to replace Airflow
Runs the complete data pipeline: Ingestion → Transformation → Modeling
"""

import sys
import os
import logging
from datetime import datetime
import time

# Add scripts directory to path
sys.path.insert(0, '/opt/pipeline/scripts')

# Import pipeline modules
import ingestion
import transformation
import modeling

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/pipeline/logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """
    Execute the complete data pipeline
    Returns: True if successful, False otherwise
    """
    pipeline_start = time.time()
    
    logger.info("=" * 80)
    logger.info("STARTING DATA PIPELINE EXECUTION")
    logger.info("=" * 80)
    logger.info(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    
    try:
        # Stage 1: Ingestion (Bronze Layer)
        logger.info("=" * 80)
        logger.info("STAGE 1/3: DATA INGESTION (Bronze Layer)")
        logger.info("=" * 80)
        stage_start = time.time()
        
        ingestion.main()
        
        stage_duration = time.time() - stage_start
        logger.info(f"✓ Stage 1 completed in {stage_duration:.2f} seconds")
        logger.info("")
        
        # Stage 2: Transformation (Silver Layer)
        logger.info("=" * 80)
        logger.info("STAGE 2/3: DATA TRANSFORMATION (Silver Layer)")
        logger.info("=" * 80)
        stage_start = time.time()
        
        transformation.main()
        
        stage_duration = time.time() - stage_start
        logger.info(f"✓ Stage 2 completed in {stage_duration:.2f} seconds")
        logger.info("")
        
        # Stage 3: Modeling (Gold Layer)
        logger.info("=" * 80)
        logger.info("STAGE 3/3: ANALYTICS MODELING (Gold Layer)")
        logger.info("=" * 80)
        stage_start = time.time()
        
        modeling.main()
        
        stage_duration = time.time() - stage_start
        logger.info(f"✓ Stage 3 completed in {stage_duration:.2f} seconds")
        logger.info("")
        
        # Pipeline Summary
        pipeline_duration = time.time() - pipeline_start
        
        logger.info("=" * 80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Status: SUCCESS ✓")
        logger.info(f"Total Duration: {pipeline_duration:.2f} seconds ({pipeline_duration/60:.2f} minutes)")
        logger.info(f"Completion Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("")
        logger.info("Data Layers:")
        logger.info("  ✓ Bronze: Raw data ingested from Kaggle")
        logger.info("  ✓ Silver: Cleaned data in PostgreSQL staging")
        logger.info("  ✓ Gold: Analytics models ready for consumption")
        logger.info("")
        logger.info("Access your data:")
        logger.info("  • Database: postgresql://airflow:airflow@postgres:5432/airflow")
        logger.info("  • Validation: Run './pipeline.sh validate'")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        pipeline_duration = time.time() - pipeline_start
        
        logger.error("=" * 80)
        logger.error("PIPELINE EXECUTION FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}")
        logger.error(f"Failed after: {pipeline_duration:.2f} seconds")
        logger.error("=" * 80)
        
        # Print full traceback for debugging
        import traceback
        logger.error("\nFull Traceback:")
        logger.error(traceback.format_exc())
        
        return False


if __name__ == "__main__":
    logger.info("Pipeline Orchestrator Started")
    
    success = run_pipeline()
    
    if success:
        logger.info("Pipeline orchestrator exiting successfully")
        sys.exit(0)
    else:
        logger.error("Pipeline orchestrator exiting with errors")
        sys.exit(1)
