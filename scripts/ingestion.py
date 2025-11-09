#!/usr/bin/env python3
"""
Bronze Layer: Data Ingestion
Downloads raw data from Kaggle and stores in local filesystem
"""

import os
import logging
from pathlib import Path
from datetime import datetime
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def download_kaggle_dataset():
    """
    Download Telco Customer Churn dataset from Kaggle
    
    Returns:
        str: Path to the downloaded CSV file
    """
    try:
        # Import kaggle after setting credentials
        import kaggle
        from kaggle.api.kaggle_api_extended import KaggleApi
        
        logger.info("Initializing Kaggle API client...")
        api = KaggleApi()
        api.authenticate()
        
        # Dataset details
        dataset_owner = "blastchar"
        dataset_name = "telco-customer-churn"
        dataset_slug = f"{dataset_owner}/{dataset_name}"
        
        # Create bronze directory if it doesn't exist
        bronze_dir = Path("/opt/pipeline/data/bronze")
        bronze_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Downloading dataset: {dataset_slug}")
        logger.info(f"Target directory: {bronze_dir}")
        
        # Download dataset
        api.dataset_download_files(
            dataset_slug,
            path=str(bronze_dir),
            unzip=True,
            quiet=False
        )
        
        logger.info("Download completed successfully")
        
        # Find the downloaded CSV file
        csv_files = list(bronze_dir.glob("*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {bronze_dir}")
        
        csv_file = csv_files[0]
        logger.info(f"Dataset file: {csv_file}")
        logger.info(f"File size: {csv_file.stat().st_size / (1024*1024):.2f} MB")
        
        # Rename to standard name for consistency
        standard_name = bronze_dir / "telco_customer_churn.csv"
        if csv_file != standard_name:
            csv_file.rename(standard_name)
            csv_file = standard_name
            logger.info(f"Renamed to: {standard_name}")
        
        # Create metadata file
        metadata = {
            "dataset_source": "Kaggle",
            "dataset_slug": dataset_slug,
            "download_timestamp": datetime.now().isoformat(),
            "file_path": str(csv_file),
            "file_size_bytes": csv_file.stat().st_size,
            "file_size_mb": round(csv_file.stat().st_size / (1024*1024), 2)
        }
        
        metadata_file = bronze_dir / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved to: {metadata_file}")
        
        return str(csv_file)
        
    except Exception as e:
        logger.error(f"Error downloading dataset: {str(e)}")
        raise


def validate_bronze_data(file_path):
    """
    Validate the downloaded data
    
    Args:
        file_path (str): Path to CSV file
        
    Returns:
        dict: Validation results
    """
    import pandas as pd
    
    logger.info(f"Validating bronze data: {file_path}")
    
    try:
        # Read the CSV
        df = pd.read_csv(file_path)
        
        validation_results = {
            "file_exists": True,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "memory_usage_mb": round(df.memory_usage(deep=True).sum() / (1024*1024), 2),
            "has_nulls": bool(df.isnull().any().any()),
            "null_columns": df.columns[df.isnull().any()].tolist(),
            "validation_timestamp": datetime.now().isoformat()
        }
        
        logger.info("=" * 60)
        logger.info("BRONZE DATA VALIDATION RESULTS")
        logger.info("=" * 60)
        logger.info(f"Total Rows: {validation_results['row_count']:,}")
        logger.info(f"Total Columns: {validation_results['column_count']}")
        logger.info(f"Memory Usage: {validation_results['memory_usage_mb']} MB")
        logger.info(f"Has Null Values: {validation_results['has_nulls']}")
        if validation_results['null_columns']:
            logger.info(f"Columns with Nulls: {', '.join(validation_results['null_columns'])}")
        logger.info("=" * 60)
        
        # Save validation results
        validation_file = Path(file_path).parent / "validation_results.json"
        with open(validation_file, 'w') as f:
            json.dump(validation_results, f, indent=2)
        
        logger.info(f"Validation results saved to: {validation_file}")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        raise


def main():
    """Main execution function"""
    logger.info("Starting Bronze Layer Ingestion")
    logger.info("=" * 60)
    
    try:
        # Step 1: Download dataset
        file_path = download_kaggle_dataset()
        
        # Step 2: Validate data
        validation_results = validate_bronze_data(file_path)
        
        # Step 3: Success
        logger.info("=" * 60)
        logger.info("✓ Bronze Layer Ingestion Completed Successfully")
        logger.info(f"✓ Data Location: {file_path}")
        logger.info(f"✓ Total Records: {validation_results['row_count']:,}")
        logger.info("=" * 60)
        
        return file_path
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("✗ Bronze Layer Ingestion Failed")
        logger.error(f"✗ Error: {str(e)}")
        logger.error("=" * 60)
        raise


if __name__ == "__main__":
    main()
