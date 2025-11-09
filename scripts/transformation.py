#!/usr/bin/env python3
"""
Silver Layer: Data Transformation
Cleans, standardizes, and loads data into PostgreSQL staging tables
"""

import os
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Create database connection using environment variables
    
    Returns:
        sqlalchemy.engine.Engine: Database connection engine
    """
    db_host = os.getenv('DB_HOST', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'airflow')
    db_user = os.getenv('DB_USER', 'airflow')
    db_password = os.getenv('DB_PASSWORD', 'airflow')
    
    connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    logger.info(f"Connecting to database: {db_host}:{db_port}/{db_name}")
    
    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False
    )
    
    return engine


def read_bronze_data():
    """
    Read raw data from bronze layer
    
    Returns:
        pd.DataFrame: Raw data
    """
    bronze_file = "/opt/pipeline/data/bronze/telco_customer_churn.csv"
    
    logger.info(f"Reading bronze data from: {bronze_file}")
    
    if not Path(bronze_file).exists():
        raise FileNotFoundError(f"Bronze data file not found: {bronze_file}")
    
    df = pd.read_csv(bronze_file)
    
    logger.info(f"Loaded {len(df):,} records from bronze layer")
    logger.info(f"Columns: {list(df.columns)}")
    
    return df


def clean_and_transform(df):
    """
    Apply data cleaning and transformation logic
    
    Args:
        df (pd.DataFrame): Raw dataframe
        
    Returns:
        pd.DataFrame: Cleaned and transformed dataframe
    """
    logger.info("Starting data transformation...")
    
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Track transformation metrics
    initial_rows = len(df_clean)
    
    # 1. COLUMN RENAMING (standardize to snake_case)
    logger.info("Step 1: Standardizing column names...")
    column_mapping = {
        'customerID': 'customer_id',
        'gender': 'gender',
        'SeniorCitizen': 'senior_citizen',
        'Partner': 'partner',
        'Dependents': 'dependents',
        'tenure': 'tenure',
        'PhoneService': 'phone_service',
        'MultipleLines': 'multiple_lines',
        'InternetService': 'internet_service',
        'OnlineSecurity': 'online_security',
        'OnlineBackup': 'online_backup',
        'DeviceProtection': 'device_protection',
        'TechSupport': 'tech_support',
        'StreamingTV': 'streaming_tv',
        'StreamingMovies': 'streaming_movies',
        'Contract': 'contract_type',
        'PaperlessBilling': 'paperless_billing',
        'PaymentMethod': 'payment_method',
        'MonthlyCharges': 'monthly_charges',
        'TotalCharges': 'total_charges',
        'Churn': 'churn'
    }
    df_clean.rename(columns=column_mapping, inplace=True)
    
    # 2. HANDLE MISSING VALUES
    logger.info("Step 2: Handling missing values...")
    
    # TotalCharges has spaces instead of values for new customers
    df_clean['total_charges'] = df_clean['total_charges'].replace(' ', '0')
    df_clean['total_charges'] = pd.to_numeric(df_clean['total_charges'], errors='coerce').fillna(0)
    
    # Log null counts
    null_counts = df_clean.isnull().sum()
    if null_counts.any():
        logger.warning(f"Null values found:\n{null_counts[null_counts > 0]}")
    
    # 3. DATA TYPE CONVERSION
    logger.info("Step 3: Converting data types...")
    
    # Numeric fields
    df_clean['tenure'] = pd.to_numeric(df_clean['tenure'], errors='coerce').fillna(0).astype(int)
    df_clean['monthly_charges'] = pd.to_numeric(df_clean['monthly_charges'], errors='coerce').fillna(0)
    df_clean['total_charges'] = pd.to_numeric(df_clean['total_charges'], errors='coerce').fillna(0)
    
    # Boolean fields
    boolean_mapping = {'Yes': True, 'No': False, 'No phone service': False, 'No internet service': False}
    
    df_clean['senior_citizen'] = df_clean['senior_citizen'].map({1: True, 0: False})
    df_clean['partner'] = df_clean['partner'].map(boolean_mapping)
    df_clean['dependents'] = df_clean['dependents'].map(boolean_mapping)
    df_clean['phone_service'] = df_clean['phone_service'].map(boolean_mapping)
    df_clean['paperless_billing'] = df_clean['paperless_billing'].map(boolean_mapping)
    
    # Churn target variable
    df_clean['churned'] = df_clean['churn'].map({'Yes': True, 'No': False})
    df_clean.drop('churn', axis=1, inplace=True)
    
    # 4. STANDARDIZE CATEGORICAL VALUES
    logger.info("Step 4: Standardizing categorical values...")
    
    # Gender
    df_clean['gender'] = df_clean['gender'].str.title()
    
    # Replace 'No internet service' and 'No phone service' with 'No' for consistency
    service_columns = ['online_security', 'online_backup', 'device_protection', 
                      'tech_support', 'streaming_tv', 'streaming_movies']
    for col in service_columns:
        df_clean[col] = df_clean[col].replace('No internet service', 'No')
    
    df_clean['multiple_lines'] = df_clean['multiple_lines'].replace('No phone service', 'No')
    
    # 5. CREATE DERIVED FIELDS
    logger.info("Step 5: Creating derived fields...")
    
    # Average monthly revenue (handle new customers with tenure=0)
    df_clean['avg_monthly_revenue'] = np.where(
        df_clean['tenure'] > 0,
        df_clean['total_charges'] / df_clean['tenure'],
        df_clean['monthly_charges']
    )
    
    # Customer segment based on tenure
    df_clean['customer_segment'] = pd.cut(
        df_clean['tenure'],
        bins=[0, 12, 36, 100],
        labels=['New', 'Growing', 'Loyal'],
        include_lowest=True
    )
    df_clean['customer_segment'] = df_clean['customer_segment'].astype(str)
    
    # 6. DATA QUALITY VALIDATION
    logger.info("Step 6: Validating data quality...")
    
    # Check for duplicates
    duplicates = df_clean['customer_id'].duplicated().sum()
    if duplicates > 0:
        logger.warning(f"Found {duplicates} duplicate customer_ids. Removing duplicates...")
        df_clean = df_clean.drop_duplicates(subset=['customer_id'], keep='first')
    
    # Check for invalid values
    invalid_tenure = (df_clean['tenure'] < 0).sum()
    invalid_charges = (df_clean['monthly_charges'] < 0).sum()
    
    if invalid_tenure > 0:
        logger.warning(f"Found {invalid_tenure} records with negative tenure. Setting to 0...")
        df_clean.loc[df_clean['tenure'] < 0, 'tenure'] = 0
    
    if invalid_charges > 0:
        logger.warning(f"Found {invalid_charges} records with negative charges. Setting to 0...")
        df_clean.loc[df_clean['monthly_charges'] < 0, 'monthly_charges'] = 0
    
    # 7. FINAL VALIDATION
    final_rows = len(df_clean)
    rows_removed = initial_rows - final_rows
    
    logger.info("=" * 60)
    logger.info("TRANSFORMATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Initial rows: {initial_rows:,}")
    logger.info(f"Final rows: {final_rows:,}")
    logger.info(f"Rows removed: {rows_removed:,}")
    logger.info(f"Null values: {df_clean.isnull().sum().sum()}")
    logger.info(f"Duplicate customer_ids: {df_clean['customer_id'].duplicated().sum()}")
    logger.info("=" * 60)
    
    # Log sample data
    logger.info("\nSample of transformed data:")
    logger.info(df_clean.head(3).to_string())
    
    return df_clean


def create_silver_table(engine):
    """
    Create silver layer staging table
    
    Args:
        engine: SQLAlchemy engine
    """
    logger.info("Creating silver layer table...")
    
    sql_file = "/opt/pipeline/sql/silver_staging.sql"
    
    with open(sql_file, 'r') as f:
        sql_script = f.read()
    
    with engine.begin() as conn:  # Use begin() for automatic commit
        # Execute the SQL script
        conn.execute(text(sql_script))
    
    logger.info("✓ Silver layer table created successfully")


def load_to_silver(df, engine):
    """
    Load transformed data to silver layer in PostgreSQL
    
    Args:
        df (pd.DataFrame): Cleaned dataframe
        engine: SQLAlchemy engine
    """
    logger.info("Loading data to silver layer...")
    
    # Add ingestion timestamp
    df['ingestion_timestamp'] = datetime.now()
    
    # Define the column order matching the table schema
    column_order = [
        'customer_id', 'gender', 'senior_citizen', 'partner', 'dependents',
        'tenure', 'phone_service', 'multiple_lines', 'internet_service',
        'online_security', 'online_backup', 'device_protection', 'tech_support',
        'streaming_tv', 'streaming_movies', 'contract_type', 'paperless_billing',
        'payment_method', 'monthly_charges', 'total_charges', 'avg_monthly_revenue',
        'customer_segment', 'churned', 'ingestion_timestamp'
    ]
    
    df = df[column_order]
    
    # Load data using bulk insert
    try:
        df.to_sql(
            name='customers_staging',
            schema='silver',
            con=engine,
            if_exists='replace',  # For simplicity, replace existing data
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"✓ Successfully loaded {len(df):,} records to silver.customers_staging")
        
    except Exception as e:
        logger.error(f"Failed to load data: {str(e)}")
        raise


def validate_silver_data(engine):
    """
    Validate data in silver layer
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        dict: Validation results
    """
    logger.info("Validating silver layer data...")
    
    validation_queries = {
        'total_records': "SELECT COUNT(*) as count FROM silver.customers_staging",
        'unique_customers': "SELECT COUNT(DISTINCT customer_id) as count FROM silver.customers_staging",
        'churned_count': "SELECT COUNT(*) as count FROM silver.customers_staging WHERE churned = TRUE",
        'null_check': "SELECT COUNT(*) as count FROM silver.customers_staging WHERE customer_id IS NULL",
        'segment_distribution': """
            SELECT customer_segment, COUNT(*) as count 
            FROM silver.customers_staging 
            GROUP BY customer_segment 
            ORDER BY customer_segment
        """
    }
    
    results = {}
    
    with engine.connect() as conn:
        for key, query in validation_queries.items():
            result = conn.execute(text(query))
            if key == 'segment_distribution':
                results[key] = [dict(row._mapping) for row in result]
            else:
                results[key] = result.fetchone()[0]
    
    logger.info("=" * 60)
    logger.info("SILVER LAYER VALIDATION RESULTS")
    logger.info("=" * 60)
    logger.info(f"Total Records: {results['total_records']:,}")
    logger.info(f"Unique Customers: {results['unique_customers']:,}")
    logger.info(f"Churned Customers: {results['churned_count']:,}")
    logger.info(f"Churn Rate: {(results['churned_count']/results['total_records']*100):.2f}%")
    logger.info(f"Null customer_ids: {results['null_check']}")
    logger.info("\nSegment Distribution:")
    for segment in results['segment_distribution']:
        logger.info(f"  {segment['customer_segment']}: {segment['count']:,}")
    logger.info("=" * 60)
    
    # Save validation results
    validation_file = "/opt/pipeline/data/silver/validation_results.json"
    Path(validation_file).parent.mkdir(parents=True, exist_ok=True)
    
    with open(validation_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"Validation results saved to: {validation_file}")
    
    return results


def main():
    """Main execution function"""
    logger.info("Starting Silver Layer Transformation")
    logger.info("=" * 60)
    
    try:
        # Step 1: Read bronze data
        df_raw = read_bronze_data()
        
        # Step 2: Clean and transform
        df_clean = clean_and_transform(df_raw)
        
        # Step 3: Connect to database
        engine = get_db_connection()
        
        # Step 4: Create silver table
        create_silver_table(engine)
        
        # Step 5: Load to silver layer
        load_to_silver(df_clean, engine)
        
        # Step 6: Validate
        validation_results = validate_silver_data(engine)
        
        # Step 7: Success
        logger.info("=" * 60)
        logger.info("✓ Silver Layer Transformation Completed Successfully")
        logger.info(f"✓ Records Loaded: {validation_results['total_records']:,}")
        logger.info(f"✓ Churn Rate: {(validation_results['churned_count']/validation_results['total_records']*100):.2f}%")
        logger.info("=" * 60)
        
        engine.dispose()
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("✗ Silver Layer Transformation Failed")
        logger.error(f"✗ Error: {str(e)}")
        logger.error("=" * 60)
        raise


if __name__ == "__main__":
    main()