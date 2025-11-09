#!/usr/bin/env python3
"""
Gold Layer: Analytics Modeling
Creates business-ready analytical tables optimized for reporting and ML
"""

import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import json
from pathlib import Path

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


def build_gold_models(engine):
    """
    Execute SQL script to build gold layer analytics models
    
    Args:
        engine: SQLAlchemy engine
    """
    logger.info("Building gold layer analytics models...")
    
    sql_file = "/opt/pipeline/sql/gold_models.sql"
    
    logger.info(f"Reading SQL script from: {sql_file}")
    
    with open(sql_file, 'r') as f:
        sql_script = f.read()
    
    # Split script into individual statements (separated by semicolons)
    # This is a simple approach; for more complex scripts, use a proper SQL parser
    statements = [s.strip() for s in sql_script.split(';') if s.strip()]
    
    logger.info(f"Executing {len(statements)} SQL statements...")
    
    for i, statement in enumerate(statements, 1):
        try:
            # Skip empty statements and comments
            if not statement or statement.startswith('--'):
                continue
            
            logger.info(f"Executing statement {i}/{len(statements)}...")
            
            # Execute each statement in its own transaction
            with engine.begin() as conn:
                conn.execute(text(statement))
            
        except Exception as e:
            # Some statements might fail (like DROP TABLE IF NOT EXISTS)
            # Log but continue
            if "does not exist" not in str(e).lower() and "already exists" not in str(e).lower():
                logger.error(f"Statement {i} failed: {str(e)}")
                # Show the statement that failed
                logger.error(f"Failed statement: {statement[:200]}...")
                raise  # Re-raise the exception if it's a real error
            else:
                logger.debug(f"Statement {i} skipped (expected): {str(e)}")
    
    logger.info("✓ Gold layer models created successfully")


def validate_gold_models(engine):
    """
    Validate gold layer tables and retrieve key metrics
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        dict: Validation results with model metrics
    """
    logger.info("Validating gold layer models...")
    
    results = {}
    
    # 1. Validate churn_summary table
    logger.info("\n" + "=" * 60)
    logger.info("Model 1: Churn Summary")
    logger.info("=" * 60)
    
    query = "SELECT * FROM gold.churn_summary ORDER BY customer_segment"
    df_churn = pd.read_sql(query, engine)
    
    logger.info(f"Rows: {len(df_churn)}")
    logger.info("\nChurn Summary by Segment:")
    logger.info(df_churn.to_string(index=False))
    
    results['churn_summary'] = df_churn.to_dict('records')
    
    # 2. Validate revenue_analysis table
    logger.info("\n" + "=" * 60)
    logger.info("Model 2: Revenue Analysis")
    logger.info("=" * 60)
    
    query = "SELECT * FROM gold.revenue_analysis ORDER BY total_monthly_revenue DESC LIMIT 10"
    df_revenue = pd.read_sql(query, engine)
    
    logger.info(f"Rows: {len(df_revenue)}")
    logger.info("\nTop 10 Revenue Segments:")
    logger.info(df_revenue[['contract_type', 'payment_method', 'customer_count', 
                            'total_monthly_revenue', 'churn_rate_percent']].to_string(index=False))
    
    results['revenue_analysis_top10'] = df_revenue.to_dict('records')
    
    # 3. Validate service_churn_correlation table
    logger.info("\n" + "=" * 60)
    logger.info("Model 3: Service Churn Correlation")
    logger.info("=" * 60)
    
    query = "SELECT * FROM gold.service_churn_correlation ORDER BY churn_rate_percent DESC LIMIT 10"
    df_service = pd.read_sql(query, engine)
    
    logger.info(f"Rows: {len(df_service)}")
    logger.info("\nHighest Churn Service Combinations:")
    logger.info(df_service[['internet_service', 'online_security', 'tech_support', 
                            'customer_count', 'churn_rate_percent']].to_string(index=False))
    
    results['service_churn_top10'] = df_service.to_dict('records')
    
    # 4. Executive Summary View
    logger.info("\n" + "=" * 60)
    logger.info("Executive Summary")
    logger.info("=" * 60)
    
    query = "SELECT * FROM gold.executive_summary"
    df_exec = pd.read_sql(query, engine)
    
    exec_summary = df_exec.iloc[0].to_dict()
    
    logger.info(f"Total Customers: {exec_summary['total_customers']:,}")
    logger.info(f"Total Churned: {exec_summary['total_churned']:,}")
    logger.info(f"Overall Churn Rate: {exec_summary['overall_churn_rate']:.2f}%")
    logger.info(f"Total Monthly Revenue: ${exec_summary['total_monthly_revenue']:,.2f}")
    logger.info(f"At-Risk Revenue: ${exec_summary['at_risk_revenue']:,.2f}")
    logger.info(f"Avg Revenue per Customer: ${exec_summary['avg_revenue_per_customer']:,.2f}")
    logger.info(f"Avg Customer Tenure: {exec_summary['avg_customer_tenure']:.1f} months")
    
    results['executive_summary'] = exec_summary
    
    # 5. Table counts
    table_count_queries = {
        'churn_summary': "SELECT COUNT(*) as count FROM gold.churn_summary",
        'revenue_analysis': "SELECT COUNT(*) as count FROM gold.revenue_analysis",
        'service_churn_correlation': "SELECT COUNT(*) as count FROM gold.service_churn_correlation"
    }
    
    table_counts = {}
    with engine.connect() as conn:
        for table, query in table_count_queries.items():
            result = conn.execute(text(query))
            table_counts[table] = result.fetchone()[0]
    
    logger.info("\n" + "=" * 60)
    logger.info("Table Record Counts")
    logger.info("=" * 60)
    for table, count in table_counts.items():
        logger.info(f"{table}: {count:,} records")
    
    results['table_counts'] = table_counts
    
    return results


def generate_insights(results):
    """
    Generate business insights from gold layer analytics
    
    Args:
        results (dict): Validation results from gold models
    """
    logger.info("\n" + "=" * 60)
    logger.info("KEY BUSINESS INSIGHTS")
    logger.info("=" * 60)
    
    exec_summary = results['executive_summary']
    churn_summary = results['churn_summary']
    
    # Insight 1: Overall Health
    logger.info("\n1. OVERALL BUSINESS HEALTH")
    logger.info(f"   • {exec_summary['total_customers']:,} active customers")
    logger.info(f"   • {exec_summary['overall_churn_rate']:.2f}% churn rate")
    logger.info(f"   • ${exec_summary['total_monthly_revenue']:,.2f} monthly recurring revenue")
    logger.info(f"   • ${exec_summary['at_risk_revenue']:,.2f} at risk from churned customers")
    
    # Insight 2: Segment Performance
    logger.info("\n2. CUSTOMER SEGMENT PERFORMANCE")
    for segment in churn_summary:
        logger.info(f"   • {segment['customer_segment']} Customers:")
        logger.info(f"     - Count: {segment['total_customers']:,}")
        logger.info(f"     - Churn Rate: {segment['churn_rate_percent']:.2f}%")
        logger.info(f"     - Avg Tenure: {segment['avg_tenure_months']:.1f} months")
        logger.info(f"     - Avg Monthly Charges: ${segment['avg_monthly_charges']:.2f}")
    
    # Insight 3: Revenue at Risk
    at_risk_pct = (exec_summary['at_risk_revenue'] / exec_summary['total_monthly_revenue']) * 100
    logger.info(f"\n3. REVENUE RISK")
    logger.info(f"   • {at_risk_pct:.1f}% of monthly revenue is from churned customers")
    logger.info(f"   • Annual revenue impact: ${exec_summary['at_risk_revenue'] * 12:,.2f}")
    
    # Insight 4: Action Items
    logger.info("\n4. RECOMMENDED ACTIONS")
    
    # Find highest churn segment
    highest_churn_segment = max(churn_summary, key=lambda x: x['churn_rate_percent'])
    logger.info(f"   • Priority: Focus on {highest_churn_segment['customer_segment']} segment")
    logger.info(f"     (Churn rate: {highest_churn_segment['churn_rate_percent']:.2f}%)")
    
    # Revenue optimization
    if results['revenue_analysis_top10']:
        top_revenue = results['revenue_analysis_top10'][0]
        logger.info(f"   • Protect high-value segment: {top_revenue['contract_type']} customers")
        logger.info(f"     (Revenue: ${top_revenue['total_monthly_revenue']:,.2f}/month)")
    
    logger.info("=" * 60)


def save_results(results):
    """
    Save validation results and insights to file
    
    Args:
        results (dict): Validation results
    """
    output_dir = Path("/opt/pipeline/data/gold")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / "validation_results.json"
    
    # Convert any non-serializable objects
    serializable_results = json.loads(json.dumps(results, default=str))
    
    with open(output_file, 'w') as f:
        json.dump(serializable_results, f, indent=2)
    
    logger.info(f"\n✓ Results saved to: {output_file}")


def main():
    """Main execution function"""
    logger.info("Starting Gold Layer Modeling")
    logger.info("=" * 60)
    
    try:
        # Step 1: Connect to database
        engine = get_db_connection()
        
        # Step 2: Build gold layer models
        build_gold_models(engine)
        
        # Step 3: Validate models
        results = validate_gold_models(engine)
        
        # Step 4: Generate insights
        generate_insights(results)
        
        # Step 5: Save results
        save_results(results)
        
        # Step 6: Success
        logger.info("\n" + "=" * 60)
        logger.info("✓ Gold Layer Modeling Completed Successfully")
        logger.info("✓ Analytics models are ready for consumption")
        logger.info("✓ Models: churn_summary, revenue_analysis, service_churn_correlation")
        logger.info("✓ Views: executive_summary")
        logger.info("=" * 60)
        
        engine.dispose()
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("✗ Gold Layer Modeling Failed")
        logger.error(f"✗ Error: {str(e)}")
        logger.error("=" * 60)
        raise


if __name__ == "__main__":
    main()