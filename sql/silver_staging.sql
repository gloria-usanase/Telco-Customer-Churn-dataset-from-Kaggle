-- Silver Layer: Cleaned and Standardized Customer Data
-- This table stores validated, typed, and standardized customer information

DROP TABLE IF EXISTS silver.customers_staging CASCADE;

CREATE TABLE silver.customers_staging (
    -- Unique identifier
    customer_id VARCHAR(50) NOT NULL,
    
    -- Demographics
    gender VARCHAR(10),
    senior_citizen BOOLEAN,
    partner BOOLEAN,
    dependents BOOLEAN,
    
    -- Service tenure
    tenure INTEGER,
    
    -- Services subscribed
    phone_service BOOLEAN,
    multiple_lines VARCHAR(20),
    internet_service VARCHAR(20),
    online_security VARCHAR(20),
    online_backup VARCHAR(20),
    device_protection VARCHAR(20),
    tech_support VARCHAR(20),
    streaming_tv VARCHAR(20),
    streaming_movies VARCHAR(20),
    
    -- Contract & Billing
    contract_type VARCHAR(30),
    paperless_billing BOOLEAN,
    payment_method VARCHAR(50),
    monthly_charges NUMERIC(10,2),
    total_charges NUMERIC(12,2),
    
    -- Derived fields
    avg_monthly_revenue NUMERIC(10,2),
    customer_segment VARCHAR(20),
    
    -- Target variable
    churned BOOLEAN,
    
    -- Metadata
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT pk_customer_id PRIMARY KEY (customer_id),
    CONSTRAINT chk_tenure_positive CHECK (tenure >= 0),
    CONSTRAINT chk_monthly_charges_positive CHECK (monthly_charges >= 0),
    CONSTRAINT chk_total_charges_positive CHECK (total_charges >= 0)
);

-- Indexes for query performance
CREATE INDEX idx_churned ON silver.customers_staging(churned);
CREATE INDEX idx_customer_segment ON silver.customers_staging(customer_segment);
CREATE INDEX idx_segment_churn ON silver.customers_staging(customer_segment, churned);
CREATE INDEX idx_contract_type ON silver.customers_staging(contract_type);
CREATE INDEX idx_ingestion_timestamp ON silver.customers_staging(ingestion_timestamp);

-- Comments for documentation
COMMENT ON TABLE silver.customers_staging IS 'Cleaned and standardized customer data from telco churn dataset';
COMMENT ON COLUMN silver.customers_staging.customer_id IS 'Unique customer identifier';
COMMENT ON COLUMN silver.customers_staging.churned IS 'TRUE if customer has churned, FALSE otherwise';
COMMENT ON COLUMN silver.customers_staging.customer_segment IS 'Customer lifecycle segment: New, Growing, or Loyal';
COMMENT ON COLUMN silver.customers_staging.avg_monthly_revenue IS 'Average monthly revenue calculated from total charges and tenure';
