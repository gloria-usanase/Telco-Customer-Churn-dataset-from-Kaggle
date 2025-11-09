-- Gold Layer: Analytics-Ready Models
-- Pre-aggregated tables optimized for business intelligence and reporting

-- Model 1: Churn Summary by Customer Segment
DROP TABLE IF EXISTS gold.churn_summary CASCADE;

CREATE TABLE gold.churn_summary AS
SELECT 
    customer_segment,
    COUNT(*) as total_customers,
    SUM(CASE WHEN churned THEN 1 ELSE 0 END) as churned_customers,
    SUM(CASE WHEN NOT churned THEN 1 ELSE 0 END) as retained_customers,
    ROUND(CAST(AVG(CASE WHEN churned THEN 1.0 ELSE 0.0 END) * 100 AS numeric), 2) as churn_rate_percent,
    ROUND(CAST(AVG(monthly_charges) AS numeric), 2) as avg_monthly_charges,
    ROUND(CAST(AVG(total_charges) AS numeric), 2) as avg_total_charges,
    ROUND(CAST(AVG(tenure) AS numeric), 1) as avg_tenure_months,
    ROUND(CAST(SUM(monthly_charges) AS numeric), 2) as total_monthly_revenue,
    CURRENT_TIMESTAMP as calculated_at
FROM silver.customers_staging
GROUP BY customer_segment
ORDER BY customer_segment;

-- Add primary key and indexes
ALTER TABLE gold.churn_summary ADD CONSTRAINT pk_churn_summary PRIMARY KEY (customer_segment);
CREATE INDEX idx_churn_rate ON gold.churn_summary(churn_rate_percent);

-- Add comments
COMMENT ON TABLE gold.churn_summary IS 'Aggregated churn metrics by customer lifecycle segment';
COMMENT ON COLUMN gold.churn_summary.customer_segment IS 'New (0-12 months), Growing (12-36 months), Loyal (36+ months)';
COMMENT ON COLUMN gold.churn_summary.churn_rate_percent IS 'Percentage of customers who churned in this segment';


-- Model 2: Revenue Analysis by Contract and Payment Method
DROP TABLE IF EXISTS gold.revenue_analysis CASCADE;

CREATE TABLE gold.revenue_analysis AS
SELECT 
    contract_type,
    payment_method,
    COUNT(*) as customer_count,
    SUM(CASE WHEN churned THEN 1 ELSE 0 END) as churned_count,
    ROUND(CAST(AVG(CASE WHEN churned THEN 1.0 ELSE 0.0 END) * 100 AS numeric), 2) as churn_rate_percent,
    ROUND(CAST(SUM(monthly_charges) AS numeric), 2) as total_monthly_revenue,
    ROUND(CAST(AVG(monthly_charges) AS numeric), 2) as avg_revenue_per_customer,
    ROUND(CAST(SUM(total_charges) AS numeric), 2) as total_lifetime_revenue,
    ROUND(CAST(SUM(CASE WHEN churned THEN monthly_charges ELSE 0 END) AS numeric), 2) as at_risk_monthly_revenue,
    ROUND(CAST(SUM(CASE WHEN churned THEN total_charges ELSE 0 END) AS numeric), 2) as lost_lifetime_revenue,
    CURRENT_TIMESTAMP as calculated_at
FROM silver.customers_staging
GROUP BY contract_type, payment_method
ORDER BY total_monthly_revenue DESC;

-- Add composite primary key and indexes
ALTER TABLE gold.revenue_analysis 
ADD CONSTRAINT pk_revenue_analysis PRIMARY KEY (contract_type, payment_method);
CREATE INDEX idx_revenue_churn ON gold.revenue_analysis(churn_rate_percent);
CREATE INDEX idx_total_revenue ON gold.revenue_analysis(total_monthly_revenue);

-- Add comments
COMMENT ON TABLE gold.revenue_analysis IS 'Revenue breakdown by contract type and payment method with churn impact';
COMMENT ON COLUMN gold.revenue_analysis.at_risk_monthly_revenue IS 'Monthly revenue from customers who have churned';
COMMENT ON COLUMN gold.revenue_analysis.lost_lifetime_revenue IS 'Total lifetime value lost to churn';


-- Model 3: Service Adoption and Churn Correlation
DROP TABLE IF EXISTS gold.service_churn_correlation CASCADE;

CREATE TABLE gold.service_churn_correlation AS
SELECT 
    internet_service,
    online_security,
    tech_support,
    COUNT(*) as customer_count,
    SUM(CASE WHEN churned THEN 1 ELSE 0 END) as churned_count,
    ROUND(CAST(AVG(CASE WHEN churned THEN 1.0 ELSE 0.0 END) * 100 AS numeric), 2) as churn_rate_percent,
    ROUND(CAST(AVG(monthly_charges) AS numeric), 2) as avg_monthly_charges,
    ROUND(CAST(AVG(tenure) AS numeric), 1) as avg_tenure_months
FROM silver.customers_staging
WHERE internet_service IS NOT NULL
GROUP BY internet_service, online_security, tech_support
HAVING COUNT(*) >= 10  -- Only include segments with meaningful sample size
ORDER BY churn_rate_percent DESC;

-- Add indexes
CREATE INDEX idx_service_churn ON gold.service_churn_correlation(churn_rate_percent);

-- Add comments
COMMENT ON TABLE gold.service_churn_correlation IS 'Churn rates by service bundle combinations';
COMMENT ON COLUMN gold.service_churn_correlation.churn_rate_percent IS 'Churn rate for this specific service combination';


-- Create a summary view for quick dashboard queries
CREATE OR REPLACE VIEW gold.executive_summary AS
SELECT 
    COUNT(*) as total_customers,
    SUM(CASE WHEN churned THEN 1 ELSE 0 END) as total_churned,
    ROUND(CAST(AVG(CASE WHEN churned THEN 1.0 ELSE 0.0 END) * 100 AS numeric), 2) as overall_churn_rate,
    ROUND(CAST(SUM(monthly_charges) AS numeric), 2) as total_monthly_revenue,
    ROUND(CAST(SUM(CASE WHEN churned THEN monthly_charges ELSE 0 END) AS numeric), 2) as at_risk_revenue,
    ROUND(CAST(AVG(monthly_charges) AS numeric), 2) as avg_revenue_per_customer,
    ROUND(CAST(AVG(tenure) AS numeric), 1) as avg_customer_tenure,
    CURRENT_TIMESTAMP as report_timestamp
FROM silver.customers_staging;

COMMENT ON VIEW gold.executive_summary IS 'High-level KPIs for executive dashboard';


-- Grant permissions
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO airflow;
GRANT SELECT ON gold.executive_summary TO airflow;