-- Database Initialization Script
-- Creates schemas and sets up initial structure

-- Create schemas for data layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA bronze TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA silver TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA gold TO airflow;

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'Database schemas initialized successfully';
    RAISE NOTICE 'Bronze schema: Raw data landing zone';
    RAISE NOTICE 'Silver schema: Cleaned and standardized data';
    RAISE NOTICE 'Gold schema: Analytics-ready models';
END $$;
