# Data Engineering Pipeline Assessment (Cron-Based Orchestration)

## Overview

This project demonstrates a production-ready data pipeline using **simple Python script orchestration** instead of Airflow. The pipeline ingests customer churn data, transforms it through multiple layers, and models it for analytics using a Medallion Architecture (Bronze → Silver → Gold).

**Dataset**: [Telco Customer Churn Dataset from Kaggle](https://www.kaggle.com/datasets/blastchar/telco-customer-churn)

**AI Tools Used**: AI assistance was used to help structure the project architecture, generate SQL transformation logic, and create documentation templates. All code was reviewed, tested, and validated to ensure production quality.

## Architecture

```
┌─────────────────┐
│  Kaggle API     │
│  (Data Source)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │  ← Raw data (CSV)
│  (Raw Storage)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │  ← Cleaned & standardized (PostgreSQL)
│  (Staging)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Gold Layer     │  ← Analytics-ready models (PostgreSQL)
│  (Analytics)    │
└─────────────────┘

Orchestrated by: orchestrator.py (Python script)
Can be scheduled with: Cron (optional)
```

## Technology Stack

- **Orchestration**: Python script (orchestrator.py) + optional cron
- **Database**: PostgreSQL 15
- **Language**: Python 3.11
- **Data Processing**: Pandas, SQLAlchemy
- **Data Source**: Kaggle API
- **Containerization**: Docker & Docker Compose

## Why Script-Based Orchestration?

**Advantages over Airflow:**
- ✅ **Simpler setup** - No webserver, scheduler, or metadata DB
- ✅ **Faster startup** - Containers start in seconds, not minutes
- ✅ **Less overhead** - Lower memory and CPU usage
- ✅ **Easier debugging** - Direct Python execution, simpler logs
- ✅ **Better for small pipelines** - No need for heavy orchestration

**When to use Airflow:**
- Complex dependencies between many tasks
- Need web UI for monitoring
- Multiple team members managing workflows
- Extensive retry/backfill requirements

## Prerequisites

1. **Docker Desktop** installed and running on your MacBook
   - Download from: https://www.docker.com/products/docker-desktop
   - Ensure Docker is running (check the whale icon in your menu bar)

2. **Kaggle API Credentials**
   - Create a Kaggle account at https://www.kaggle.com
   - Go to Account Settings → API → Create New API Token
   - This downloads `kaggle.json` to your Downloads folder
   - Move it to the project: `cp ~/Downloads/kaggle.json ./kaggle.json`

3. **Basic Requirements**
   - At least 2GB of free RAM (much less than Airflow!)
   - 1GB of free disk space
   - Terminal/Command Line access

## Project Structure

```
data-pipeline-assessment/
├── docker-compose.yml          # Container orchestration 
├── Dockerfile                  # Pipeline container image
├── README.md                   # This file
├── REPORT.md                   # Design decisions & architecture
├── requirements.txt            # Python dependencies 
├── kaggle.json                 # Kaggle API credentials (you provide)
├── orchestrator.py             # Main pipeline orchestrator
├── crontab                     # Optional: cron schedule configuration
├── pipeline.sh                 # Helper script for common operations
├── scripts/
│   ├── ingestion.py           # Bronze layer: Data ingestion
│   ├── transformation.py      # Silver layer: Data cleaning
│   └── modeling.py            # Gold layer: Analytics models
├── sql/
│   ├── init.sql               # Database initialization
│   ├── silver_staging.sql     # Silver layer table DDL
│   └── gold_models.sql        # Gold layer table DDL
├── data/
│   └── bronze/                # Raw data storage (created at runtime)
└── logs/                      # Pipeline logs (created at runtime)
```

## Quick Start (One Command)

1. **Clone or extract this repository**
2. **Add your Kaggle credentials** (see Prerequisites #2)
3. **Run the pipeline**:

```bash
docker-compose up --build
```

That's it! The pipeline will:
- Build all containers
- Initialize PostgreSQL database
- Automatically run the pipeline once
- Ingest → Transform → Model the data


## Detailed Setup Instructions

### Step 1: Prepare Kaggle Credentials

```bash
# Move your kaggle.json to the project root
cp ~/Downloads/kaggle.json ./kaggle.json

# Verify the file exists
ls -la kaggle.json
```

### Step 2: Start the Pipeline

```bash
# Build and start all containers
docker-compose up --build

# Or run in detached mode (background)
docker-compose up --build -d
```

**What happens:**
1. PostgreSQL starts and initializes schemas
2. Pipeline container builds with Python dependencies
3. Orchestrator script runs automatically:
   - Stage 1: Ingestion (downloads from Kaggle)
   - Stage 2: Transformation (cleans and loads to PostgreSQL)
   - Stage 3: Modeling (creates analytics tables)
4. Pipeline completes and containers stay running

### Step 3: View Results

**Check pipeline logs:**
```bash
docker-compose logs pipeline
# Or use the helper script:
./pipeline.sh logs
```

**Connect to database:**
```bash
docker-compose exec postgres psql -U airflow -d airflow
# Or use the helper script:
./pipeline.sh db
```

**Validate data:**
```bash
./pipeline.sh validate
```

## Running the Pipeline

### Initial Run (Automatic)

The pipeline runs automatically when you start the containers:
```bash
docker-compose up --build
```

### Manual Re-run

To run the pipeline again without restarting containers:
```bash
./pipeline.sh run
```

Or directly:
```bash
docker-compose exec pipeline python3 /opt/pipeline/orchestrator.py
```

### Scheduled Runs with Cron (Optional)

To set up recurring pipeline runs:

1. **Edit the `crontab` file** with your schedule:
   ```
   # Run daily at 2 AM
   0 2 * * * cd /opt/pipeline && python3 orchestrator.py >> logs/cron.log 2>&1
   ```

2. **Update Dockerfile** to enable cron:
   ```dockerfile
   # Add to Dockerfile after line 20:
   COPY crontab /etc/cron.d/pipeline-cron
   RUN chmod 0644 /etc/cron.d/pipeline-cron && \
       crontab /etc/cron.d/pipeline-cron
   ```

3. **Update docker-compose command** to run cron:
   ```yaml
   command: bash -c "cron && tail -f /opt/pipeline/logs/pipeline.log"
   ```

4. **Rebuild and restart**:
   ```bash
   docker-compose up --build
   ```

## Validation & Testing

### Verify Data at Each Layer

#### 1. Check Bronze Layer (Raw Files)

```bash
# List raw data files
ls -lh data/bronze/

# View first 10 rows
head -n 10 data/bronze/telco_customer_churn.csv
```

**Expected**: CSV file with ~7,000 rows of customer data.

#### 2. Check Silver Layer (Staging Database)

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

# Run validation queries
SELECT COUNT(*) FROM silver.customers_staging;
SELECT * FROM silver.customers_staging LIMIT 5;

# Check data quality
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(CASE WHEN churned = TRUE THEN 1 ELSE 0 END) as churned_count
FROM silver.customers_staging;

# Exit PostgreSQL
\q
```

**Expected**: 
- ~7,000 records
- All customer_ids are unique
- ~26% churn rate

#### 3. Check Gold Layer (Analytics Models)

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

# View churn summary
SELECT * FROM gold.churn_summary ORDER BY customer_segment;

# View revenue analysis
SELECT * FROM gold.revenue_analysis ORDER BY total_revenue DESC LIMIT 10;

# Exit
\q
```

**Expected**:
- `churn_summary`: Aggregated churn metrics by segment
- `revenue_analysis`: Revenue broken down by customer attributes

### Using Helper Script

The `pipeline.sh` script makes common operations easier:

```bash
# View all commands
./pipeline.sh help

# Check pipeline status
./pipeline.sh status

# Validate all layers
./pipeline.sh validate

# View logs
./pipeline.sh logs

# Manually run pipeline
./pipeline.sh run

# Connect to database
./pipeline.sh db
```

## Stopping the Pipeline

```bash
# Stop all containers
docker-compose down

# Stop and remove all data (clean slate)
docker-compose down -v

# Stop and remove everything including images
docker-compose down -v --rmi all
```

## Troubleshooting

### Issue: "kaggle.json not found"

```bash
# Verify the file is in the project root
ls -la kaggle.json

# If not, copy it again
cp ~/Downloads/kaggle.json ./kaggle.json

# Rebuild containers
docker-compose down
docker-compose up --build
```

### Issue: "Database connection failed"

```bash
# Verify PostgreSQL is running
docker-compose ps

# Test connection
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT 1;"

# Restart PostgreSQL
docker-compose restart postgres
```

### Issue: "Pipeline fails during execution"

```bash
# Check full logs
docker-compose logs pipeline

# Or use helper script
./pipeline.sh logs pipeline

# Run pipeline manually to see errors
docker-compose exec pipeline python3 /opt/pipeline/orchestrator.py
```

### Issue: "Port 5432 already in use"

```bash
# Find what's using port 5432
lsof -i :5432

# Kill the process or change the port in docker-compose.yml
# Change "5432:5432" to "5433:5432"
```

## Re-running the Pipeline

To re-run the entire pipeline:

```bash
# Method 1: Using helper script (recommended)
./pipeline.sh run

# Method 2: Direct execution
docker-compose exec pipeline python3 /opt/pipeline/orchestrator.py

# Method 3: Complete reset
docker-compose down -v          # Remove all data
docker-compose up --build       # Restart fresh
```

## Performance Characteristics

- **Container Startup**: ~5-10 seconds (much faster than Airflow!)
- **Ingestion**: ~30 seconds (downloading 1.5MB dataset)
- **Transformation**: ~45 seconds (cleaning 7,000 records)
- **Modeling**: ~30 seconds (creating analytics tables)
- **Total Pipeline**: ~2-3 minutes end-to-end

**Memory Usage:**
- PostgreSQL: ~50MB
- Pipeline Container: ~100MB
- **Total: ~150MB** (vs ~1GB+ with Airflow)

## Comparison: Script vs Airflow

| Feature | Script Orchestration | Airflow |
|---------|---------------------|---------|
| **Setup Time** | < 1 minute | 3-5 minutes |
| **Memory Usage** | ~150MB | ~1GB+ |
| **Startup Time** | 5-10 seconds | 30-60 seconds |
| **Complexity** | Simple Python script | Web UI + Scheduler + DB |
| **Best For** | Small pipelines, simple workflows | Complex DAGs, team collaboration |
| **Learning Curve** | Minimal | Moderate |
| **Monitoring** | Logs | Web UI + Logs |
| **Scheduling** | Cron or external trigger | Built-in scheduler |

## Extending to Cron Scheduling

If you want to run this pipeline on a schedule (e.g., daily at 2 AM):

**Option 1: System Cron (Mac/Linux)**
```bash
# Edit your crontab
crontab -e

# Add this line (adjust path to your project):
0 2 * * * cd /path/to/data-pipeline-assessment && docker-compose exec pipeline python3 /opt/pipeline/orchestrator.py >> logs/cron.log 2>&1
```

**Option 2: Container Cron (included in this project)**
See "Scheduled Runs with Cron" section above.

**Option 3: External Scheduler**
- Use a tool like Jenkins, GitHub Actions, or AWS EventBridge
- Call `docker-compose exec pipeline python3 /opt/pipeline/orchestrator.py`

## Data Quality & Validation

The pipeline includes automated data quality validations:

1. **Schema Validation**: Ensures all required columns exist
2. **Null Checks**: Validates critical fields are not null
3. **Type Validation**: Ensures numeric fields contain valid numbers
4. **Referential Integrity**: Checks customer_id uniqueness
5. **Business Logic**: Validates churn flag consistency

All validations are logged in `/opt/pipeline/logs/pipeline.log`

## Next Steps for Extension

See `REPORT.md` for detailed discussion on:
- Scaling to production environments
- Adding incremental loads
- Implementing data quality frameworks
- CI/CD integration
- Monitoring and alerting
- When to migrate to Airflow

## Support

For assessment questions, contact the hiring team.

For technical issues with this setup:
1. Check the Troubleshooting section above
2. Review logs: `docker-compose logs` or `./pipeline.sh logs`
3. Verify Docker Desktop is running properly
4. Use helper script: `./pipeline.sh help`

## License

This project is for assessment purposes only.

---

**Key Advantages of This Approach:**
- ✅ Simpler and faster than Airflow
- ✅ Perfect for small to medium pipelines
- ✅ Easy to understand and debug
- ✅ Low resource requirements
- ✅ Production-ready code quality
- ✅ Can scale to Airflow when needed
