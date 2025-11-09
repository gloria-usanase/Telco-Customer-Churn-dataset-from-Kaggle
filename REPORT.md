# Architecture Overview

This data architecture implements a **medallion-style pipeline** (Bronze → Silver → Gold) to move data from **Kaggle** into **analytics-ready PostgreSQL models**, fully automated by a Python orchestrator.

The design cleanly separates concerns:

- **Ingestion** (from Kaggle)
- **Raw storage** (Bronze)
- **Cleansing & standardization** (Silver)
- **Business-ready transformations & analytics** (Gold)
- **Automation & scheduling** (via `orchestrator.py`)

This structure improves **traceability**, **reproducibility**, **data quality**, and **maintainability** of the entire pipeline.

---

## Components

### 1. Kaggle API (Data Source)

- Acts as the **external data provider**.  
- Datasets are pulled directly via **Kaggle’s API** (e.g., using the `kaggle` CLI or Python SDK).  
- The API calls are parameterized by:
  - Dataset name
  - File paths
  - Versioning / update frequency  
- Ensures a **repeatable and scriptable** way of retrieving the latest data.

---

### 2. Bronze Layer – Raw Storage (CSV)

**Purpose:** Immutable landing zone for all ingested data.

- Stores **raw CSV files** exactly as downloaded from Kaggle.  
- No transformations are applied besides minimal technical handling (e.g., file naming, partitioning, basic validation).  
- **Key characteristics:**
  - *Append-only:* keeps historical versions for audit and reproducibility.  
  - *Schema-on-read:* no strict schema enforcement.  
  - *Single source of truth* for all ingested data.  
- **Benefits:**
  - Enables complete reprocessing if downstream logic changes.  
  - Protects against accidental data loss or corruption in later layers.  

---

### 3. Silver Layer – Staging (PostgreSQL)

**Purpose:** Cleaned and standardized version of the raw data.

- Data from the Bronze CSV files is **loaded into PostgreSQL** staging tables.  
- **Typical transformations include:**
  - Data type casting (e.g., strings → dates or numbers)  
  - Handling missing or inconsistent values  
  - Normalizing categorical values (e.g., consistent case or encoding)  
  - Deduplication and integrity checks  
- **Design principles:**
  - Maintains a one-to-one or near-source mapping with improved data quality.  
  - Still avoids embedding complex business rules.  
- **Benefits:**
  - Provides a *clean and consistent* dataset for analytics.  
  - Separates data cleaning logic from business transformations.  

---

### 4. Gold Layer – Analytics (PostgreSQL)

**Purpose:** Business-ready, analytics-optimized data models.

- Built on top of the Silver layer using SQL transformations.  
- **Contents:**
  - Aggregated tables (e.g., by category, time, or region)  
  - Feature tables for ML models  
  - Dimensional models (fact and dimension tables) for BI/reporting  
- **Transformations include:**
  - Business rules and KPIs  
  - Joins and relationships between entities  
  - Metric definitions  
- **Characteristics:**
  - Stable schemas designed for *dashboards, reports, and data science*.  
  - Performance-optimized through indexing, partitioning, and pre-aggregation.  
- **Benefits:**
  - Allows analysts and BI tools to query data directly.  
  - Ensures *consistency and standardization* of business metrics.  

---

## Orchestration – `orchestrator.py`

**Purpose:** End-to-end automation and control of the data pipeline.

`orchestrator.py` performs the following functions:

1. **Ingestion**
   - Calls the Kaggle API to download datasets.  
   - Stores raw files into the Bronze layer (organized by dataset and date).  

2. **Load to Silver**
   - Parses Bronze CSV files.  
   - Cleans and validates data.  
   - Loads into PostgreSQL staging tables.  

3. **Transform to Gold**
   - Executes SQL scripts to build analytics-ready models.  

4. **Monitoring & Logging**
   - Tracks pipeline execution (start/end time, row counts, errors).  
   - Enables observability for debugging and reporting.  

5. **Scheduling**
   - Can be triggered via *cron*, an external scheduler, or CI/CD pipeline.  
   - Ensures data refreshes on a defined schedule (e.g., daily, weekly).  

**Benefits:**

- Centralized configuration (dataset IDs, paths, credentials).  
- Encapsulates the entire lifecycle in a single script.  
- Extensible to include validation checks, alerting, and versioning.  

---

## Why This Design Works

- **Separation of Concerns:** Each layer has a distinct purpose.  
- **Reproducibility:** Every Gold model can be traced back to its Bronze source.  
- **Maintainability:** Cleaning, staging, and business logic are modularized.  
- **Scalability:** Easily extendable to multiple datasets or sources.  
- **Reliability:** The orchestrator ensures consistent, automated execution.

---

## Architecture Diagram

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