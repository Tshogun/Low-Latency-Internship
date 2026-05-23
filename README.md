# Healthcare Analytics Pipeline on GCP

A cloud-native, event-driven healthcare analytics platform built on Google Cloud Platform (GCP) for ingesting, processing, transforming, deduplicating, and analyzing healthcare data at scale.

The system combines data lake architecture, metadata-driven processing, serverless compute, automated deduplication, and BI dashboards into a single end-to-end pipeline.

---

# Problem Statement

Healthcare organizations generate large volumes of semi-structured data from multiple systems such as patient registration, lab systems, EMRs, and operational workflows.

This data often suffers from:

- Duplicate records
- Inconsistent schemas
- Poor data quality
- Lack of centralized analytics
- Manual processing overhead
- Difficulty scaling ingestion pipelines

The goal of this project was to build a scalable and maintainable cloud-native data platform capable of:

- Ingesting healthcare datasets in near real-time
- Automatically validating and transforming incoming data
- Storing optimized analytical formats (Parquet)
- Loading data into a warehouse for querying
- Performing automated deduplication
- Supporting metadata-driven schema evolution
- Enabling analytics dashboards for business users
- Allowing non-technical users to manage schema updates through a UI

---

# Project Overview

This project implements an event-driven ETL/ELT architecture using:

- Google Cloud Storage
- Eventarc
- Cloud Run
- FastAPI
- Pandas + PyArrow
- BigQuery
- Cloud Functions
- Cloud Scheduler
- Looker Studio
- Nuxt.js Metadata Management UI

The pipeline is designed to be:

- Serverless
- Metadata-driven
- Scalable
- Extensible
- Production-oriented

---

# Architecture

```text
                ┌──────────────────────────┐
                │  API / Manual Upload     │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │  GCS Raw Data Lake       │
                │  raw/...                 │
                └────────────┬─────────────┘
                             │
                    Eventarc Trigger
                             │
                             ▼
                ┌──────────────────────────┐
                │ Cloud Run Processor      │
                │ FastAPI + Pandas         │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │ Processed Parquet Layer  │
                │ processed/...            │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │ BigQuery Raw Tables      │
                │ *_raw                    │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │ Cloud Function           │
                │ Deduplication Engine     │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │ BigQuery Latest Tables   │
                │ *_latest                 │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │ Looker Studio Dashboard  │
                └──────────────────────────┘
```

---

# GCP Services Used

| Service | Purpose |
|---|---|
| Cloud Storage | Raw and processed data lake |
| Eventarc | Event-driven orchestration |
| Cloud Run | Data processing service |
| BigQuery | Analytical warehouse |
| Cloud Functions | Deduplication processing |
| Cloud Scheduler | Automated orchestration |
| Looker Studio | Visualization layer |

---

# Project Structure

```text
healthcare-pipeline/
│
├── processor/
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── dedup-function/
│   ├── main.py
│   └── requirements.txt
│
├── metadata/
│   ├── registry.json
│   └── patient/
│       └── v1.json
│
├── metadata-ui/
│   └── Nuxt.js application
│
└── README.md
```

---

# Cloud Storage Layout

Bucket:

```text
pipeline-data-lake-hlc
```

## Raw Layer

```text
raw/patient/YYYY/YYYY-MM/*.json
```

## Processed Layer

```text
processed/patient/YYYY/YYYY-MM/*.parquet
```

---

# Processing Pipeline

## Step 1 — Data Upload

Healthcare datasets are uploaded manually or through APIs into:

```text
gs://pipeline-data-lake-hlc/raw/
```

Supported formats:

- JSON
- CSV

---

## Step 2 — Eventarc Trigger

Every file upload event automatically triggers the processing pipeline through Eventarc.

---

## Step 3 — Cloud Run Processing

Service:

```text
healthcare-processor
```

Runtime:

```text
Python + FastAPI
```

The processor performs:

- File ingestion
- Schema normalization
- Data validation
- Deduplication
- Data enrichment
- Parquet conversion
- BigQuery loading

---

# Data Cleaning & Enrichment

The processor standardizes incoming healthcare data using Pandas transformations.

## Cleaning

- Standardized column names
- Duplicate removal
- Datatype normalization
- Timestamp parsing
- Name formatting

## Enrichment

Additional derived fields:

| Field | Description |
|---|---|
| processed_timestamp | ETL processing timestamp |
| age | Calculated from date_of_birth |
| quality_score | Data quality metric |
| source_file | Source object path |

---

# BigQuery Warehouse

Dataset:

```text
healthcare_analytics
```

## Raw Tables

Example:

```text
patient_v1_raw
```

Raw processed Parquet files are loaded into BigQuery.

---

# Metadata-Driven Architecture

One of the core design goals of the project was reducing hardcoded pipeline logic.

Schemas, primary keys, partitioning, clustering, and deduplication rules are managed dynamically using metadata JSON files.

---

# Registry Example

```json
{
  "datasets": {
    "patient": {
      "active_version": "v1",
      "schema_path": "metadata/patient/v1.json"
    }
  }
}
```

---

# Dataset Schema Example

```json
{
  "dataset": "patient",
  "version": "v1",
  "primary_key": ["patient_id"],
  "deduplication": {
    "order_by": "created_at"
  },
  "bigquery": {
    "partition_field": "created_at",
    "cluster_fields": ["patient_id"]
  },
  "fields": {
    "patient_id": "string",
    "first_name": "string",
    "last_name": "string",
    "gender": "string",
    "date_of_birth": "date",
    "created_at": "timestamp"
  }
}
```

---

# Metadata Management UI (Nuxt.js)

To make the platform usable for non-technical users, a metadata management UI was built using Nuxt.js.

The UI allows users to:

- Create new dataset schemas
- Update field definitions
- Configure primary keys
- Define deduplication logic
- Configure BigQuery partitioning/clustering
- Manage schema versions
- Update registry mappings

This removes the need for manually editing JSON files inside GCS and makes the platform more accessible to analysts and operations teams.

---

# Deduplication Pipeline

A dedicated Cloud Function performs scheduled deduplication.

The function:

- Reads metadata definitions
- Dynamically generates SQL
- Applies deduplication logic
- Creates analytics-ready tables

---

# Deduplication Logic

```sql
ROW_NUMBER() OVER (
    PARTITION BY patient_id
    ORDER BY created_at DESC
)
```

The latest record per patient is retained.

---

# Output Tables

Example:

```text
patient_v1_latest
```

These tables are used directly for analytics and dashboards.

---

# Scheduling

Cloud Scheduler triggers the deduplication function daily.

Example:

```bash
gcloud scheduler jobs create http dedup-daily \
  --schedule="0 2 * * *" \
  --uri="https://us-central1-gcp-data-piepline.cloudfunctions.net/dedup-runner" \
  --http-method=GET \
  --time-zone="Asia/Kolkata"
```

---

# Analytics Layer

Looker Studio connects directly to:

```text
patient_v1_latest
```

Dashboards include:

- Total patient count
- Gender distribution
- Age distribution
- Data quality trends
- Record growth over time
- Latest patient activity

---

# Example Queries

## Raw Data

```sql
SELECT *
FROM `gcp-data-piepline.healthcare_analytics.patient_v1_raw`
LIMIT 100;
```

---

## Deduplicated Data

```sql
SELECT *
FROM `gcp-data-piepline.healthcare_analytics.patient_v1_latest`
LIMIT 100;
```

---

# Deployment

## Deploy Cloud Run Processor

```bash
gcloud run deploy healthcare-processor \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

---

## Deploy Deduplication Function

```bash
gcloud functions deploy dedup-runner \
  --runtime python311 \
  --trigger-http \
  --entry-point run_dedup \
  --allow-unauthenticated \
  --region us-central1
```

---

# IAM Roles Used

Key IAM permissions:

- BigQuery Data Editor
- BigQuery Job User
- Storage Admin
- Cloud Build Builder
- Logging Writer

---

# Future Improvements

Potential next steps:

- Incremental MERGE-based deduplication
- Data quality monitoring dashboards
- CI/CD pipelines
- Terraform infrastructure provisioning
- Schema evolution workflows
- Data lineage tracking
- Real-time streaming ingestion
- Monitoring and alerting

---

# Key Engineering Concepts Demonstrated

- Event-driven architecture
- Serverless data engineering
- Metadata-driven pipelines
- Data lake + warehouse architecture
- Automated orchestration
- Cloud-native ETL
- Parquet optimization
- BigQuery analytics
- Self-service schema management

---

# Tech Stack

| Category | Technology |
|---|---|
| Backend | Python, FastAPI |
| Data Processing | Pandas, PyArrow |
| Frontend | Nuxt.js |
| Storage | Google Cloud Storage |
| Compute | Cloud Run |
| Warehouse | BigQuery |
| Deduplication | Cloud Functions |
| Scheduling | Cloud Scheduler |
| Visualization | Looker Studio |

---

# Learning Outcomes

This project demonstrates practical implementation of:

- Modern ETL/ELT pipelines
- Event-driven cloud systems
- Data lake architecture
- Metadata-driven engineering
- BigQuery optimization
- Analytical dashboarding
- Production-grade GCP workflows
