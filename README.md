# 🏥 Healthcare Analytics Pipeline on GCP

A cloud-native, event-driven healthcare data pipeline built on Google Cloud Platform (GCP) that ingests healthcare data, processes and transforms it into Parquet format, loads it into BigQuery, performs automated deduplication, and visualizes analytics using Looker Studio.

---

# 🚀 Project Overview

This project demonstrates a modern data engineering architecture using:

- Google Cloud Storage (Data Lake)
- Eventarc
- Cloud Run
- FastAPI
- Pandas + PyArrow
- BigQuery
- Cloud Functions
- Cloud Scheduler
- Looker Studio

The pipeline is metadata-driven, scalable, event-triggered, and production-oriented.

---

# 🏗️ Architecture

```text
API / Manual Upload
        ↓
GCS Bucket (raw/)
        ↓
Eventarc Trigger
        ↓
Cloud Run (FastAPI Processor)
        ↓
GCS Bucket (processed/ Parquet)
        ↓
BigQuery Raw Tables
        ↓
Cloud Function (Deduplication)
        ↓
BigQuery Latest Tables
        ↓
Looker Studio Dashboard
```

---

# ☁️ GCP Services Used

| Service | Purpose |
|---|---|
| Cloud Storage | Raw + Processed data lake |
| Eventarc | Event-driven triggers |
| Cloud Run | File processing service |
| BigQuery | Data warehouse |
| Cloud Functions | Automated deduplication |
| Cloud Scheduler | Scheduled orchestration |
| Looker Studio | Analytics dashboard |

---

# 📂 Project Structure

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
└── README.md
```

---

# 📦 Cloud Storage Layout

Bucket Name:

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

# ⚙️ Processing Pipeline

## Step 1 — Upload Raw Data

Data is uploaded via API or manually into:

```text
gs://pipeline-data-lake-hlc/raw/
```

Supported formats:

- JSON
- CSV

---

## Step 2 — Eventarc Trigger

A new object upload triggers Eventarc.

Eventarc invokes the Cloud Run service automatically.

---

## Step 3 — Cloud Run Processing

Service Name:

```text
healthcare-processor
```

Region:

```text
us-central1
```

Runtime:

```text
Python + FastAPI
```

### Responsibilities

- Read raw files from GCS
- Convert to Pandas DataFrame
- Standardize column names
- Deduplicate records using `patient_id`
- Clean and normalize fields
- Calculate age
- Add processing metadata
- Convert data to Parquet
- Upload Parquet back to GCS
- Load data into BigQuery

---

# 🧹 Data Cleaning & Enrichment

The processor performs:

## Cleaning

- Normalize column names
- Remove duplicates
- Standardize names
- Convert timestamps
- Convert date fields

## Enrichment

Additional fields generated:

| Field | Description |
|---|---|
| processed_timestamp | Processing time |
| age | Calculated from DOB |
| quality_score | Basic data quality metric |
| source_file | Original source file |

---

# 🪣 BigQuery Layer

Dataset:

```text
healthcare_analytics
```

## Raw Table

```text
patient_v1_raw
```

Raw data from Parquet files is loaded into BigQuery.

---

# 🧠 Metadata-Driven Architecture

Schemas are managed dynamically using metadata.

## registry.json

Tracks datasets and active schema versions.

Example:

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

## Schema Definition Example

```json
{
  "dataset": "patient",
  "version": "v1",
  "primary_key": ["patient_id"],
  "deduplication": {
    "order_by": "created_at"
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

# 🔁 Deduplication Pipeline

Deduplication is implemented using:

- Cloud Function
- BigQuery SQL
- Metadata-driven logic

## Dedup Logic

Uses:

```sql
ROW_NUMBER() OVER (
    PARTITION BY patient_id
    ORDER BY created_at DESC
)
```

Keeps only the latest record.

---

## Output Table

```text
patient_v1_latest
```

This becomes the analytics-ready table.

---

# ⏰ Scheduling

Cloud Scheduler triggers the deduplication Cloud Function daily.

Example:

```bash
gcloud scheduler jobs create http dedup-daily \
  --schedule="0 2 * * *" \
  --uri="https://us-central1-gcp-data-piepline.cloudfunctions.net/dedup-runner" \
  --http-method=GET \
  --time-zone="Asia/Kolkata"
```

---

# 📊 Analytics Layer

Looker Studio is connected to:

```text
patient_v1_latest
```

## Dashboard Metrics

- Total Patients
- Gender Distribution
- Age Distribution
- Data Quality Score
- Records Over Time

---

# 🧪 Example Queries

## Raw Data

```sql
SELECT *
FROM `gcp-data-piepline.healthcare_analytics.patient_v1_raw`
LIMIT 100;
```

## Deduplicated Data

```sql
SELECT *
FROM `gcp-data-piepline.healthcare_analytics.patient_v1_latest`
LIMIT 100;
```

---

# 🚀 Deployment

## Deploy Cloud Run Processor

```bash
gcloud run deploy healthcare-processor \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

---

## Deploy Dedup Cloud Function

```bash
gcloud functions deploy dedup-runner \
  --runtime python311 \
  --trigger-http \
  --entry-point run_dedup \
  --allow-unauthenticated \
  --region us-central1
```

---

# 🔐 IAM Roles Used

Required roles include:

- BigQuery Data Editor
- BigQuery Job User
- Storage Admin
- Cloud Build Builder
- Logging Writer

---

# 📈 Future Improvements

Potential enhancements:

- Incremental deduplication using MERGE
- Data quality monitoring
- Multi-dataset support
- CI/CD with GitHub Actions
- Terraform infrastructure
- Data lineage tracking
- Schema evolution automation
- Monitoring & alerting

---

# 🎯 Key Engineering Concepts Demonstrated

- Event-driven architecture
- Cloud-native data engineering
- Metadata-driven pipelines
- Data lake + warehouse design
- Parquet optimization
- BigQuery analytics
- Automated orchestration
- Scalable ETL processing

---

# 🛠️ Tech Stack

| Category | Technology |
|---|---|
| Backend | Python, FastAPI |
| Data Processing | Pandas, PyArrow |
| Storage | Google Cloud Storage |
| Compute | Cloud Run |
| Warehouse | BigQuery |
| Scheduling | Cloud Scheduler |
| Deduplication | Cloud Functions |
| Visualization | Looker Studio |

---

# 📚 Learning Outcomes

This project demonstrates practical implementation of:

- Data lake architecture
- ETL/ELT workflows
- Event-driven processing
- Schema management
- Data warehousing
- Analytical dashboards
- Production-oriented GCP services

---

# 👨‍💻 Author

Built as a hands-on cloud data engineering project using Google Cloud Platform.

---

# ⭐ If You Found This Useful

Feel free to fork, improve, and build upon this architecture.
