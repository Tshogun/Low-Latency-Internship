import logging
import os
from typing import Dict
from datetime import datetime
import json
import time
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from google.cloud import storage, bigquery
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO, StringIO
from pathlib import Path
import traceback

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# =========================
# App
# =========================
app = FastAPI(
    title="Healthcare Raw → Parquet → BigQuery Pipeline",
    version="3.2.0"
)

# =========================
# Config
# =========================
PROJECT_ID = os.getenv("GCP_PROJECT", "gcp-data-piepline")
RAW_BUCKET_NAME = os.getenv("RAW_BUCKET_NAME", "pipeline-data-lake-hlc")
BQ_DATASET = "healthcare_analytics"

TYPE_MAPPING = {
    "string": "STRING",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "int": "INT64",
    "float": "FLOAT64"
}

# =========================
# Clients
# =========================
storage_client = None
bucket = None
bq_client = None


def init_clients():
    global storage_client, bucket, bq_client

    try:
        if storage_client is None:
            storage_client = storage.Client()
            bucket = storage_client.bucket(RAW_BUCKET_NAME)
            bucket.get_iam_policy(timeout=10)

        if bq_client is None:
            bq_client = bigquery.Client()

        return True

    except Exception as e:
        logger.error(f"❌ Client init failed: {str(e)}")
        logger.error(traceback.format_exc())
        return False


# =========================
# Metadata helpers
# =========================
def read_json_from_gcs(path: str):
    blob = bucket.blob(path)
    return json.loads(blob.download_as_text())


def build_bq_schema(fields: Dict[str, str]):
    return [
        bigquery.SchemaField(name, TYPE_MAPPING[dtype])
        for name, dtype in fields.items()
    ]


def enforce_schema(df: pd.DataFrame, schema_fields):
    """Keep only fields defined in metadata schema"""
    return df[[col for col in df.columns if col in schema_fields]]


def get_table_id(dataset: str, version: str):
    return f"{PROJECT_ID}.{BQ_DATASET}.{dataset}_{version}_raw"


def table_exists(table_id: str) -> bool:
    try:
        bq_client.get_table(table_id)
        return True
    except:
        return False


def create_table(table_id: str, schema, partition_field=None, cluster_fields=None):
    table = bigquery.Table(table_id, schema=schema)

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(field=partition_field)

    if cluster_fields:
        table.clustering_fields = cluster_fields

    bq_client.create_table(table)
    logger.info(f"🆕 Created table {table_id}")


def load_parquet_to_bq(table_id: str, gcs_uri: str):
    job = bq_client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_APPEND",
        ),
    )
    job.result()
    logger.info(f"📊 Loaded data into {table_id}")


# =========================
# Data Processing
# =========================
def parse_dataset_from_path(file_path: str) -> str:
    if not file_path.startswith("raw/"):
        return None
    return Path(file_path).parts[1]


def clean_and_enrich_dataframe(df: pd.DataFrame, file_path: str = "unknown") -> pd.DataFrame:
    df = df.copy()

    df.columns = [str(col).lower().strip().replace(' ', '_') for col in df.columns]

    now = pd.Timestamp.utcnow().tz_localize(None)

    df["processed_timestamp"] = now
    df["source_file"] = file_path

    if "patient_id" in df.columns:
        df = df.drop_duplicates(subset=["patient_id"], keep="last")

    for col in ["first_name", "last_name"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    if "date_of_birth" in df.columns:
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce").dt.date

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    if "date_of_birth" in df.columns:
        dob_series = pd.to_datetime(df["date_of_birth"], errors="coerce")
        df["age"] = ((now - dob_series).dt.days // 365)

    df["quality_score"] = 1.0
    df.loc[df.isnull().any(axis=1), "quality_score"] = 0.7

    return df


def generate_parquet_path(file_path: str) -> str:
    now = datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%Y-%m")
    base_name = Path(file_path).stem
    dataset = parse_dataset_from_path(file_path)

    return f"processed/{dataset}/{year}/{month}/{base_name}.parquet"


# =========================
# Lifecycle
# =========================
@app.on_event("startup")
async def startup():
    logger.info("🚀 Pipeline ready (Parquet + BigQuery)")
    logger.info(f"📦 Bucket: {RAW_BUCKET_NAME}")


@app.get("/health")
async def health_check():
    if not init_clients():
        raise HTTPException(503, "Services unavailable")
    return {"status": "healthy"}


# =========================
# MAIN EVENT HANDLER
# =========================
@app.post("/")
async def process_gcs_event(request: Request, background_tasks: BackgroundTasks):

    if not init_clients():
        raise HTTPException(503, "Clients not ready")

    try:
        event = await request.json()
        bucket_name = event.get("bucket", "")
        file_path = event.get("name", "")

        logger.info(f"📥 Event: gs://{bucket_name}/{file_path}")

        if bucket_name != RAW_BUCKET_NAME or not file_path.startswith("raw/"):
            return {"status": "ignored"}

        dataset = parse_dataset_from_path(file_path)
        if not dataset:
            return {"status": "invalid_dataset"}

        blob = bucket.blob(file_path)

        for _ in range(3):
            if blob.exists():
                break
            time.sleep(1)

        if not blob.exists():
            return {"status": "file_not_ready"}

        content = blob.download_as_bytes()

        # Parse input
        if file_path.endswith(".json"):
            data = json.loads(content.decode("utf-8"))
            df = pd.DataFrame([data] if isinstance(data, dict) else data)
        elif file_path.endswith(".csv"):
            df = pd.read_csv(StringIO(content.decode("utf-8")))
        else:
            return {"status": "unsupported"}

        if df.empty:
            return {"status": "empty"}

        # =========================
        # CLEAN
        # =========================
        df = clean_and_enrich_dataframe(df, file_path)

        # =========================
        # LOAD METADATA + ENFORCE SCHEMA (FIX)
        # =========================
        registry = read_json_from_gcs("metadata/registry.json")
        dataset_info = registry["datasets"][dataset]

        version = dataset_info["active_version"]
        schema_path = dataset_info["schema_path"]

        schema_json = read_json_from_gcs(schema_path)

        schema_fields = list(schema_json["fields"].keys())
        df = enforce_schema(df, schema_fields)

        logger.info(f"✅ Final columns: {df.columns.tolist()}")

        # =========================
        # PARQUET
        # =========================
        parquet_path = generate_parquet_path(file_path)

        table = pa.Table.from_pandas(df, preserve_index=False)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        # =========================
        # BACKGROUND TASK
        # =========================
        async def upload_and_load():
            try:
                processed_blob = bucket.blob(parquet_path)
                processed_blob.upload_from_file(buffer)

                logger.info(f"✅ Uploaded {parquet_path}")

                table_id = get_table_id(dataset, version)
                schema = build_bq_schema(schema_json["fields"])

                if not table_exists(table_id):
                    create_table(table_id, schema)

                gcs_uri = f"gs://{RAW_BUCKET_NAME}/{parquet_path}"

                load_parquet_to_bq(table_id, gcs_uri)

            except Exception as e:
                logger.error(f"❌ Upload/BQ failed: {str(e)}")
                logger.error(traceback.format_exc())

        background_tasks.add_task(upload_and_load)

        return {
            "status": "processing",
            "dataset": dataset,
            "rows": len(df),
            "output": parquet_path
        }

    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(500, str(e))