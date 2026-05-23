import json
import functions_framework
from google.cloud import storage, bigquery
import logging
from datetime import datetime

# =========================
# Setup
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT = "gcp-data-piepline"
BQ_DATASET = "healthcare_analytics"
BUCKET = "pipeline-data-lake-hlc"

storage_client = storage.Client()
bq_client = bigquery.Client()


# =========================
# Helpers
# =========================
def read_json(path):
    """Load JSON from GCS"""
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(path)

    if not blob.exists():
        raise Exception(f"Schema not found: {path}")

    return json.loads(blob.download_as_text())


def generate_query(dataset_name, metadata):
    """Generate dedup query"""

    if "primary_key" not in metadata:
        raise Exception("Missing primary_key in metadata")

    pk = ", ".join(metadata["primary_key"])
    order_by = metadata["deduplication"]["order_by"]
    version = metadata["version"]

    base = f"{dataset_name}_{version}"

    return f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{BQ_DATASET}.{base}_latest` AS
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {pk}
                ORDER BY {order_by} DESC
            ) AS row_num
        FROM `{PROJECT}.{BQ_DATASET}.{base}_raw`
    )
    WHERE row_num = 1
    """


# =========================
# MAIN FUNCTION
# =========================
@functions_framework.http
def run_dedup(request):
    try:
        registry = read_json("metadata/registry.json")

        results = []

        for dataset_name, info in registry["datasets"].items():
            try:
                schema_path = info.get("schema_path")

                if not schema_path:
                    raise Exception("Missing schema_path")

                metadata = read_json(schema_path)

                query = generate_query(dataset_name, metadata)

                logger.info(f"🚀 Running dedup: {dataset_name}")

                job = bq_client.query(query)
                job.result()

                results.append({
                    "dataset": dataset_name,
                    "status": "success"
                })

                logger.info(f"✅ Done: {dataset_name}")

            except Exception as e:
                logger.error(f"❌ {dataset_name}: {str(e)}")

                results.append({
                    "dataset": dataset_name,
                    "status": "failed",
                    "error": str(e)
                })

        return {
            "status": "completed",
            "datasets": results,
            "timestamp": datetime.utcnow().isoformat()   # ✅ FIXED
        }

    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")

        return {
            "status": "error",
            "message": str(e)
        }, 500