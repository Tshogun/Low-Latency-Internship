from google.cloud import bigquery, storage
import json

PROJECT_ID = "gcp-data-piepline"
DATASET_ID = "healthcare_analytics"

TYPE_MAPPING = {
    "string": "STRING",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "int": "INT64",
    "float": "FLOAT64"
}

storage_client = storage.Client()
bq_client = bigquery.Client()


def read_json(bucket_name, path):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(path)
    return json.loads(blob.download_as_text())


def build_schema(fields):
    return [
        bigquery.SchemaField(name, TYPE_MAPPING[dtype])
        for name, dtype in fields.items()
    ]


def get_table_id(dataset, version):
    return f"{PROJECT_ID}.{DATASET_ID}.{dataset}_{version}_raw"


def table_exists(table_id):
    try:
        bq_client.get_table(table_id)
        return True
    except:
        return False


def create_table(table_id, schema, partition_field=None, cluster_fields=None):
    table = bigquery.Table(table_id, schema=schema)

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            field=partition_field
        )

    if cluster_fields:
        table.clustering_fields = cluster_fields

    bq_client.create_table(table)


def load_parquet_to_bq(table_id, gcs_uri):
    job = bq_client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_APPEND",
        ),
    )
    job.result()