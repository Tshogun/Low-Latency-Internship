import logging
import os
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from google.cloud import storage
from google.cloud.storage import exceptions
import uuid
import json
from datetime import datetime
from typing import Dict, Any, List
import jsonschema
from starlette.middleware.base import BaseHTTPMiddleware
import traceback

# Configure logging FIRST
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(debug=False)  # debug=False for production

BUCKET_NAME = "pipeline-data-lake-hlc"
storage_client = None
bucket = None
registry = {}
schemas = {}

class DebugMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        logger.info(f"Request: {request.method} {request.url}")
        response = await call_next(request)
        logger.info(f"Response: {response.status_code}")
        return response

# Add middleware AFTER app creation
app.add_middleware(DebugMiddleware)

def init_storage():
    global storage_client, bucket
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        bucket.get_iam_policy()  # Test access
        logger.info("✅ Storage initialized")
        return True
    except Exception as e:
        logger.error(f"❌ Storage init failed: {e}")
        return False

def load_metadata():
    global registry, schemas
    try:
        registry_blob = bucket.blob("metadata/registry.json")
        if not registry_blob.exists():
            logger.error("❌ registry.json not found")
            return False
        
        registry = json.loads(registry_blob.download_as_text())
        logger.info(f"✅ Loaded {len(registry['datasets'])} datasets")
        
        # Load schemas
        schemas.clear()
        for dataset, info in registry["datasets"].items():
            schema_path = info["schema_path"]
            schema_blob = bucket.blob(schema_path)
            if schema_blob.exists():
                schemas[dataset] = json.loads(schema_blob.download_as_text())
                logger.info(f"✅ Schema loaded: {dataset}")
        
        logger.info(f"✅ Metadata ready: {list(schemas.keys())}")
        return True
    except Exception as e:
        logger.error(f"❌ Metadata failed: {e}")
        return False

def get_hierarchical_path(dataset: str, file_id: str, file_ext: str = "json") -> str:
    now = datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%Y-%m")
    return f"raw/{dataset}/{year}/{month}/{file_id}.{file_ext}"

def validate_against_schema(data: Dict[str, Any], dataset: str) -> tuple[bool, str]:
    if dataset not in schemas:
        return False, f"No schema for '{dataset}'"
    
    schema = schemas[dataset].get("fields", schemas[dataset])
    try:
        jsonschema.validate(instance=data, schema=schema)
        return True, "Valid"
    except jsonschema.ValidationError as e:
        return False, str(e.message)

def upload_to_gcs(data: bytes, path: str, content_type: str = "application/json"):
    blob = bucket.blob(path)
    blob.upload_from_string(data, content_type=content_type)
    blob.reload()
    logger.info(f"✅ Uploaded: {path}")

# ✅ STARTUP SAFE - No global calls
@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Starting healthcare pipeline API...")
    if init_storage() and load_metadata():
        logger.info("✅ Startup complete!")
    else:
        logger.error("❌ Startup failed - service unhealthy")

@app.get("/")
async def health(request: Request):
    return {
        "status": "healthy",
        "datasets": list(registry.get("datasets", {}).keys()),
        "schemas_loaded": len(schemas),
        "bucket": BUCKET_NAME
    }

@app.post("/ingest/json")
async def ingest_json(payload: Dict[str, Any], request: Request):
    if not bucket:  # Safety check
        raise HTTPException(503, "Service not ready")
    
    dataset = payload.get("dataset")
    if not dataset:
        raise HTTPException(400, "dataset required")
    
    if dataset not in registry.get("datasets", {}):
        raise HTTPException(400, f"Dataset '{dataset}' not registered")
    
    data_to_validate = {k: v for k, v in payload.items() if k != "dataset"}
    is_valid, error = validate_against_schema(data_to_validate, dataset)
    
    if not is_valid:
        raise HTTPException(400, error)
    
    file_id = str(uuid.uuid4())
    path = get_hierarchical_path(dataset, file_id)
    upload_to_gcs(json.dumps(payload).encode(), path)
    
    return {
        "status": "stored",
        "dataset": dataset,
        "path": path,
        "file_id": file_id
    }

@app.post("/ingest/file")
async def ingest_file(dataset: str, file: UploadFile = File(...)):
    if not bucket:
        raise HTTPException(503, "Service not ready")
    
    if dataset not in registry.get("datasets", {}):
        raise HTTPException(400, f"Dataset '{dataset}' not registered")
    
    content = await file.read()
    data = json.loads(content)
    is_valid, error = validate_against_schema(data, dataset)
    
    if not is_valid:
        raise HTTPException(400, error)
    
    file_id = str(uuid.uuid4())
    path = get_hierarchical_path(dataset, file_id)
    upload_to_gcs(content, path, file.content_type)
    
    return {"status": "uploaded", "path": path, "file_id": file_id}

@app.get("/debug")
async def debug():
    return {
        "registry": registry,
        "schemas_loaded": list(schemas.keys()),
        "storage_ready": bucket is not None
    }
