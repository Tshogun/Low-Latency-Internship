import os
import json
import re
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from google.cloud import storage
from typing import Dict, List

app = FastAPI(title="Metadata Registry API")

# Configure CORS properly
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "https://your-vercel-app.vercel.app",
        "*"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

BUCKET_NAME = "pipeline-data-lake-hlc"
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

def read_json(gcs_path: str) -> Dict:
    """Read and parse JSON from GCS"""
    blob = bucket.blob(gcs_path)
    if not blob.exists():
        raise HTTPException(status_code=404, detail=f"{gcs_path} not found")
    content = blob.download_as_string()
    return json.loads(content)

def write_json(gcs_path: str, data: Dict):
    """Write JSON to GCS"""
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")

@app.options("/{path:path}")
async def options_handler(path: str):
    """Handle OPTIONS requests for CORS preflight"""
    return JSONResponse(content={"message": "OK"}, status_code=200)

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "Metadata API is running",
        "bucket": BUCKET_NAME,
        "endpoints": [
            "GET  /api/registry",
            "POST /api/registry",
            "GET  /api/schema?dataset=patient&version=v1",
            "POST /api/schema?dataset=patient",
            "GET  /api/versions?dataset=patient",
            "GET  /api/active-version/{dataset}"
        ]
    }

@app.get("/api/registry")
async def get_registry():
    """Return the content of metadata/registry.json"""
    try:
        return read_json("metadata/registry.json")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/registry")
async def update_registry(registry_data: Dict = Body(...)):
    """Overwrite registry.json (used when activating a version)"""
    try:
        write_json("metadata/registry.json", registry_data)
        return {"status": "ok", "message": "Registry updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/schema")
async def get_schema(dataset: str, version: str):
    """Return a specific schema file, e.g. metadata/patient/v1.json"""
    try:
        if ".." in dataset or ".." in version or "/" in dataset or "/" in version:
            raise HTTPException(status_code=400, detail="Invalid dataset or version name")
        path = f"metadata/{dataset}/{version}.json"
        return read_json(path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/schema")
async def save_new_schema(dataset: str, schema: Dict = Body(...)):
    """
    Save a new version of the schema for a dataset.
    Automatically determines the next version number (e.g., v3).
    Also updates registry.json to make this the active version.
    """
    try:
        if ".." in dataset or "/" in dataset:
            raise HTTPException(status_code=400, detail="Invalid dataset name")
        
        # 1. Get existing versions
        prefix = f"metadata/{dataset}/"
        blobs = bucket.list_blobs(prefix=prefix)
        existing_versions = []
        for blob in blobs:
            match = re.search(r'v(\d+)\.json$', blob.name)
            if match:
                existing_versions.append(int(match.group(1)))
        
        # 2. Determine next version number
        next_num = max(existing_versions, default=0) + 1
        new_version = f"v{next_num}"
        new_path = f"{prefix}{new_version}.json"
        
        # 3. Save the new schema file
        write_json(new_path, schema)
        
        # 4. Update registry.json to point to this new version
        registry_path = "metadata/registry.json"
        try:
            registry = read_json(registry_path)
        except:
            # If registry doesn't exist, create it
            registry = {"datasets": {}}
        
        # Update or create dataset entry
        if dataset not in registry["datasets"]:
            registry["datasets"][dataset] = {}
        
        registry["datasets"][dataset]["active_version"] = new_version
        registry["datasets"][dataset]["schema_path"] = new_path
        
        # Save updated registry
        write_json(registry_path, registry)
        
        return {
            "dataset": dataset, 
            "new_version": new_version, 
            "path": new_path,
            "active_version": new_version,
            "message": f"Created {new_version} for {dataset} and set as active"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/schema/without-activate")
async def save_schema_without_activate(dataset: str, schema: Dict = Body(...)):
    """
    Save a new version WITHOUT updating registry (for testing)
    """
    try:
        if ".." in dataset or "/" in dataset:
            raise HTTPException(status_code=400, detail="Invalid dataset name")
        
        prefix = f"metadata/{dataset}/"
        blobs = bucket.list_blobs(prefix=prefix)
        existing_versions = []
        for blob in blobs:
            match = re.search(r'v(\d+)\.json$', blob.name)
            if match:
                existing_versions.append(int(match.group(1)))
        
        next_num = max(existing_versions, default=0) + 1
        new_version = f"v{next_num}"
        new_path = f"{prefix}{new_version}.json"
        write_json(new_path, schema)
        
        return {
            "dataset": dataset, 
            "new_version": new_version, 
            "path": new_path,
            "message": f"Created {new_version} for {dataset} (registry not updated)"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/versions")
async def list_versions(dataset: str):
    """Return list of version strings (e.g., ['v1','v2']) for a dataset"""
    try:
        if ".." in dataset or "/" in dataset:
            raise HTTPException(status_code=400, detail="Invalid dataset name")
        
        prefix = f"metadata/{dataset}/"
        blobs = bucket.list_blobs(prefix=prefix)
        versions = []
        for blob in blobs:
            match = re.search(r'v(\d+)\.json$', blob.name)
            if match:
                versions.append(f"v{match.group(1)}")
        versions.sort(key=lambda x: int(x[1:]))
        return {"dataset": dataset, "versions": versions}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/active-version/{dataset}")
async def get_active_version(dataset: str):
    """Get the active version for a specific dataset"""
    try:
        registry = read_json("metadata/registry.json")
        if dataset not in registry.get("datasets", {}):
            raise HTTPException(404, f"Dataset '{dataset}' not found in registry")
        return {
            "dataset": dataset, 
            "active_version": registry["datasets"][dataset]["active_version"],
            "schema_path": registry["datasets"][dataset]["schema_path"]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)