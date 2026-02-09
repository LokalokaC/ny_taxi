from pathlib import Path
from google.cloud import storage
from google.api_core.exceptions import NotFound, Conflict, Forbidden
import time, logging
from src.utils import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

def upload_to_gcs(asset: dict)-> dict:
    local_path = Path(asset["local_path"])
    if not local_path.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")
    
    project_id = asset["project_id"]
    bucket_name = asset["bucket"]
    object_name = f"{asset['dataset_name']}/{local_path.name}"
    gcs_uri = asset["gcs_uri"]

    try:
        client = storage.Client(project=project_id)
    except Exception:
        logger.exception("Failed to init GCS client")
        raise

    try:
        bucket = client.get_bucket(bucket_name)
        logger.info("Bucket %s already exists.", bucket_name)
    except NotFound:
        try:
            bucket = client.create_bucket(bucket_name, project=project_id, location="US")
            logger.info("Created bucket %s", bucket_name)
        except Conflict:
            logger.warning("Bucket name %s already exists in another project. Attempting to use it directly.", bucket_name)
            bucket = client.bucket(bucket_name, user_project=project_id)
        except Forbidden as e:
            raise RuntimeError(
                f"Cannot create bucket '{bucket_name}'. Missing 'storage.buckets.create' "
                f"(need roles/storage.admin or roles/storage.bucketCreator)."
            ) from e
    except Forbidden:
        logger.warning(
            "Bucket '%s' may exist but is not accessible (Forbidden). Proceeding with handle.",
            bucket_name
        )
        bucket = client.bucket(bucket_name, user_project=project_id)
        
    blob = bucket.blob(object_name)
    blob.chunk_size = 8 * 1024 * 1024 #8MB

    start_time = time.time()
    logger.info("Starting to upload: %s to %s", local_path.name, gcs_uri)
    
    try:
        if blob.exists(client=client):
            logger.info("Upload skipped (already exists) | file: %s | gcs_uri: %s | success: %s",
                    local_path.name, gcs_uri, True)
            return asset
    
        blob.upload_from_filename(
            filename=str(local_path), content_type="application/vnd.apache.parquet")
        elapsed_time = round(time.time() - start_time, 2)
        logger.info("File uploaded | file: %s | gcs_uri: %s | time: %.2fs | success: %s", local_path.name, gcs_uri, elapsed_time, True)
        return asset
    except Forbidden as e:
        raise RuntimeError(
            f"Forbidden on upload to bucket '{bucket_name}'. Ensure service account has 'Storage Object Admin' "
            f"role at least (bucket-level IAM)."
        ) from e
    except Exception as e:
        if isinstance(e, Exception) and "412" in str(e):
            logger.warning("Upload skipped (412 PreconditionFailed - already exists) | file: %s | gcs_uri: %s",
                       local_path.name, gcs_uri)
            return asset
        logger.exception("Failed to upload | file: %s | gcs_uri: %s | success: %s", local_path.name, gcs_uri, False)
        raise
