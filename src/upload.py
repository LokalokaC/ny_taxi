from pathlib import Path
from google.cloud import storage
import time, logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def upload_to_gcs(asset: dict)-> dict:
    local_path = asset["local_path"]
    p = Path(local_path)

    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")
    file_name = p.name
    gcs_bucket_name = asset["bucket"]
    object_name = f"{asset['dataset_name']}/{file_name}"
    gcs_uri = asset["gcs_uri"]
    start_time = time.time()

    logger.info("Starting to upload | file: %s to %s", file_name, gcs_uri)

    try:
        client = storage.Client()
    except Exception:
        logger.exception("Error occurred while initializing GCS client.")
        raise

    bucket = client.bucket(gcs_bucket_name)
    blob = bucket.blob(object_name)
    blob.chunk_size = 8 * 1024 * 1024 #8MB

    if blob.exists(client=client):
        logger.info("Upload skipped (exists) | file: %s | gcs_uri: %s | success: %s", file_name, gcs_uri, True)
        return asset
    
    try:
        blob.upload_from_filename(str(p), content_type="application/x-parquet")
        elapsed_time = round(time.time() - start_time, 2)
        logger.info("File uploaded | file: %s | gcs_uri: %s | time: %.2fs | success: %s", file_name, gcs_uri, elapsed_time, True)
        return asset
    except Exception:
        logger.exception("Failed to upload | file: %s | gcs_uri: %s | success: %s", file_name, gcs_uri, False)
        raise