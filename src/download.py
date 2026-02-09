from pathlib import Path
from dataclasses import dataclass
import hashlib, requests, time, logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.utils import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class NotReadyError(Exception):
    pass

def get_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["HEAD", "GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

@dataclass
class Asset:
    taxi_type: str
    year: int
    month: int
    project_id: str
    bucket: str
    dataset_name: str
    stg_table_name: str
    table_name: str
    file_name: str
    source_url: str
    gcs_uri: str
    

def build_asset(
        taxi_type: str,
        year: int,
        month: int,
        source_base: str,
        project_id: str,
        bucket:str,
        dataset_name: str
        ) -> Asset:
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    return Asset(
        taxi_type = taxi_type,
        year = year,
        month = month,
        project_id = project_id,
        bucket = bucket,
        dataset_name = dataset_name,
        stg_table_name = f"{taxi_type}_taxi_staging",
        table_name = f"{taxi_type}_taxi",
        file_name = file_name,
        source_url=f"{source_base}/{file_name}",
        gcs_uri=f"gs://{bucket}/{dataset_name}/{file_name}"
    )

def check_url(url: str, min_size: int = 1_000_000) -> None:
    """
    Check whether the data is available for downloading.
    """
    with get_session() as session:
        r = session.head(url, timeout=30, allow_redirects=True)
    if r.status_code != 200:
        raise NotReadyError(f"status = {r.status_code}")
    size = int(r.headers.get("Content-Length", "0"))
    if size < min_size:
        raise NotReadyError(f"Too small {size} bytes")

def download_to_tmp(asset: dict) -> dict:
    """
    Download a file from the given URL and save under tmp/<dataset_name>/<taxi_type>/.
    Logs progress, warnings, and timing information.
    """  
    start_time = time.time()
    h = hashlib.md5()

    base = Path(__file__).resolve().parent.parent / 'tmp' / asset["dataset_name"] / asset["taxi_type"]
    base.mkdir(parents=True, exist_ok=True)
    logger.debug("Temporary directory created at %s",str(base))

    file_path = base / asset["file_name"]

    if file_path.exists() and file_path.stat().st_size > 1_000_000:
        asset["local_path"] = str(file_path)
        logger.info("Download skipped(exists) | file: %s | Size: %.2f MB | Success: %s",
                    file_path.name, file_path.stat().st_size/1024/1024, True)
        return asset

    logger.info("Starting download %s", file_path.name)

    try:
        with get_session() as session:
            r = session.get(asset["source_url"], stream=True, timeout=120)
            r.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    h.update(chunk)
    except requests.RequestException as e:
        try:
            if file_path.exists():
                file_path.unlink()
        except Exception:
            logger.warning("Failed to remove partial file %s after error.", str(file_path))

        elapsed_time = round(time.time() - start_time, 2)
        logger.error("Download failed | url: %s | file %s | error %s | Success: %s",
                        asset["source_url"], asset["file_name"], str(e), False)
        raise
    
    elapsed_time = round(time.time() - start_time, 2)
    size = file_path.stat().st_size

    if size < 1_000_000:
        logger.warning("Downloaded file unusually small | file %s | size: %.1f KB",
                       file_path.name, size/1024)

    asset["local_path"] = str(file_path)

    logger.info("Download completed | file: %s | Size: %.2f MB | Time: %.2f s | md5: %s | Success: %s",
                file_path.name, size/1024/1024, elapsed_time, h.hexdigest(), True)
    
    return asset