from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import List, Optional, Dict, Any
import time, logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def create_bigquery_datasets(
        project_id: str,
        dataset_name: str,
        location: str = 'US'
        ) -> str:
    start_time = time.time()
    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        logger.exception("Error occurred while initializing BigQuery client: %s", e)
        raise

    dataset_id = f"{project_id}.{dataset_name}"

    try:
        client.get_dataset(dataset_id)
        logger.info("Dataset already existed. Skip creating datasets: %s", dataset_id)
        return dataset_id
    except NotFound:
        logger.info("Dataset not found. Will create: %s", dataset_id)
    
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location

    try:
        client.create_dataset(dataset, timeout=30)
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        logger.info("Created dataset %s for (%.2fs)", dataset_id, elapsed_time)
        return dataset_id
    except Exception as e:
        logger.exception("Failed to create dataset %s: %s ", dataset, e)
        raise


def create_bigquery_tables(
        project_id: str,
        dataset_id: str,
        table_name: str,
        schema: List[bigquery.SchemaField],
        partition_field: Optional[str] = None,
        clustering_fields: Optional[List[str]] = None
        ) -> str:
    
    start_time = time.time()
    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        logger.exception("Error occurred while initializing BigQuery client: %s", e)
        raise

    table_id = f"{dataset_id}.{table_name}"

    try:
        client.get_table(table_id)
        logger.info("Table already existed. Skip creating %s", table_id)
        return table_id
    except NotFound:
        logger.info("Table not found. Will create %s", table_name)

    table = bigquery.Table(table_id, schema=schema)
    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
    if clustering_fields:
        table.clustering_fields = clustering_fields
    
    try:
        client.create_table(table)
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        logger.info("Table %s created (%.2fs).", table_id, elapsed_time)
        return table_id
    except Exception as e:
        logger.exception("Failed to create %s", table_id)
        raise

def load_to_tables(
        project_id: str,
        bucket: str,
        dataset_name: str,
        table_name: str,
        taxi_type: str,
        year: int,
        month: int,
        write_disposition: str = "WRITE_TRUNCATE",
        autodetect: bool = True,
) -> Dict[str, Any]:
    """
    Load one parquet file after another from GCS to staging table.
    """
    start_time = time.time()
    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        logger.exception("Error occurred while initializing BigQuery client: %s", e)
        raise

    table_id = f"{project_id}.{dataset_name}.{table_name}"
    gcs_uri = f"gs://{bucket}/{dataset_name}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        autodetect=autodetect,
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )

    logger.info("Loading a parquet file to table %s from %s", table_id, gcs_uri)
    
    try:
        job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        job.result()
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        logger.info("Parquet file loaded to %s successfully (%.2fs). JobID=%s", table_id, elapsed_time, job.job_id)
        return job.job_id
    except Exception as e:
        logger.info("Job failed when loading into %s", table_id)
        raise

def load_to_bigquery(
        asset: dict,
        write_disposition: str = "WRITE_TRUNCATE",
        autodetect: bool = True,
):
    start_time = time.time()
    try:
        client = bigquery.Client(project=asset["project_id"])
    except Exception as e:
        logger.exception("Error occurred while initializing BigQuery client: %s", e)
        raise
    
    table_id = f"{asset['project_id']}.{asset['dataset_name']}.{asset['table_name']}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        autodetect=autodetect,
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )

    logger.info(f"Loading {asset['gcs_uri']} into {table_id}")
    try:
        job = client.load_table_from_uri(asset['gcs_uri'], table_id, job_config=job_config)
        job.result()
        elapsed = round(time.time() - start_time, 2)
        logger.info(f"Loaded {asset['gcs_uri']} to {table_id} in {elapsed}s")
        return {"job_id": job.job_id, "success": True}
    except Exception as e:
        logger.exception(f"Failed loading {asset['gcs_uri']} -> {table_id}: {e}")
        raise

