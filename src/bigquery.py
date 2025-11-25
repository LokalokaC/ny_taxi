from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest
from airflow.exceptions import AirflowSkipException
from pathlib import Path
from jinja2 import Template
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

def load_to_bigquery(
        asset: dict,
        write_disposition: str = "WRITE_TRUNCATE",
        autodetect: bool = False,
) -> dict:
    '''
    Check whether data from gcs exists in staging table by its partition_id.
    If not, load from gcs to staging table.
    '''
    start_time = time.time()
    try:
        client = bigquery.Client(project=asset["project_id"])
    except Exception as e:
        logger.exception("Error occurred while initializing BigQuery client: %s", e)
        raise
    
    year = asset['year']
    month = asset['month']
    partition_id = f"{year}{month:02d}"
    table_id = f"{asset['project_id']}.{asset['dataset_name']}.{asset['stg_table_name']}${partition_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        #autodetect=autodetect,
        schema_update_options=["ALLOW_FIELD_RELAXATION"],
    )

    logger.info(f"Loading {asset['gcs_uri']} into {table_id}")
    try:
        job = client.load_table_from_uri(asset['gcs_uri'], table_id, job_config=job_config)
        job.result()
        elapsed = round(time.time() - start_time, 2)
        logger.info("Loaded to staging table: %s | file: %s | time: %.2fs | job_id: %s | success: %s", table_id, asset['gcs_uri'], elapsed, job.job_id, True)
        return asset
    except BadRequest as e:
        if "WRITE_EMPTY" in str(e):
            raise AirflowSkipException("Partition already has data, skip.")
        else:
            raise

def merge_to_main(asset: dict) -> dict:
    '''
    Check whether data from staging table is in main table already.
    If not, merge into main table.
    '''
    project_id = asset["project_id"]
    dataset_name = asset["dataset_name"]
    table_name = asset["table_name"]
    stg_table_name  = asset["stg_table_name"]
    file_name = asset["file_name"]
    year = asset["year"]
    month = asset["month"]

    client = bigquery.Client(project=project_id)

    CHECK_MERGE_SQL_DIR = Path(__file__).resolve().parent / 'merge_check.sql'
    with open(CHECK_MERGE_SQL_DIR, encoding="utf-8") as f:
        template = Template(f.read())
        check_sql = template.render(
        year=year,
        month=month,
        project_id=project_id,
        dataset_name=dataset_name,
        table_name=table_name,
        stg_table_name=stg_table_name
    )
        
    check_job = client.query(
        check_sql,
        job_config=bigquery.QueryJobConfig(
            use_legacy_sql=False,
            priority=bigquery.QueryPriority.BATCH
        )
    )
    check_result = any(True for _ in check_job.result())

    if not check_result:
        raise AirflowSkipException(
            f"No new rows to MERGE for {table_name} {year}-{month:02d}; skipping."
        )

    MERGE_SQL_DIR = Path(__file__).resolve().parent / 'merge.sql'
    with open(MERGE_SQL_DIR, encoding="utf-8") as f:
        template = Template(f.read())
        sql = template.render(
        year=year,
        month=month,
        project_id=project_id,
        dataset_name=dataset_name,
        table_name=table_name,
        stg_table_name=stg_table_name
    )
        
    job = client.query(
        sql, job_config=bigquery.QueryJobConfig(
            use_legacy_sql=False,
            priority=bigquery.QueryPriority.BATCH
        )
    )
    job.result()

    return {"Loaded to main table": file_name,"job_id": job.job_id, "success": True}