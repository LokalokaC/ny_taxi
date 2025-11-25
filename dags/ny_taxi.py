from pathlib import Path
from datetime import timedelta
import pendulum
import configparser
import json

from src.download import Asset, build_asset, check_url, download_to_tmp, NotReadyError


from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException

config = configparser.RawConfigParser()
config.read(Path('app.ini'))

PROJECT_ID   = config.get(section='GCS', option='GCS_PROJECT_ID')
BUCKET       = config.get(section='GCS', option='GCS_BUCKET')
DATASET_NAME = config.get(section='BQ', option='BQ_DATASET')
TIME_ZONE    = "Asia/Taipei"
SOURCE_BASE  = config.get(section='SOURCE', option='SOURCE_BASE')

raw_taxi = config.get("SOURCE", "TAXI_TYPE", fallback="[]")
TAXI_TYPES = json.loads(raw_taxi)

with open("schema.json") as f:
    taxi_configs = json.load(f)
SCHEMA_FIELDS = taxi_configs["schema_fields"]
CLUSTER_FIELDS = taxi_configs.get("cluster_fields")
TIME_PARTITION = taxi_configs.get("time_partitioning")
STAGING_TABLE = [f"{t}_taxi_staging" for t in TAXI_TYPES]
MAIN_TABLE = [f"{t}_taxi" for t in TAXI_TYPES]

with DAG(
    dag_id="ny_taxi_ingestion",
    start_date=pendulum.datetime(2025, 7, 1, tz=TIME_ZONE),
    schedule= "@monthly",
    catchup=True,
    tags=["ny_taxi","gcs","bigquery"],
    max_active_runs=2,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30)
        },
) as dag:
    
    from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator

    create_datasets = BigQueryCreateEmptyDatasetOperator(
            task_id = "create_bigquery_datasets",
            gcp_conn_id="google_cloud_default",
            project_id = PROJECT_ID,
            dataset_id = DATASET_NAME,
            location = "US",
            exists_ok = True,
        )
        
    create_staging_tables = BigQueryCreateEmptyTableOperator.partial(
        task_id = "create_bigquery_staging_tables",
        gcp_conn_id="google_cloud_default",
        project_id = PROJECT_ID,
        dataset_id = DATASET_NAME,
        exists_ok = True,
        schema_fields=SCHEMA_FIELDS,
        time_partitioning={"type": "MONTH"},
        cluster_fields=CLUSTER_FIELDS
    ).expand(table_id = STAGING_TABLE)
    
    create_main_tables = BigQueryCreateEmptyTableOperator.partial(
        task_id = "create_bigquery_tables",
        gcp_conn_id="google_cloud_default",
        project_id = PROJECT_ID,
        dataset_id = DATASET_NAME,
        exists_ok = True,
        schema_fields=SCHEMA_FIELDS,
        time_partitioning=TIME_PARTITION,
        cluster_fields=CLUSTER_FIELDS
    ).expand(table_id = MAIN_TABLE)
    
    @task
    def build_assets(taxi_type: str) -> dict:
        from dataclasses import asdict
        context = get_current_context()
        logical_date = context["logical_date"].in_timezone(TIME_ZONE)
        year = logical_date.year
        month = logical_date.month
        asset = build_asset(taxi_type=taxi_type, year=year, month=month, project_id=PROJECT_ID, bucket=BUCKET, dataset_name=DATASET_NAME)
        return asdict(asset)

    @task
    def check_availability(asset: dict, min_size: int = 1_000_000) -> dict:
        try:
            check_url(asset["source_url"], min_size=min_size)
            asset["checked"] = True
            return asset
        except NotReadyError as e:
            asset["checked"] = False
            raise AirflowSkipException(f"Not ready: {asset['source_url']} ({e})")

    @task()
    def download(asset: dict)-> dict:

        return download_to_tmp(asset=asset)

    @task()
    def upload(asset: dict) -> dict:
        from src.upload import upload_to_gcs
        return upload_to_gcs(asset=asset)
    
    @task()
    def gcs_to_staging(asset: dict, write_disposition: str, autodetect: bool) -> dict:
        from src.bigquery import load_to_bigquery
        return load_to_bigquery(
            asset=asset,
            write_disposition=write_disposition,
            autodetect=autodetect
        )
    
    @task()
    def staging_to_merge(asset: dict) -> dict:
        from src.bigquery import merge_to_main
        return merge_to_main(asset=asset)
   
    
    assets = build_assets.expand(taxi_type=TAXI_TYPES)
    _check_availability = check_availability.expand(asset=assets)
    _download = download.expand(asset=_check_availability)
    _upload = upload.expand(asset=_download)
    _gcs_to_staging = gcs_to_staging.partial(write_disposition="WRITE_TRUNCATE", autodetect=False).expand(asset=_upload)
    _staging_to_merge = staging_to_merge.expand(asset=_gcs_to_staging)

    create_datasets >> create_main_tables >> create_staging_tables >> assets
    assets >> _check_availability >> _download >> _upload >> _gcs_to_staging >> _staging_to_merge

    