from pathlib import Path
import pendulum
import configparser
import json

from src.download import Asset, build_asset, check_url, download_to_tmp, NotReadyError
from src.upload import upload_to_gcs
from src.bigquery import load_to_bigquery

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator

config = configparser.RawConfigParser()
config.read(Path('app.ini'))
raw_taxi = config.get("SOURCE", "TAXI_TYPE", fallback="[]")

PROJECT_ID   = config.get(section='GCS', option='GCS_PROJECT_ID')
BUCKET       = config.get(section='GCS', option='GCS_BUCKET')
DATASET_NAME = config.get(section='BQ', option='BQ_DATASET')
TABLE_NAME   = config.get(section='BQ', option='BQ_TABLE')
TIME_ZONE    = "Asia/Taipei"
SOURCE_BASE  = config.get(section='SOURCE', option='SOURCE_BASE')
TAXI_TYPES   = json.loads(raw_taxi)
SCHEMA_FIELDS = []
CLUSTER_FIELDS = []

with DAG(
    dag_id="ny_taxi_ingestion",
    start_date=pendulum.datetime(2025, 1, 1, tz=TIME_ZONE),
    schedule= "@daily",
    catchup=False,
    tags=["ny_taxi","gcs","bigquery"],
    default_args={"retries": 2},
) as dag:

    create_datasets = BigQueryCreateEmptyDatasetOperator(
            task_id = "create_bigquery_datasets",
            project_id = PROJECT_ID,
            dataset_id = DATASET_NAME,
            location = "US",
            exists_ok = True,
        )
        
    create_tables = BigQueryCreateEmptyTableOperator(
        task_id = "create_bigquery_tables",
        project_id = PROJECT_ID,
        dataset_id = DATASET_NAME,
        table_id = TABLE_NAME,
        exists_ok = True,
        schema_fields=(SCHEMA_FIELDS or None),
        time_partitioning={"type": "DAY", "field": "pickup_datetime"},
        cluster_fields=CLUSTER_FIELDS,
        )
    
    @task
    def build_assets(taxi_type: str) -> dict:
        from dataclasses import asdict
        context = get_current_context()
        logical_date = context["logical_date"].in_timezone(TIME_ZONE)
        year = logical_date.year
        month = logical_date.month
        asset = build_asset(taxi_type=taxi_type, year=year, month=month, project_id=PROJECT_ID, bucket=BUCKET, dataset_name=DATASET_NAME, table_name=TABLE_NAME)
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
        return upload_to_gcs(asset=asset)
    
    @task()
    def gcs_to_bigquery(asset: dict, write_disposition: str, autodetect: bool):
        return load_to_bigquery(
            asset=asset,
            write_disposition=write_disposition,
            autodetect=autodetect
        )
    
    assets = build_assets.expand(taxi_type=TAXI_TYPES)
    _check_availability = check_availability.expand(asset=assets)
    _download = download.expand(asset=_check_availability)
    _upload = upload.expand(asset=_download)
    _gcs_to_bigquery = gcs_to_bigquery.partial(write_disposition="WRITE_TRUNCATE", autodetect=True).expand(asset=_upload)

    create_datasets >> create_tables >> assets
    assets >> _check_availability >> _download >> _upload >> _gcs_to_bigquery

    