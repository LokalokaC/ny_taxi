import pandas as pd
from pathlib import Path

base = Path(__file__).resolve().parent.parent / 'tmp' / 'green_tripdata_2025-10.parquet'
##docker_dir = Path("/opt/airflow/tmp/ny_taxi/yellow/yellow_tripdata_2025-07.parquet")
print(base)

df = pd.read_parquet(base, engine='pyarrow')
print(df.dtypes)