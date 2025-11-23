import pandas as pd
from pathlib import Path

base = Path(__file__).resolve().parent.parent / 'tmp' / 'yellow_tripdata_2025-09.parquet'
docker_dir = Path("/opt/airflow/tmp/ny_taxi/yellow/yellow_tripdata_2025-07.parquet")
print(base)

df = pd.read_parquet(docker_dir, engine='pyarrow')
print(df.iloc[0])