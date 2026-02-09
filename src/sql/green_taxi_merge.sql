MERGE `{{ project_id }}.{{ dataset_name }}.{{ table_name }}` T
USING (
  SELECT
    S.*,
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
      S.VendorID,
      S.lpep_pickup_datetime,
      S.lpep_dropoff_datetime,
      S.PULocationID,
      S.DOLocationID,
      S.passenger_count,
      S.trip_distance,
      S.total_amount,
      S.payment_type
    )))) AS trip_hash
  FROM `{{ project_id }}.{{ dataset_name }}.{{ stg_table_name }}` AS S
  WHERE EXTRACT(YEAR  FROM _PARTITIONTIME) = {{ year }}
    AND EXTRACT(MONTH FROM _PARTITIONTIME) = {{ month }}
) U
ON T.trip_hash = U.trip_hash
WHEN NOT MATCHED THEN INSERT (
  VendorID,
  lpep_pickup_datetime, 
  lpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  trip_type,
  cbd_congestion_fee,
  trip_hash
)
VALUES (
  U.VendorID,
  U.lpep_pickup_datetime,
  U.lpep_dropoff_datetime,
  U.passenger_count,
  U.trip_distance,
  U.RatecodeID,
  U.PULocationID,
  U.DOLocationID,
  U.payment_type,
  U.fare_amount,
  U.extra,
  U.mta_tax,
  U.tip_amount,
  U.ehail_fee,
  U.improvement_surcharge,
  U.total_amount,
  U.congestion_surcharge,
  U.trip_type,
  U.cbd_congestion_fee,
  U.trip_hash
);