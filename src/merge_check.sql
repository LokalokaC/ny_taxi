    WITH S AS (
      SELECT
        TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
          S.VendorID,
          S.tpep_pickup_datetime,
          S.tpep_dropoff_datetime,
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
    )
    SELECT 1
    FROM S
    WHERE NOT EXISTS (
      SELECT 1 FROM `{{project_id}}.{{dataset_name}}.{{table_name}}` T
      WHERE T.trip_hash = S.trip_hash
    )
    LIMIT 1