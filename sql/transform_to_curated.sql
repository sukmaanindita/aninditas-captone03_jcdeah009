MERGE INTO `{curated_table}` T
USING (
  WITH combined_staging AS (
    -- 1. Data Batch (April & Mei)
    -- event_id deterministic: SHA256(source_file_identity + source_row_number + business fields).
    -- source_file_identity dan source_row_number berasal dari staging table (diisi oleh
    -- load_staging.py sebelum filter/dedup/transform apa pun), sehingga:
    --   - setiap baris fisik source punya event_id unik (row number tidak pernah collide)
    --   - rerun file yang sama menghasilkan event_id yang identik (idempotent)
    --   - April vs Mei tidak pernah collide (source_file_identity berbeda per periode)
    -- IFNULL dipakai di setiap komponen karena BigQuery CONCAT() mengembalikan NULL jika
    -- salah satu argumen NULL -- tanpa ini, satu kolom bisnis NULL akan membuat event_id
    -- seluruhnya NULL.
    SELECT
      TO_HEX(SHA256(
        CONCAT(
          IFNULL(source_file_identity, ''), '|',
          IFNULL(CAST(source_row_number AS STRING), ''), '|',
          IFNULL(CAST(lpep_pickup_datetime AS STRING), ''), '|',
          IFNULL(CAST(lpep_dropoff_datetime AS STRING), ''), '|',
          IFNULL(CAST(vendorid AS STRING), ''), '|',
          IFNULL(CAST(pulocationid AS STRING), ''), '|',
          IFNULL(CAST(dolocationid AS STRING), ''), '|',
          IFNULL(CAST(passenger_count AS STRING), ''), '|',
          IFNULL(CAST(trip_distance AS STRING), ''), '|',
          IFNULL(CAST(fare_amount AS STRING), ''), '|',
          IFNULL(CAST(total_amount AS STRING), ''), '|',
          IFNULL(CAST(payment_type AS STRING), '')
        )
      )) AS event_id,
      TIMESTAMP(lpep_pickup_datetime) AS pickup_datetime,
      TIMESTAMP(lpep_dropoff_datetime) AS dropoff_datetime,
      SAFE_CAST(vendorid AS INT64) AS vendor_id,
      SAFE_CAST(ratecodeid AS INT64) AS rate_code_id,
      SAFE_CAST(pulocationid AS INT64) AS pickup_location_id,
      SAFE_CAST(dolocationid AS INT64) AS dropoff_location_id,
      SAFE_CAST(passenger_count AS INT64) AS passenger_count,
      SAFE_CAST(trip_distance AS FLOAT64) AS trip_distance,
      SAFE_CAST(fare_amount AS FLOAT64) AS fare_amount,
      SAFE_CAST(extra AS FLOAT64) AS extra,
      SAFE_CAST(mta_tax AS FLOAT64) AS mta_tax,
      SAFE_CAST(tip_amount AS FLOAT64) AS tip_amount,
      SAFE_CAST(tolls_amount AS FLOAT64) AS tolls_amount,
      SAFE_CAST(ehail_fee AS FLOAT64) AS ehail_fee,
      SAFE_CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
      SAFE_CAST(total_amount AS FLOAT64) AS total_amount,
      SAFE_CAST(payment_type AS INT64) AS payment_type,
      SAFE_CAST(trip_type AS INT64) AS trip_type,
      SAFE_CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
      'BATCH' AS data_source,
      TIMESTAMP(loaded_at) AS ingestion_time
    FROM `{staging_table}`
    WHERE EXTRACT(YEAR FROM lpep_pickup_datetime) = {year}
      AND EXTRACT(MONTH FROM lpep_pickup_datetime) = {month}

    UNION ALL

    -- 2. Data Streaming (Juni & Juli)
    -- Kolom di bawah mengikuti schema ASLI staging stream table (snake_case --
    -- lihat sql/create_staging_stream.sql & dags/scripts/event_schema.json),
    -- BUKAN kolom gaya batch (lpep pickup/dropoff datetime, PascalCase-style
    -- vendor/location id names dari NYC TLC raw file).
    -- Mapping ke kolom curated: ratecode_id (staging) -> rate_code_id (curated),
    -- sisanya nama sudah sama.
    --
    -- Filter periode pakai pickup_datetime (waktu bisnis trip), bukan
    -- ingestion_time (waktu Pub/Sub diterima), supaya konsisten dengan branch
    -- batch di atas dan dengan desain curated table (PARTITION BY pickup_date).
    -- ingestion_time tetap dipakai sebagai kolom metadata (data_source lineage
    -- & dedup ordering di bawah), bukan untuk filter period.
    --
    -- Tidak ada logic dedup tambahan di sini: event_id dari publisher.py sudah
    -- deterministic (UUID5 dari business_key), sehingga redelivery Pub/Sub
    -- at-least-once menghasilkan event_id identik dan sudah tertangani oleh
    -- ROW_NUMBER() OVER (PARTITION BY event_id ...) di bawah -- sama untuk
    -- kedua source (BATCH & STREAMING).
    SELECT
      event_id,
      TIMESTAMP(pickup_datetime) AS pickup_datetime,
      TIMESTAMP(dropoff_datetime) AS dropoff_datetime,
      SAFE_CAST(vendor_id AS INT64) AS vendor_id,
      SAFE_CAST(ratecode_id AS INT64) AS rate_code_id,
      SAFE_CAST(pickup_location_id AS INT64) AS pickup_location_id,
      SAFE_CAST(dropoff_location_id AS INT64) AS dropoff_location_id,
      SAFE_CAST(passenger_count AS INT64) AS passenger_count,
      SAFE_CAST(trip_distance AS FLOAT64) AS trip_distance,
      SAFE_CAST(fare_amount AS FLOAT64) AS fare_amount,
      SAFE_CAST(extra AS FLOAT64) AS extra,
      SAFE_CAST(mta_tax AS FLOAT64) AS mta_tax,
      SAFE_CAST(tip_amount AS FLOAT64) AS tip_amount,
      SAFE_CAST(tolls_amount AS FLOAT64) AS tolls_amount,
      SAFE_CAST(ehail_fee AS FLOAT64) AS ehail_fee,
      SAFE_CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
      SAFE_CAST(total_amount AS FLOAT64) AS total_amount,
      SAFE_CAST(payment_type AS INT64) AS payment_type,
      SAFE_CAST(trip_type AS INT64) AS trip_type,
      SAFE_CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
      'STREAMING' AS data_source,
      ingestion_time AS ingestion_time
    FROM `{staging_stream_table}`
    WHERE EXTRACT(YEAR FROM TIMESTAMP(pickup_datetime)) = {year}
      AND EXTRACT(MONTH FROM TIMESTAMP(pickup_datetime)) = {month}
  ),

  transformed_data AS (
    SELECT
      s.event_id,
      s.data_source,
      s.ingestion_time,
      
      -- Pickup Timestamps & Date Dimensions
      s.pickup_datetime,
      DATE(s.pickup_datetime) AS pickup_date,
      FORMAT_DATE('%A', DATE(s.pickup_datetime)) AS pickup_day_name,
      EXTRACT(HOUR FROM s.pickup_datetime) AS pickup_hour,
      EXTRACT(DAYOFWEEK FROM s.pickup_datetime) IN (1, 7) AS is_weekend,
      
      -- Dropoff & Duration
      s.dropoff_datetime,
      TIMESTAMP_DIFF(s.dropoff_datetime, s.pickup_datetime, MINUTE) AS trip_duration_minutes,
      
      -- Vendor & Rate Code
      s.vendor_id,
      s.rate_code_id,
      
      -- Location & Lookup Join (location_id, borough, zone dari load_lookup.py)
      s.pickup_location_id,
      pu.borough AS pickup_borough,
      pu.zone AS pickup_zone,
      s.dropoff_location_id,
      do.borough AS dropoff_borough,
      do.zone AS dropoff_zone,
      
      -- Metrics & Fares
      s.passenger_count,
      s.trip_distance,
      s.fare_amount,
      s.extra,
      s.mta_tax,
      s.tip_amount,
      s.tolls_amount,
      s.ehail_fee,
      s.improvement_surcharge,
      s.total_amount,
      
      -- Payment & Trip Type Description
      s.payment_type,
      CASE s.payment_type
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Other'
      END AS payment_type_desc,
      s.trip_type,
      s.congestion_surcharge
    FROM combined_staging s
    LEFT JOIN `{taxi_zone_table}` pu ON s.pickup_location_id = pu.location_id
    LEFT JOIN `{taxi_zone_table}` do ON s.dropoff_location_id = do.location_id
    WHERE s.pickup_datetime IS NOT NULL
      AND s.dropoff_datetime > s.pickup_datetime
      AND s.trip_distance >= 0
      AND s.fare_amount >= 0
  )

  -- Deduplikasi internal staging
  -- event_id sekarang SELALU terisi untuk kedua source (BATCH: hash deterministic di atas,
  -- STREAMING: event_id asli dari staging stream), sehingga fallback composite key
  -- (pickup_datetime|pickup_location_id|fare_amount) yang berisiko collide antar trip
  -- berbeda tidak lagi diperlukan.
  SELECT * EXCEPT(row_num) FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(
        PARTITION BY event_id
        ORDER BY ingestion_time DESC
      ) AS row_num
    FROM transformed_data
  ) WHERE row_num = 1
) S

-- Matching Key untuk MERGE ke Curated Table
ON T.event_id = S.event_id

WHEN MATCHED THEN
  UPDATE SET
    T.dropoff_datetime = S.dropoff_datetime,
    T.trip_duration_minutes = S.trip_duration_minutes,
    T.pickup_day_name = S.pickup_day_name,
    T.pickup_hour = S.pickup_hour,
    T.is_weekend = S.is_weekend,
    T.passenger_count = S.passenger_count,
    T.trip_distance = S.trip_distance,
    T.fare_amount = S.fare_amount,
    T.total_amount = S.total_amount,
    T.payment_type = S.payment_type,
    T.payment_type_desc = S.payment_type_desc,
    T.ingestion_time = S.ingestion_time

WHEN NOT MATCHED THEN
  INSERT (
    event_id, data_source, ingestion_time, pickup_datetime, pickup_date,
    pickup_day_name, pickup_hour, is_weekend, dropoff_datetime, trip_duration_minutes,
    vendor_id, rate_code_id, pickup_location_id, pickup_borough, pickup_zone,
    dropoff_location_id, dropoff_borough, dropoff_zone, passenger_count,
    trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    ehail_fee, improvement_surcharge, total_amount, payment_type, payment_type_desc,
    trip_type, congestion_surcharge
  )
  VALUES (
    S.event_id, S.data_source, S.ingestion_time, S.pickup_datetime, S.pickup_date,
    S.pickup_day_name, S.pickup_hour, S.is_weekend, S.dropoff_datetime, S.trip_duration_minutes,
    S.vendor_id, S.rate_code_id, S.pickup_location_id, S.pickup_borough, S.pickup_zone,
    S.dropoff_location_id, S.dropoff_borough, S.dropoff_zone, S.passenger_count,
    S.trip_distance, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount,
    S.ehail_fee, S.improvement_surcharge, S.total_amount, S.payment_type, S.payment_type_desc,
    S.trip_type, S.congestion_surcharge
  );