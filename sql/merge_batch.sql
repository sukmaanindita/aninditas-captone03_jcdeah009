-- Upsert payment types into payment_type_dim
MERGE `{mart_dataset}.payment_type_dim` T
USING (
  SELECT DISTINCT
    CASE CAST(payment_type AS STRING)
      WHEN '1' THEN 'credit'
      WHEN '2' THEN 'cash'
      WHEN '3' THEN 'no_charge'
      WHEN '4' THEN 'dispute'
      WHEN '5' THEN 'unknown'
      WHEN '6' THEN 'voided'
      ELSE 'unknown'
    END AS payment_type_code,
    CASE CAST(payment_type AS STRING)
      WHEN '1' THEN 'Credit Card'
      WHEN '2' THEN 'Cash'
      WHEN '3' THEN 'No Charge'
      WHEN '4' THEN 'Dispute'
      WHEN '5' THEN 'Unknown'
      WHEN '6' THEN 'Voided Trip'
      ELSE 'Unknown'
    END AS payment_type_label
  FROM `{staging_table}`
) S
ON T.payment_type_code = S.payment_type_code
WHEN NOT MATCHED THEN
  INSERT (payment_type_code, payment_type_label, loaded_at)
  VALUES (S.payment_type_code, S.payment_type_label, CURRENT_TIMESTAMP());

-- Merge staging into ride_fact using business_key to avoid duplicates
MERGE `{mart_dataset}.ride_fact` T
USING (
  SELECT
    CONCAT(
      FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', tpep_pickup_datetime), '|',
      FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', tpep_dropoff_datetime), '|',
      CAST(passenger_count AS STRING), '|', CAST(trip_distance AS STRING), '|', CAST(total_amount AS STRING), '|',
      CASE CAST(payment_type AS STRING)
        WHEN '1' THEN 'credit' WHEN '2' THEN 'cash' WHEN '3' THEN 'no_charge' WHEN '4' THEN 'dispute' WHEN '5' THEN 'unknown' WHEN '6' THEN 'voided' ELSE 'unknown' END
    ) AS business_key,
    GENERATE_UUID() AS ride_id,
    TIMESTAMP(tpep_pickup_datetime) AS pickup_datetime,
    TIMESTAMP(tpep_dropoff_datetime) AS dropoff_datetime,
    CAST(passenger_count AS INT64) AS passenger_count,
    CAST(trip_distance AS FLOAT64) AS trip_distance,
    CASE CAST(payment_type AS STRING)
      WHEN '1' THEN 'credit'
      WHEN '2' THEN 'cash'
      WHEN '3' THEN 'no_charge'
      WHEN '4' THEN 'dispute'
      WHEN '5' THEN 'unknown'
      WHEN '6' THEN 'voided'
      ELSE 'unknown'
    END AS payment_type_code,
    CAST(fare_amount AS FLOAT64) AS fare_amount,
    CAST(tip_amount AS FLOAT64) AS tip_amount,
    CAST(total_amount AS FLOAT64) AS total_amount,
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS month,
    DATE(tpep_pickup_datetime) AS day,
    CONCAT(CAST(EXTRACT(YEAR FROM tpep_pickup_datetime) AS STRING), '-', LPAD(CAST(EXTRACT(MONTH FROM tpep_pickup_datetime) AS STRING), 2, '0')) AS batch_id,
    CURRENT_TIMESTAMP() AS loaded_at
  FROM `{staging_table}`
) S
ON T.business_key = S.business_key
WHEN MATCHED THEN
  UPDATE SET
    ride_id = S.ride_id,
    pickup_datetime = S.pickup_datetime,
    dropoff_datetime = S.dropoff_datetime,
    passenger_count = S.passenger_count,
    trip_distance = S.trip_distance,
    payment_type_code = S.payment_type_code,
    fare_amount = S.fare_amount,
    tip_amount = S.tip_amount,
    total_amount = S.total_amount,
    year = S.year,
    month = S.month,
    day = S.day,
    batch_id = S.batch_id,
    loaded_at = S.loaded_at
WHEN NOT MATCHED THEN
  INSERT (business_key, ride_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, payment_type_code, fare_amount, tip_amount, total_amount, year, month, day, batch_id, loaded_at)
  VALUES (S.business_key, S.ride_id, S.pickup_datetime, S.dropoff_datetime, S.passenger_count, S.trip_distance, S.payment_type_code, S.fare_amount, S.tip_amount, S.total_amount, S.year, S.month, S.day, S.batch_id, S.loaded_at);
