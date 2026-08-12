-- Validasi Row Count untuk Data Source BATCH pada Curated Layer
SELECT
  EXTRACT(YEAR FROM pickup_datetime) AS year,
  EXTRACT(MONTH FROM pickup_datetime) AS month,
  data_source,
  COUNT(*) AS row_count
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH'
  AND DATE(pickup_datetime) BETWEEN '2026-04-01' AND '2026-05-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2;


-- Hanya Bulan April dan Mei 2026 yang diambil untuk validasi, karena data BATCH hanya tersedia untuk periode tersebut.
SELECT
  MIN(DATE(pickup_datetime)) AS min_date,
  MAX(DATE(pickup_datetime)) AS max_date,
  COUNT(*) AS total_rows
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH';


-- Cek event_id NULL untuk memastikan tidak ada duplikasi atau kehilangan data.
SELECT
  data_source,
  COUNT(*) AS total_rows,
  COUNTIF(event_id IS NULL) AS null_event_id,
  COUNTIF(event_id IS NOT NULL) AS populated_event_id
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH'
GROUP BY data_source;


-- Cek duplikasi event_id untuk memastikan integritas data.
SELECT
  event_id,
  COUNT(*) AS cnt
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH'
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;


-- Cek duplikasi berdasarkan period
SELECT
  event_id,
  COUNT(*) AS cnt
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH'
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;


-- Cek NULL pada kolom penting untuk memastikan tidak ada data yang hilang.
SELECT
  COUNTIF(event_id IS NULL) AS null_event_id,
  COUNTIF(pickup_datetime IS NULL) AS null_pickup_datetime,
  COUNTIF(dropoff_datetime IS NULL) AS null_dropoff_datetime,
  COUNTIF(pickup_location_id IS NULL) AS null_pickup_location_id,
  COUNTIF(dropoff_location_id IS NULL) AS null_dropoff_location_id,
  COUNTIF(trip_distance IS NULL) AS null_trip_distance,
  COUNTIF(fare_amount IS NULL) AS null_fare_amount,
  COUNTIF(total_amount IS NULL) AS null_total_amount,
  COUNTIF(payment_type IS NULL) AS null_payment_type
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH'
  AND DATE(pickup_datetime) BETWEEN '2026-04-01' AND '2026-05-31';


-- Cek invalid values
SELECT
  COUNTIF(trip_distance < 0) AS negative_distance,
  COUNTIF(fare_amount < 0) AS negative_fare,
  COUNTIF(total_amount < 0) AS negative_total,
  COUNTIF(passenger_count <= 0) AS invalid_passenger_count,
  COUNTIF(dropoff_datetime < pickup_datetime) AS invalid_duration
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH'
  AND DATE(pickup_datetime) BETWEEN '2026-04-01' AND '2026-05-31';



SELECT
  EXTRACT(YEAR FROM pickup_datetime) AS year,
  EXTRACT(MONTH FROM pickup_datetime) AS month,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT event_id) AS distinct_event_ids,
  COUNTIF(event_id IS NULL) AS null_event_id,
  COUNTIF(pickup_datetime IS NULL) AS null_pickup,
  COUNTIF(dropoff_datetime IS NULL) AS null_dropoff,
  COUNTIF(trip_distance IS NULL) AS null_distance,
  COUNTIF(trip_distance < 0) AS invalid_distance,
  COUNTIF(fare_amount < 0) AS invalid_fare,
  COUNTIF(total_amount < 0) AS invalid_total,
  COUNTIF(passenger_count <= 0) AS invalid_passenger
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH'
  AND DATE(pickup_datetime) BETWEEN '2026-04-01' AND '2026-05-31'
GROUP BY 1, 2
ORDER BY 1, 2;


-- Cek Idempotency: Jalankan query ini beberapa kali untuk memastikan hasilnya konsisten dan tidak berubah.
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT event_id) AS distinct_event_ids
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH'
  AND DATE(pickup_datetime) BETWEEN '2026-04-01' AND '2026-05-31';