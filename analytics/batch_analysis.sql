-- ============================================================
-- Capstone Project 3
-- Batch Analytical Query
-- Dataset : NYC Green Taxi
-- Period  : April & May 2026
-- ============================================================

-- Query 1: Perbandingan performa April vs May 2026

SELECT
    DATE_TRUNC(DATE(pickup_datetime), MONTH) AS trip_month,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_trip_revenue,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(passenger_count), 2) AS avg_passenger_count
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH'
  AND DATE(pickup_datetime) >= '2026-04-01'
  AND DATE(pickup_datetime) < '2026-06-01'
GROUP BY trip_month
ORDER BY trip_month;


-- Query 2: Top 10 pickup zones berdasarkan jumlah trip
-- selama April-May 2026.

SELECT
    z.zone AS pickup_zone,
    z.borough AS pickup_borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(c.total_amount), 2) AS total_revenue,
    ROUND(AVG(c.trip_distance), 2) AS avg_trip_distance
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated` c
LEFT JOIN `jcdeah-009.cp3_aninditas_staging.taxi_zone_mapping_staging` z
    ON c.pickup_location_id = z.location_id
WHERE c.data_source = 'BATCH'
  AND DATE(c.pickup_datetime) >= '2026-04-01'
  AND DATE(c.pickup_datetime) < '2026-06-01'
GROUP BY
    pickup_zone,
    pickup_borough
ORDER BY total_trips DESC
LIMIT 10;