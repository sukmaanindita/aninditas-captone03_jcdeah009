-- Fact: hourly demand pattern by day-of-week.
-- Grain: 1 row per (day_of_week_num, hour_of_day).
--
-- Source: `{curated_table}` (green_tripdata_curated) only -- trip/event
-- data, REQUIRED to come from the curated layer. No location dimension join
-- needed at this grain.
--
-- No `data_source` filter -> both BATCH and STREAMING trips are included
-- automatically.
--
-- All date/time aggregation uses `pickup_datetime` (business trip time),
-- never `ingestion_time`.
--
-- No extra dedup logic needed: `green_tripdata_curated` already guarantees
-- one row per `event_id` (curation MERGE), so this aggregation can never
-- double-count a trip.
--
-- CREATE OR REPLACE TABLE = full atomic replace every run -> rerun-safe by
-- construction.
CREATE OR REPLACE TABLE `{mart_table}`
CLUSTER BY day_of_week_num
AS
SELECT
  FORMAT_DATE('%A', DATE(c.pickup_datetime)) AS day_of_week,
  EXTRACT(DAYOFWEEK FROM DATE(c.pickup_datetime)) AS day_of_week_num,
  EXTRACT(HOUR FROM c.pickup_datetime) AS hour_of_day,
  COUNT(c.event_id) AS total_trips,
  SUM(c.passenger_count) AS total_passengers,
  ROUND(AVG(c.fare_amount), 2) AS avg_fare_amount,
  ROUND(AVG(c.congestion_surcharge), 2) AS avg_congestion_surcharge,
  CURRENT_TIMESTAMP() AS updated_at
FROM `{curated_table}` c
WHERE c.pickup_datetime IS NOT NULL
GROUP BY 1, 2, 3;
