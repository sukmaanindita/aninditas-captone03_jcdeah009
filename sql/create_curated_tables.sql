CREATE TABLE IF NOT EXISTS `{curated_dataset}.{curated_table}` (
  -- Metadata
  event_id STRING,
  data_source STRING,
  ingestion_time TIMESTAMP,
  
  -- Trip Timestamps & Date Dimensions
  pickup_datetime TIMESTAMP,
  pickup_date DATE,
  pickup_day_name STRING,
  pickup_hour INT64,
  is_weekend BOOLEAN,
  dropoff_datetime TIMESTAMP,
  trip_duration_minutes INT64,
  
  -- Vendor & Rate Info
  vendor_id INT64,
  rate_code_id INT64,
  
  -- Location & Zone Info
  pickup_location_id INT64,
  pickup_borough STRING,
  pickup_zone STRING,
  dropoff_location_id INT64,
  dropoff_borough STRING,
  dropoff_zone STRING,
  
  -- Trip Metrics & Costs
  passenger_count INT64,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  ehail_fee FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  
  -- Payment & Trip Type Info
  payment_type INT64,
  payment_type_desc STRING,
  trip_type INT64,
  congestion_surcharge FLOAT64
)
PARTITION BY pickup_date
CLUSTER BY data_source, pickup_location_id, dropoff_location_id;