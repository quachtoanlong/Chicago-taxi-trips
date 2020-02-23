SELECT 
  unique_key,
  taxi_id,
  trip_start_timestamp,
  EXTRACT(HOUR FROM trip_start_timestamp) as start_hour,
  EXTRACT(DAYOFWEEK FROM trip_start_timestamp) as start_weekday,
  EXTRACT(MONTH FROM trip_start_timestamp) as start_month,
  EXTRACT(QUARTER FROM trip_start_timestamp) as start_quarter,
  
  trip_end_timestamp,
  trip_seconds,
  trip_miles,
  pickup_community_area,
  dropoff_community_area,
  fare,
  tips,
  tolls,
  extras,
  trip_total,
  payment_type,
  company,
  case when tips > 0 then 1 else 0 end as tip_status

FROM `portfolio-66520.chicago_taxi_trips.taxi_trips`
WHERE payment_type = "Credit Card"
  AND RAND() < 100000/(SELECT COUNT(*) FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` WHERE payment_type = "Credit Card")
