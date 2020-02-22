  #Question: which taxi company should a new drivers choose, bASed on how long did it take to earn 2000$


  #Build a taxi driver databASe
  #Assuming the driver first ride in this databASe is also the first ride in real life


  WITH 
  data AS (
  SELECT *
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  )

  ,pm_info AS (
  SELECT
      taxi_id, company, payment_type,
      count(unique_key) AS total_payment_count
    FROM data
    GROUP BY taxi_id, company, payment_type
  )

  ,p AS (
  SELECT
    pm_info.taxi_id,
    pm_info.company, 
    payment_type
    FROM pm_info
    inner join
    (SELECT
      taxi_id, 
      company, 
      max(total_payment_count) AS max_trip
    FROM pm_info
    GROUP BY taxi_id, company) AS max
    ON max.taxi_id = pm_info.taxi_id AND max.company = pm_info.company AND max_trip = total_payment_count
    )
    

  #Final data set containing basic information of each driver
  ,driver_data AS (  
  SELECT
    data.taxi_id,
    data.company,
    p.payment_type AS prefered_payment,
    min(trip_start_timestamp) AS first_trip,
    max(trip_end_timestamp) AS lASt_trip,
    sum(fare) AS total_fare,
    sum(trip_seconds) AS total_time,
    sum(trip_miles) AS total_miles,  
      
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` AS data
  INNER JOIN p ON
    p.taxi_id = data.taxi_id AND p.company = data.company
  GROUP BY 
    data.taxi_id,
    company,
    p.payment_type
  )

  #Now I'm interested in learning how much money most taxi drivers earn based from their companies. 
  #To see it, I create a percentile table for the fare earned by each company

  ,percentile AS (
  SELECT * FROM 
  (SELECT 
    company,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(10)] AS percentile_10,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(25)] AS percentile_25,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(50)] AS percentile_50,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(75)] AS percentile_75,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(90)] AS percentile_90,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(95)] AS percentile_95,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(99)] AS percentile_99,
    count(distinct taxi_id) as driver_count,
    sum(total_fare) as company_fare
  FROM driver_data 
  group by company

  UNION ALL
  SELECT
    "Total" as company,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(10)] AS percentile_10,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(25)] AS percentile_25,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(50)] AS percentile_50,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(75)] AS percentile_75,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(90)] AS percentile_90,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(95)] AS percentile_95,
    APPROX_QUANTILES(total_fare, 100)[OFFSET(99)] AS percentile_99,
    count(distinct taxi_id) as driver_count,
    sum(total_fare) as company_fare
  FROM driver_data 
  group by company)
  order by driver_count desc
  )

  #From this, we see that 75% of drivers was able to earn 2600$ or more. This is a not bad fare, from my developing-world perspective. 
  #Let's learn how long did it take a general driver to earn that much

  ,fare_data as (
  SELECT
    taxi_id,
    company,
    trip_start_timestamp,
    trip_end_timestamp,
    fare,
    tips,
    sum(fare) over (partition by taxi_id,company order by trip_end_timestamp) as cumulative_fare,
    sum(tips) over (partition by taxi_id,company order by trip_end_timestamp) as cumulative_tips  
  FROM data
  )


  ,udata as (
  SELECT distinct
    fare_data.taxi_id,
    fare_data.company,
    five_dollar_time,
    first_trip,
    ABS(TIMESTAMP_DIFF(five_dollar_time, first_trip, DAY)) as day_to_five
    FROM fare_data
    INNER JOIN ( SELECT
        taxi_id,
        company,
        min(trip_end_timestamp) as five_dollar_time
        FROM fare_data
        WHERE cumulative_fare > 100
        group by taxi_id, company) AS p
      ON p.taxi_id = fare_data.taxi_id and p.company = fare_data.company
    LEFT JOIN driver_data as driver
      ON driver.taxi_id = fare_data.taxi_id and driver.company = fare_data.company
    order by taxi_id 
      
  )

  SELECT 
    company,
    APPROX_QUANTILES(day_to_five, 100)[OFFSET(50)] AS percentile_50,
    APPROX_QUANTILES(day_to_five, 100)[OFFSET(75)] AS percentile_75,
    APPROX_QUANTILES(day_to_five, 100)[OFFSET(90)] AS percentile_90,
      count(taxi_id) AS driver_number
    FROM udata
    GROUP BY company
    ORDER BY driver_number

