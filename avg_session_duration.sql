--------------------------------------------------------------------------------------------------->
-- Average Session Duration: conceptual calculation
--------------------------------------------------------------------------------------------------->

"""
### Metric Background

Average Session Duration is defined as the average length of time a visitor spends on a website or app during a single visit.

We calculate this metric by dividing the total duration of all sessions by the number of sessions during a period of time.

NOTE: we also want to create an identifier to determine sessions by User. We achieve this by creating an identifier using user ID and session ID columns.

**Computation:**

1. numerator: total duration of all sessions
2. denominator: count of the number of sessions during our time frame
"""

WITH base AS (

  SELECT
    user_pseudo_id,
      (
        SELECT value.int_value
        FROM UNNEST(event_params)
        WHERE key = "ga_session_id") AS ga_session_id,
        --TIMESTAMP_SECONDS(CAST(CAST(event_timestamp as INT64)/1000000 as INT64)) as time_converted
        -- updated timestamp conversion
        TIMESTAMP_MICROS(event_timestamp) AS time_converted
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'

),

duration AS (

  SELECT
    MIN(time_converted) AS start_time,
    MAX(time_converted) AS end_time,
    CONCAT(user_pseudo_id, "_", ga_session_id) AS u_id,
    TIMESTAMP_DIFF(MAX(time_converted), MIN(time_converted), SECOND) AS session_duration
  FROM base
  WHERE ga_session_id IS NOT NULL -- caps sessions greater than 30 minutes
  GROUP BY u_id

)


SELECT
  SUM(session_duration) / 60 AS duration_minutes,
  COUNT(u_id) AS number_sessions,
  ROUND(SAFE_DIVIDE(SUM(session_duration), COUNT(u_id)) / 60, 2),   -- incorporate safe_divide
  --ROUND((SUM(session_duration) / 60)/(COUNT(u_id)), 2) AS avg_session_duration_min
  ROUND(AVG(session_duration) / 60, 2) AS avg_session_duration_minutes -- stylistically cleaner interpretation
FROM duration
WHERE session_duration <= 1800