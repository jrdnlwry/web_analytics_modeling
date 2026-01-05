-- For each path, compute average time on page using the difference between the current and next event within a session. Cap at 900 seconds.

-- Grain level: each row represents a session, which we will take and aggregate to find our metric
-- Each row represents the start and end time of a session within GA.


-- We want to find the min and max time stamp for an event on a given page for a given user's session. We then take the difference between the two data points to get our duration.

-- Once we have the duration across each session for each page we calculate the avg on a page level to derive our avg time on page.

-- Our base query will grab the necessary columns

-- enrichment: we determine the min and max time stamps for a given event

-- agg layer: we perform our aggregation within this layer


WITH base AS (

  SELECT
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_name,
    user_pseudo_id,
    event_date,
    (
      SELECT
        value.int_value
      FROM UNNEST (event_params)
      WHERE key = "ga_session_id"
    ) AS session_id,
    (
      SELECT
        value.string_value
      FROM UNNEST (event_params)
      WHERE key = "page_location"
    ) AS page

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'

),

duration_layer AS (

  SELECT
    event_date,
    page,
    CONCAT(user_pseudo_id, "_", session_id) AS u_id,
    min(event_timestamp) AS start_time,
    max(event_timestamp) AS end_time,
    TIMESTAMP_DIFF(max(event_timestamp), min(event_timestamp), SECOND) AS duration
  FROM base
  GROUP BY event_date, page, u_id

)

SELECT
  event_date,
  AVG(duration) / 60 AS avg_duration_min,
  page
FROM duration_layer
WHERE duration <= 900
GROUP BY event_date, page 
ORDER BY event_date ASC,
         AVG(duration) DESC


