# DECLARE fromdate TIMESTAMP DEFAULT '2014-01-01 00:00:00';  -- dates for after 2013
DECLARE start_date STRING DEFAULT '20210101';
DECLARE end_date STRING DEFAULT '20210110';

WITH base AS (

  SELECT 
    *,
    CONCAT(user_id, "_", session_id) AS u_id
  FROM (

    SELECT
      event_date,
      TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
      user_pseudo_id AS user_id,
      (
        SELECT value.int_value
        FROM UNNEST(event_params)
        WHERE key = "ga_session_id"
      ) AS session_id,
      (
        SELECT value.string_value
        FROM UNNEST(event_params)
        WHERE key = "page_location"
      ) AS page

    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE event_date BETWEEN start_date AND end_date
      AND event_name IN ("page_view")

  )

),

ordered_path AS (

  SELECT 
    b.*,
    ROW_NUMBER() OVER( PARTITION BY b.u_id ORDER BY b.event_timestamp ASC) AS rn
  FROM base AS b

),

path_id AS (

  SELECT
    *,
    CONCAT(page, "_", page_2, "_", page_3) AS path
  FROM(

    SELECT
      *,
      LEAD(page) OVER(PARTITION BY u_id ORDER BY rn ASC) AS page_2,
      LEAD(page, 2) OVER(PARTITION BY u_id ORDER BY rn ASC) AS page_3
    FROM ordered_path

  ) WHERE rn = 1

)


SELECT
  path,
  COUNT(path) AS occurrence
FROM path_id
WHERE path IS NOT NULL
GROUP BY path
ORDER BY occurrence DESC



















