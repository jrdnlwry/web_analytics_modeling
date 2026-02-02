# base query
WITH base AS (

  SELECT
    event_date,
    user_pseudo_id,
    event_timestamp,
    TIMESTAMP_MICROS(event_timestamp) AS time_stamp,
    event_name
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
    AND event_name = "page_view"

), 

# enrichment layer -- create the week_timestamp variable
-- Parse the week variable out of the event_date

-- parse_week AS (

parse_week AS (

  SELECT
    event_date,
    user_pseudo_id,
    time_stamp,
    CONCAT(week_extract, "-", year_extract) AS week_parted
  FROM(

    SELECT 
      *,
      EXTRACT(ISOWEEK FROM DATE(time_stamp)) AS week_extract,
      EXTRACT(ISOYEAR FROM DATE(time_stamp)) AS year_extract
    FROM base

  )

),
daily_active_users AS (

  # calculate DAU

  SELECT
    COUNT(DISTINCT user_pseudo_id) AS DAU,
    event_date

  FROM parse_week
  GROUP BY event_date

),

weekly_active_users AS (

  SELECT
    COUNT(DISTINCT user_pseudo_id) AS WAU,
    week_parted
  FROM parse_week
  GROUP BY week_parted

)

SELECT 
  p.event_date,
  p.week_parted,
  d.DAU,
  w.WAU,
  ROUND(SAFE_DIVIDE(d.DAU, w.WAU), 2) AS stickiness

FROM parse_week AS p
JOIN daily_active_users AS d
  ON d.event_date = p.event_date
JOIN weekly_active_users w
  ON w.week_parted = p.week_parted
GROUP BY p.event_date, p.week_parted, d.DAU, w.WAU
ORDER BY p.event_date ASC










