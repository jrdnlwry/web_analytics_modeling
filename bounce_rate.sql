---------------------------------------------------------------------------------------------------->
-- Bounce Rate Conceputal Work
---------------------------------------------------------------------------------------------------->

"""
Bounce Rate: 

The percentage of sessions where a visitor views only one apage on a website before leaving, without any interaction.

GA4's definition of a bounced session is:

A session is considered engaged if it meets any of these:
	* >= seconds of engagement time
	* >= 2 page/screen views
	* Or contains a conversion event

"""

WITH base AS (

  SELECT
    user_pseudo_id,
      (
        SELECT value.int_value
        FROM UNNEST(event_params)
        WHERE key = "ga_session_id") AS ga_session_id,
    TIMESTAMP_MICROS(event_timestamp) AS time_converted,
    event_name

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
  GROUP BY u_id
  HAVING TIMESTAMP_DIFF(MAX(time_converted), MIN(time_converted), SECOND) < 1800

),

-- page views per session calc
event_count AS (

  SELECT
    COUNTIF(event_name="page_view") AS page_views,
    COUNTIF(event_name="purchase") AS purchases,
    CONCAT(user_pseudo_id, "_", ga_session_id) AS u_id
  FROM base
  GROUP BY u_id

)

SELECT
  SUM(bounced) / count(*) AS bounce_rate,
  ROUND(SAFE_DIVIDE(SUM(bounced), count(*)), 4) AS bounce_rate_pct
FROM (

  SELECT
    d.u_id,
    d.session_duration,
    e.page_views,
    e.purchases,
    CASE
        WHEN d.session_duration < 10 AND e.page_views < 2 AND e.purchases = 0 THEN 1 -- 1 indicates bounced session
        ELSE 0
    END AS bounced
  FROM duration AS d
  LEFT JOIN event_count AS e
    ON d.u_id = e.u_id

)