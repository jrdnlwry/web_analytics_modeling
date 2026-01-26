-- LTV by Acquisition Channel

--For each acquisition channel, compute average user LTV using all orders linked to that user.

WITH base AS (

  SELECT
    event_date,
    user_pseudo_id,
    event_name,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    (
      SELECT
        value.int_value
      FROM UNNEST (event_params)
      WHERE KEY = "ga_session_id"
    ) AS session_id,
    CONCAT(user_pseudo_id, "_",   (SELECT value.int_value FROM UNNEST (event_params) WHERE KEY = "ga_session_id")) AS u_id,
    traffic_source.medium,
    ecommerce.purchase_revenue,
    ecommerce.transaction_id

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
    AND event_name = "purchase"

),

aov_tbl AS (

  SELECT
    SUM(purchase_revenue) AS total_revenue,
    COUNT(transaction_id) AS num_orders,
    ROUND(SUM(purchase_revenue) / COUNT(transaction_id), 2) AS AOV,
    medium
  FROM base
  GROUP BY medium

),

purchase_freq AS (

  SELECT
    COUNT(transaction_id) AS num_orders,
    COUNT(DISTINCT user_pseudo_id) AS num_orders,
    ROUND(COUNT(transaction_id) / COUNT(DISTINCT user_pseudo_id), 2) AS purchase_frequency,
    medium

  FROM base
  GROUP BY medium

)
# LTV calculation
SELECT
  a.AOV,
  a.medium,
  p.purchase_frequency,
  ROUND((a.AOV * p.purchase_frequency), 2) AS LTV 

FROM purchase_freq AS p
JOIN aov_tbl AS a
  ON p.medium = a.medium