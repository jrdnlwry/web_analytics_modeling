----------------------------------------------------------------------------------------->
-- Conceptual Funnel Work according to Heap's documentation

-- The funnel below can be used to mirror the funnel in Heap’s dashboard.

-- It calculates the number of users who have completed Step 1 within the time frame, the number of users who have completed Step 2, and then Step 3.

-- It's both sequential and time-constrained, though there are many modifications you may wish to make, as shown below.

------------------------------------------------------------------------------------------->


WITH e1 AS (

  SELECT 
    DISTINCT user_pseudo_id,
    1 AS step_1,
    MIN(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)) AS step_1_time
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE DATE_TRUNC(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX), WEEK) >= DATE '2021-01-01'
      AND DATE_TRUNC(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX), WEEK) <= DATE '2021-01-31'
      AND event_name = "page_view"
  GROUP BY 1

),

e2 AS (

  SELECT 
    e1.user_pseudo_id,
    1 AS step_2,
    MIN(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)) AS step_2_time
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e2
    JOIN e1
      ON e1.user_pseudo_id = e2.user_pseudo_id
      WHERE PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN step_1_time
        AND DATE '2021-01-31'
        AND event_name = "add_to_cart"
  GROUP BY 1

),
e3 AS (

  SELECT
    e3.user_pseudo_id,
    1 AS step_3,
    MIN(_TABLE_SUFFIX) AS step_3_time
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e3
  JOIN e2
    ON e2.user_pseudo_id = e3.user_pseudo_id
      WHERE PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN step_2_time
        AND DATE '2021-01-31'
        AND event_name = "purchase"
  GROUP BY 1

)


  -- SELECT
  --   e1.user_pseudo_id,
  --   step_1,
  --   step_1_time,
  --   step_2,
  --   step_2_time,
  --   step_3,
  --   step_3_time
  -- FROM e1
  -- LEFT JOIN e2 ON e1.user_pseudo_id = e2.user_pseudo_id
  -- LEFT JOIN e3 ON e2.user_pseudo_id = e3.user_pseudo_id

SELECT
  SUM(step_1) AS step_1,
  SUM(step_2) AS step_2,
  SUM(step_3) AS step_3
  -- ROUND(SUM(step_2)/SUM(step_1)::DECIMAL, 2) as step_1to2_conversion,
  -- ROUND(SUM(step_3)/SUM(step_2)::DECIMAL,2) as step_2to3_conversion,
  -- ROUND(SUM(step_3)/SUM(step_1)::DECIMAL, 2) as overall_conversion

FROM(

  SELECT
    e1.user_pseudo_id,
    step_1,
    step_1_time,
    step_2,
    step_2_time,
    step_3,
    step_3_time
  FROM e1
  LEFT JOIN e2 ON e1.user_pseudo_id = e2.user_pseudo_id
  LEFT JOIN e3 ON e2.user_pseudo_id = e3.user_pseudo_id

)