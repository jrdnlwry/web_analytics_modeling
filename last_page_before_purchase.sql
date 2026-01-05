WITH base AS (

  SELECT
    TIMESTAMP_MICROS(event_timestamp) AS date,
    event_name,
    user_pseudo_id,
    (
      SELECT
        value.int_value
      FROM UNNEST(event_params)
        WHERE key = "ga_session_id"

    ) AS ga_session_id,
    (
      SELECT
        value.string_value
      FROM UNNEST(event_params)
        WHERE key = "page_location"
    ) AS page_location,
    CONCAT(user_pseudo_id, "_",   (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id")) AS u_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131' AND REGEXP_CONTAINS(event_name, r"purchase|page_view")

),

-- identify on purchase events
purchase AS (

  SELECT
    p.date,
    p.u_id,
    p.event_name

  FROM base AS p
  WHERE event_name = "purchase"

)

SELECT *
FROM base
WHERE u_id IN (SELECT p.u_id FROM purchase AS p)
ORDER BY u_id, date ASC