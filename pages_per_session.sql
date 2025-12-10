----------------------------------------------------------------------------->
--- Page Views Per Session

-- Pages per session: total page_view events / total session_start events

"""
Website metric showing the average number of pages a user views during a single visit (session) to a website.

This is calculated by dividing total pageviews by total sessions; it indicates engagement, with higher numbers suggesting users are exploring more content.

"""

WITH base as (

  SELECT
    COUNTIF(event_name = "session_start") AS sessions,
    COUNTIF(event_name = "page_view") AS page_views
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'

)

SELECT
  ROUND(SAFE_DIVIDE(page_views, sessions), 2) AS pages_per_session
FROM base