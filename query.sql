-- ============================================================================
-- GDELT air-freight disruption feed — ONE scan per day.
-- Returns one raw row per deduped, aviation-covered disruption event for @run_date.
-- Severity, hub attribution, lane impact and z-scores are all computed downstream
-- in pandas (see fetch.py), so this query stays cheap and hubs stay config-driven.
--
-- Parameters (bound by fetch.py):
--   @run_date            DATE   — the day to score
--   @mention_window_days INT64  — extra days of coverage to sweep (ingest-lag tail)
--
-- Cost control: fetch.py sets maximum_bytes_billed, so this can never leave the
-- BigQuery free tier — a too-large scan errors out instead of billing.
-- ============================================================================

WITH aviation_articles AS (
  SELECT
    DocumentIdentifier AS url,
    SAFE_CAST(REGEXP_EXTRACT(V2Counts, r'KILL#([0-9]+)') AS INT64) AS fatalities_reported,
    V2Themes AS article_topics
  FROM `gdelt-bq.gdeltv2.gkg_partitioned`
  WHERE _PARTITIONDATE BETWEEN @run_date AND DATE_ADD(@run_date, INTERVAL @mention_window_days DAY)
    AND (
      V2Themes LIKE '%AVIATION%'          OR
      V2Themes LIKE '%AIRPORT%'           OR
      V2Themes LIKE '%ECON_SUPPLY_CHAIN%' OR
      V2Themes LIKE '%TRANSPORT%'         OR
      V2Themes LIKE '%NATURAL_DISASTER%'  OR
      V2Themes LIKE '%STRIKE%'
    )
),

aviation_mentions AS (
  SELECT
    m.GLOBALEVENTID,
    COUNT(DISTINCT m.MentionIdentifier) AS aviation_mention_count,
    COUNT(DISTINCT m.MentionSourceName) AS aviation_source_count,
    ROUND(AVG(m.Confidence), 2)         AS avg_confidence_pct,
    AVG(m.MentionDocTone)               AS avg_mention_tone,
    MIN(m.MentionTimeDate)              AS first_mention_time,
    MAX(a.fatalities_reported)          AS fatalities,
    ANY_VALUE(a.article_topics)         AS article_topics
  FROM `gdelt-bq.gdeltv2.eventmentions_partitioned` AS m
  INNER JOIN aviation_articles AS a
    ON m.MentionIdentifier = a.url
  WHERE m._PARTITIONDATE BETWEEN @run_date AND DATE_ADD(@run_date, INTERVAL @mention_window_days DAY)
  GROUP BY m.GLOBALEVENTID
),

event_data AS (
  SELECT
    GlobalEventID,
    PARSE_DATE('%Y%m%d', CAST(SQLDATE AS STRING)) AS event_date,
    EventRootCode,
    QuadClass,
    GoldsteinScale,
    NumMentions,
    Actor1Name,
    Actor2Name,
    ActionGeo_CountryCode AS action_location_country,
    ActionGeo_FullName    AS action_location_name,
    ActionGeo_Lat         AS event_latitude,
    ActionGeo_Long        AS event_longitude,
    SOURCEURL
  FROM `gdelt-bq.gdeltv2.events_partitioned`
  WHERE _PARTITIONDATE = @run_date
    AND EventRootCode IN ('14', '15', '16', '17', '18', '19', '20')
    AND ActionGeo_FullName IS NOT NULL
    AND ActionGeo_Lat IS NOT NULL
    AND ActionGeo_Long IS NOT NULL
),

joined AS (
  SELECT
    e.GlobalEventID,
    e.event_date,
    e.EventRootCode,
    e.QuadClass,
    e.GoldsteinScale,
    am.avg_mention_tone,
    am.aviation_mention_count,
    am.aviation_source_count,
    am.avg_confidence_pct,
    am.first_mention_time,
    COALESCE(am.fatalities, 0) AS fatalities,
    e.Actor1Name,
    e.Actor2Name,
    e.action_location_country,
    e.action_location_name,
    e.event_latitude,
    e.event_longitude,
    e.SOURCEURL,
    am.article_topics,
    ROW_NUMBER() OVER (
      PARTITION BY e.event_date, e.action_location_name, e.EventRootCode,
                   e.Actor1Name, e.Actor2Name
      ORDER BY am.aviation_mention_count DESC, e.NumMentions DESC
    ) AS dedup_rank
  FROM event_data AS e
  INNER JOIN aviation_mentions AS am
    ON e.GlobalEventID = am.GLOBALEVENTID
)

SELECT * EXCEPT(dedup_rank)
FROM joined
WHERE dedup_rank = 1
