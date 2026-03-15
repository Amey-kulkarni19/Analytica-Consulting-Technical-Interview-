-- 03_answer.sql (DuckDB-correct)
-- Creates: top_25_facilities

CREATE OR REPLACE TABLE top_25_facilities AS
WITH bounds AS (
  SELECT MAX(month) AS max_month FROM mart_facility_monthly
),
features AS (
  SELECT
    wdid,
    ANY_VALUE(facility_name) AS facility_name,
    ANY_VALUE(place_county) AS place_county,
    ANY_VALUE(facility_region) AS facility_region,

    SUM(CASE WHEN month >= (SELECT max_month FROM bounds) - INTERVAL '12 months'
             THEN enforcement_count ELSE 0 END) AS actions_12mo,
    SUM(CASE WHEN month >= (SELECT max_month FROM bounds) - INTERVAL '12 months'
             THEN total_assessment_sum ELSE 0 END) AS penalty_12mo,

    SUM(CASE WHEN month >= (SELECT max_month FROM bounds) - INTERVAL '3 months'
             THEN enforcement_count ELSE 0 END) AS actions_3mo,

    AVG(CASE WHEN month <  (SELECT max_month FROM bounds) - INTERVAL '3 months'
              AND month >= (SELECT max_month FROM bounds) - INTERVAL '15 months'
             THEN enforcement_count ELSE NULL END) AS baseline_avg,
    STDDEV_SAMP(CASE WHEN month <  (SELECT max_month FROM bounds) - INTERVAL '3 months'
                      AND month >= (SELECT max_month FROM bounds) - INTERVAL '15 months'
                     THEN enforcement_count ELSE NULL END) AS baseline_std,

    MAX(month) AS last_action_month
  FROM mart_facility_monthly
  GROUP BY wdid
),
scored AS (
  SELECT
    *,
    EXP(-ABS(DATE_DIFF('day', last_action_month, (SELECT max_month FROM bounds))) / 90.0) AS recency_score,
    LN(1 + actions_12mo) AS freq_score,
    LN(1 + penalty_12mo) AS severity_score,

    CASE
      WHEN baseline_std IS NOT NULL AND baseline_std > 0
       AND actions_3mo/3.0 >= 3
       AND ((actions_3mo/3.0) - baseline_avg) / baseline_std >= 2
        THEN 1
      WHEN baseline_avg IS NOT NULL AND baseline_avg > 0
       AND actions_3mo/3.0 >= 3
       AND (actions_3mo/3.0) >= 2 * baseline_avg
        THEN 1
      ELSE 0
    END AS anomaly_flag
  FROM features
),
final AS (
  SELECT
    wdid,
    facility_name,
    place_county,
    facility_region,
    actions_12mo,
    penalty_12mo,
    actions_3mo,
    baseline_avg,
    baseline_std,
    anomaly_flag,

    (0.35 * recency_score) + (0.35 * freq_score) + (0.25 * severity_score) + (0.05 * anomaly_flag) AS priority_score,

    CASE
      WHEN anomaly_flag = 1 THEN 'recent spike vs baseline (3mo vs prior 12mo)'
      ELSE NULL
    END AS anomaly_reason
  FROM scored
)
SELECT *
FROM final
ORDER BY priority_score DESC
LIMIT 25;