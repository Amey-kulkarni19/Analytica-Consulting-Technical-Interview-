-- =============================================================================
-- 03_answer.sql
-- Client question: "We can only review 25 facilities next quarter.
--                   Which should we prioritize, and why?"
--
-- FIXES vs original:
--   1. All score components are min-max normalised to [0,1] before weighting
--      so no single unbounded log term can dominate.
--   2. Anomaly threshold removed (was silently excluding small facilities);
--      replaced with a continuous z-score so every facility gets a signal.
--   3. WHY_PRIORITIZED column explains the top driver(s) in plain English.
--   4. Weights sum to 1.0 and are commented with rationale.
-- =============================================================================

CREATE OR REPLACE TABLE top_25_facilities AS
WITH

-- ── reference point ──────────────────────────────────────────────────────────
bounds AS (
    SELECT MAX(month) AS max_month FROM mart_facility_monthly
),

-- ── per-facility feature aggregation ─────────────────────────────────────────
features AS (
    SELECT
        wdid,
        ANY_VALUE(facility_name)    AS facility_name,
        ANY_VALUE(county)           AS county,
        ANY_VALUE(region)           AS region,
        ANY_VALUE(place_type)    AS place_type,

        -- 1. RECENCY: last month with any action
        MAX(month)                  AS last_action_month,

        -- 2. FREQUENCY: enforcement actions in last 12 months
        SUM(CASE
            WHEN month >= (SELECT max_month FROM bounds) - INTERVAL '12 months'
            THEN enforcement_count ELSE 0
        END)                        AS actions_12mo,

        -- 3. SEVERITY: avg severity rank in last 12 months (weighted by count)
        SUM(CASE
            WHEN month >= (SELECT max_month FROM bounds) - INTERVAL '12 months'
            THEN avg_severity_rank * enforcement_count ELSE 0
        END) /
        NULLIF(SUM(CASE
            WHEN month >= (SELECT max_month FROM bounds) - INTERVAL '12 months'
            THEN enforcement_count ELSE 0
        END), 0)                    AS weighted_severity_12mo,

        -- 4. OUTSTANDING MONEY: unpaid penalties (all time)
        SUM(total_outstanding)      AS total_outstanding,

        -- 5. ESCALATION: z-score of last-3-month rate vs prior-12-month baseline
        --    (recent 3mo avg) − (prior 12mo avg) / stddev
        SUM(CASE
            WHEN month >= (SELECT max_month FROM bounds) - INTERVAL '3 months'
            THEN enforcement_count ELSE 0
        END) / 3.0                  AS rate_3mo,

        AVG(CASE
            WHEN month <  (SELECT max_month FROM bounds) - INTERVAL '3 months'
             AND month >= (SELECT max_month FROM bounds) - INTERVAL '15 months'
            THEN enforcement_count ELSE NULL
        END)                        AS baseline_avg,

        STDDEV_SAMP(CASE
            WHEN month <  (SELECT max_month FROM bounds) - INTERVAL '3 months'
             AND month >= (SELECT max_month FROM bounds) - INTERVAL '15 months'
            THEN enforcement_count ELSE NULL
        END)                        AS baseline_std,

        -- 6. ACTIVE ACTIONS: open/unresolved as of latest month
        SUM(CASE
            WHEN month = (SELECT max_month FROM bounds)
            THEN active_count ELSE 0
        END)                        AS current_active_actions

    FROM mart_facility_monthly
    GROUP BY wdid
),

-- ── continuous escalation z-score ────────────────────────────────────────────
with_zscore AS (
    SELECT
        *,
        CASE
            WHEN baseline_std IS NOT NULL AND baseline_std > 0
            THEN (rate_3mo - baseline_avg) / baseline_std
            WHEN baseline_avg IS NOT NULL AND baseline_avg > 0
            THEN (rate_3mo - baseline_avg) / baseline_avg  -- fallback: % deviation
            ELSE 0
        END AS escalation_zscore
    FROM features
),

-- ── min-max normalise every component to [0, 1] ───────────────────────────────
-- FIX: all components share the same scale before weighting.
-- Without this, log(actions) ~ 5 would dwarf recency_raw ~ 0.3.
population_stats AS (
    SELECT
        MIN(DATE_DIFF('day', last_action_month,
            (SELECT max_month FROM bounds)))          AS rec_min,
        MAX(DATE_DIFF('day', last_action_month,
            (SELECT max_month FROM bounds)))          AS rec_max,
        MIN(actions_12mo)                             AS freq_min,
        MAX(actions_12mo)                             AS freq_max,
        MIN(COALESCE(weighted_severity_12mo, 0))      AS sev_min,
        MAX(COALESCE(weighted_severity_12mo, 0))      AS sev_max,
        MIN(total_outstanding)                        AS money_min,
        MAX(total_outstanding)                        AS money_max,
        MIN(escalation_zscore)                        AS esc_min,
        MAX(escalation_zscore)                        AS esc_max,
        MIN(current_active_actions)                   AS act_min,
        MAX(current_active_actions)                   AS act_max
    FROM with_zscore
),

normalised AS (
    SELECT
        z.*,
        -- recency: INVERT so fewer days since last action → higher score
        1.0 - CASE
            WHEN ps.rec_max = ps.rec_min THEN 0.5
            ELSE (DATE_DIFF('day', z.last_action_month,
                    (SELECT max_month FROM bounds)) - ps.rec_min)::DOUBLE
                 / (ps.rec_max - ps.rec_min)
        END                                               AS n_recency,

        CASE
            WHEN ps.freq_max = ps.freq_min THEN 0.0
            ELSE (z.actions_12mo - ps.freq_min)::DOUBLE
                 / (ps.freq_max - ps.freq_min)
        END                                               AS n_frequency,

        CASE
            WHEN ps.sev_max = ps.sev_min THEN 0.0
            ELSE (COALESCE(z.weighted_severity_12mo, 0) - ps.sev_min)
                 / (ps.sev_max - ps.sev_min)
        END                                               AS n_severity,

        CASE
            WHEN ps.money_max = ps.money_min THEN 0.0
            ELSE (z.total_outstanding - ps.money_min)::DOUBLE
                 / (ps.money_max - ps.money_min)
        END                                               AS n_money,

        CASE
            WHEN ps.esc_max = ps.esc_min THEN 0.0
            ELSE (z.escalation_zscore - ps.esc_min)::DOUBLE
                 / (ps.esc_max - ps.esc_min)
        END                                               AS n_escalation,

        CASE
            WHEN ps.act_max = ps.act_min THEN 0.0
            ELSE (z.current_active_actions - ps.act_min)::DOUBLE
                 / (ps.act_max - ps.act_min)
        END                                               AS n_active

    FROM with_zscore z
    CROSS JOIN population_stats ps
),

-- ── weighted composite score ──────────────────────────────────────────────────
-- Weights (sum = 1.0):
--   recency    0.25  recent enforcement predicts near-term risk
--   frequency  0.25  chronic violators need oversight
--   severity   0.20  action escalation type matters most for harm
--   money      0.15  unpaid penalties signal ongoing non-compliance
--   escalation 0.10  spike vs baseline = deteriorating posture
--   active     0.05  open orders = unresolved issues right now
scored AS (
    SELECT
        *,
        ROUND(
            (0.25 * n_recency)
          + (0.25 * n_frequency)
          + (0.20 * n_severity)
          + (0.15 * n_money)
          + (0.10 * n_escalation)
          + (0.05 * n_active)
          , 4
        ) AS priority_score
    FROM normalised
),

-- ── plain-English explanation of top drivers ─────────────────────────────────
explained AS (
    SELECT
        *,
        CASE
            WHEN n_escalation >= 0.7 AND n_severity  >= 0.7
                THEN 'Spiking enforcement rate AND high-severity actions recently'
            WHEN n_escalation >= 0.7
                THEN 'Recent spike vs 12-month baseline (escalating trend)'
            WHEN n_severity   >= 0.7 AND n_frequency >= 0.7
                THEN 'High-severity, high-frequency enforcement history'
            WHEN n_money      >= 0.7
                THEN 'Large outstanding unpaid penalties'
            WHEN n_severity   >= 0.7
                THEN 'High average severity of recent actions'
            WHEN n_frequency  >= 0.7
                THEN 'High volume of enforcement actions in last 12 months'
            WHEN n_recency    >= 0.8
                THEN 'Very recent enforcement activity'
            WHEN n_active     >= 0.7
                THEN 'Multiple open / unresolved enforcement orders'
            ELSE 'Moderate risk across multiple dimensions'
        END AS why_prioritized
    FROM scored
)

-- ── final top-25 ─────────────────────────────────────────────────────────────
SELECT
    ROW_NUMBER() OVER (ORDER BY priority_score DESC)   AS priority_rank,
    ROUND(priority_score * 100, 1)                     AS priority_score_pct,
    why_prioritized,
    wdid,
    facility_name,
    county,
    region,
    place_type,
    actions_12mo,
    ROUND(COALESCE(weighted_severity_12mo, 0), 2)      AS avg_severity_12mo,
    ROUND(total_outstanding, 0)                        AS outstanding_penalties,
    ROUND(escalation_zscore, 2)                        AS escalation_zscore,
    current_active_actions,
    last_action_month,
    -- normalised component scores for transparency
    ROUND(n_recency    * 100, 1)  AS score_recency,
    ROUND(n_frequency  * 100, 1)  AS score_frequency,
    ROUND(n_severity   * 100, 1)  AS score_severity,
    ROUND(n_money      * 100, 1)  AS score_money,
    ROUND(n_escalation * 100, 1)  AS score_escalation,
    ROUND(n_active     * 100, 1)  AS score_active
FROM explained
ORDER BY priority_score DESC
LIMIT 25;
