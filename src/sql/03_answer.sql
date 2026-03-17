-- 03_answer.sql
-- Client question: "Which 25 facilities should we prioritize for review next quarter?"
--
-- Ranked by three observable dimensions in order:
--   1. outstanding_balance  — total assessed minus total paid, floored at zero
--   2. total_actions        — chronic vs isolated offender
--   3. most_recent_action   — recency as tiebreaker
--
-- No composite score. Each dimension is a separate column so the reasoning
-- is transparent and auditable.

CREATE OR REPLACE TABLE priority_facilities AS

WITH facility_summary AS (
    SELECT
        f.wdid,
        f.facility_name,
        f.region,
        f.county,
        f.place_type,

        COUNT(*)                                            AS total_actions,
        MAX(fe.effective_date)                              AS most_recent_action,
        MIN(fe.effective_date)                              AS first_action,

        SUM(COALESCE(fe.total_assessment_amount, 0))        AS total_assessed,
        SUM(COALESCE(fe.total_paid_completed, 0))           AS total_paid,
        SUM(COALESCE(fe.outstanding_amount, 0))             AS outstanding_balance,

        -- Recent activity: last 3 years of the dataset
        SUM(CASE
            WHEN fe.effective_date >= (
                SELECT MAX(effective_date) - INTERVAL '3 years'
                FROM fact_enforcement
            ) THEN 1 ELSE 0 END
        )                                                   AS actions_last_3yr

    FROM fact_enforcement fe
    JOIN dim_facility f USING (wdid)
    GROUP BY f.wdid, f.facility_name, f.region, f.county, f.place_type
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY outstanding_balance DESC, total_actions DESC, most_recent_action DESC
    )                       AS rank,
    wdid,
    facility_name,
    region,
    county,
    place_type,
    total_actions,
    actions_last_3yr,
    most_recent_action,
    first_action,
    total_assessed,
    total_paid,
    outstanding_balance

FROM facility_summary
WHERE total_actions > 0
ORDER BY outstanding_balance DESC, total_actions DESC, most_recent_action DESC
LIMIT 25;
