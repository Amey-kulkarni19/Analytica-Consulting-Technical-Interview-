-- =============================================================================
-- 02_mart.sql
-- Creates: mart_facility_monthly
--
-- One row per (facility × calendar month).
-- Joins fact → dim_facility so every row has full facility metadata.
-- Adds score component columns (not yet normalised — normalisation happens
-- in 03_answer.sql using window functions over the whole population).
-- =============================================================================

CREATE OR REPLACE TABLE mart_facility_monthly AS
SELECT
    f.wdid,
    df.facility_name,
    df.agency_name,
    df.region,
    df.county,
    df.city,
    df.place_type,
    df.latitude,
    df.longitude,

    DATE_TRUNC('month', f.effective_date)::DATE   AS month,
    EXTRACT(YEAR FROM f.effective_date)::INT       AS year,

    -- volume / frequency
    COUNT(*)                                       AS enforcement_count,
    SUM(f.is_monetary)                             AS monetary_count,

    -- severity: avg rank this month (1=informal … 7=ACL)
    AVG(f.severity_rank)                           AS avg_severity_rank,
    MAX(f.severity_rank)                           AS max_severity_rank,

    -- money
    SUM(f.total_assessment_amount)                 AS total_assessment,
    SUM(f.liability_amount)                        AS total_liability,
    SUM(f.liability_paid)                          AS total_paid,
    SUM(f.outstanding_amount)                      AS total_outstanding,

    -- active (not yet terminated) actions this month
    SUM(CASE WHEN f.status = 'Active' THEN 1 ELSE 0 END) AS active_count

FROM fact_enforcement f
LEFT JOIN dim_facility df
    ON f.wdid = df.wdid
WHERE f.effective_date IS NOT NULL
GROUP BY
    f.wdid,
    df.facility_name,
    df.agency_name,
    df.region,
    df.county,
    df.city,
    df.place_type,
    df.latitude,
    df.longitude,
    DATE_TRUNC('month', f.effective_date)::DATE,
    EXTRACT(YEAR FROM f.effective_date)::INT;
