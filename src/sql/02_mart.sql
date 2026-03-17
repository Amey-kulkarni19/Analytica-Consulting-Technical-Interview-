-- =============================================================================
-- 02_mart.sql  —  fact_enforcement → mart_facility_monthly
--
-- One row per (facility × calendar month).
-- Removed: avg_severity_rank, max_severity_rank, monetary_count
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

    DATE_TRUNC('month', f.effective_date)::DATE     AS month,
    EXTRACT(YEAR FROM f.effective_date)::INT         AS year,

    COUNT(*)                                         AS enforcement_count,

    SUM(f.total_assessment_amount)                   AS total_assessment,
    SUM(f.liability_amount)                          AS total_liability,
    SUM(f.liability_paid)                            AS total_paid,
    SUM(f.outstanding_amount)                        AS total_outstanding,

    SUM(CASE WHEN f.status = 'Active' THEN 1 ELSE 0 END) AS active_count

FROM fact_enforcement f
LEFT JOIN dim_facility df ON f.wdid = df.wdid
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
