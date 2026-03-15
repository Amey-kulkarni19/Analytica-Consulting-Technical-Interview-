-- 02_mart.sql
-- Creates: mart_facility_monthly (facility x month)

CREATE OR REPLACE TABLE mart_facility_monthly AS
SELECT
  f.wdid,
  df.facility_name,
  df.place_county,
  df.facility_region,
  DATE_TRUNC('month', f.effective_date)::DATE AS month,
  COUNT(*) AS enforcement_count,
  SUM(COALESCE(f.total_assessment_amount, 0)) AS total_assessment_sum,
  SUM(COALESCE(f.liability_amount, 0)) AS liability_sum,
  SUM(COALESCE(f.project_amount, 0)) AS project_sum
FROM fact_enforcement_actions f
LEFT JOIN dim_facility df
  ON f.wdid = df.wdid
WHERE f.effective_date IS NOT NULL
GROUP BY 1,2,3,4,5;