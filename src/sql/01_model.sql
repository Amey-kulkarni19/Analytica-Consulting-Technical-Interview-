-- 01_model.sql
-- Creates: stg_permits_orders, stg_enforcement_actions, dim_facility, dim_time, dim_action_type, fact_enforcement_actions

CREATE OR REPLACE TABLE stg_permits_orders AS
SELECT
  UPPER(TRIM(wdid))                                    AS wdid,
  TRIM(reg_measure_id)                                 AS reg_measure_id,
  TRIM(reg_measure_type)                               AS reg_measure_type,
  TRIM(program_category)                               AS program_category,
  TRIM(facility_id)                                    AS facility_id,
  TRIM(facility_region)                                AS facility_region,
  TRIM(facility_name)                                  AS facility_name,
  TRIM(place_type)                                     AS place_type,
  TRIM(place_address)                                  AS place_address,
  TRIM(place_city)                                     AS place_city,
  TRIM(place_zip)                                      AS place_zip,
  TRIM(place_county)                                   AS place_county,
  TRY_CAST(latitude_decimal_degrees  AS DOUBLE)        AS latitude,
  TRY_CAST(longitude_decimal_degrees AS DOUBLE)        AS longitude,
  -- dates (multiple possible formats in public datasets)
  COALESCE(
    TRY_STRPTIME(effective_date, '%m/%d/%Y')::DATE,
    TRY_STRPTIME(effective_date, '%Y-%m-%d')::DATE
  ) AS effective_date,
  COALESCE(
    TRY_STRPTIME(termination_date, '%m/%d/%Y')::DATE,
    TRY_STRPTIME(termination_date, '%Y-%m-%d')::DATE
  ) AS termination_date,
  COALESCE(
    TRY_STRPTIME(adoption_date, '%m/%d/%Y')::DATE,
    TRY_STRPTIME(adoption_date, '%Y-%m-%d')::DATE
  ) AS adoption_date
FROM raw_permits_orders
WHERE wdid IS NOT NULL AND TRIM(wdid) <> '';

CREATE OR REPLACE TABLE stg_enforcement_actions AS
SELECT
  TRIM("REGION")                                       AS region,
  TRIM("FACILITY ID")                                  AS facility_id,
  TRIM("FACILITY NAME")                                AS facility_name,
  TRIM("AGENCY NAME")                                  AS agency_name,
  TRIM("PLACE TYPE")                                   AS place_type,
  TRIM("PLACE SUBTYPE")                                AS place_subtype,
  TRY_CAST("PLACE LATITUDE"  AS DOUBLE)                AS latitude,
  TRY_CAST("PLACE LONGITUDE" AS DOUBLE)                AS longitude,
  UPPER(TRIM("WDID"))                                  AS wdid,
  TRIM("REG MEASURE ID")                               AS reg_measure_id,
  TRIM("REG MEASURE TYPE")                             AS reg_measure_type,
  TRIM("ENFORCEMENT ID (EID)")                         AS enforcement_eid,
  TRIM("ORDER / RESOLUTION NUMBER")                    AS order_resolution_number,
  TRIM("ENFORCEMENT ACTION TYPE")                      AS enforcement_action_type,
  COALESCE(
    TRY_STRPTIME("EFFECTIVE DATE", '%m/%d/%Y')::DATE,
    TRY_STRPTIME("EFFECTIVE DATE", '%Y-%m-%d')::DATE
  ) AS effective_date,
  COALESCE(
    TRY_STRPTIME("ADOPTION / ISSUANCE DATE", '%m/%d/%Y')::DATE,
    TRY_STRPTIME("ADOPTION / ISSUANCE DATE", '%Y-%m-%d')::DATE
  ) AS adoption_date,
  COALESCE(
    TRY_STRPTIME("TERMINATION DATE", '%m/%d/%Y')::DATE,
    TRY_STRPTIME("TERMINATION DATE", '%Y-%m-%d')::DATE
  ) AS termination_date,
  TRIM("STATUS")                                       AS status,
  TRIM("TITLE")                                        AS title,
  TRIM("DESCRIPTION")                                  AS description,
  TRIM("PROGRAM")                                      AS program,
  TRIM("PROGRAM CATEGORY")                             AS program_category,

  -- money fields (stored as VARCHAR; strip commas/$)
  TRY_CAST(REPLACE(REPLACE(TRIM("TOTAL ASSESSMENT AMOUNT"), '$', ''), ',', '') AS DOUBLE) AS total_assessment_amount,
  TRY_CAST(REPLACE(REPLACE(TRIM("INITIAL ASSESSED AMOUNT"), '$', ''), ',', '') AS DOUBLE) AS initial_assessed_amount,
  TRY_CAST(REPLACE(REPLACE(TRIM("LIABILITY $ AMOUNT"), '$', ''), ',', '') AS DOUBLE)       AS liability_amount,
  TRY_CAST(REPLACE(REPLACE(TRIM("PROJECT $ AMOUNT"), '$', ''), ',', '') AS DOUBLE)         AS project_amount,
  TRY_CAST(REPLACE(REPLACE(TRIM("LIABILITY $ PAID"), '$', ''), ',', '') AS DOUBLE)         AS liability_paid,
  TRY_CAST(REPLACE(REPLACE(TRIM("PROJECT $ COMPLETED"), '$', ''), ',', '') AS DOUBLE)      AS project_completed,
  TRY_CAST(REPLACE(REPLACE(TRIM("TOTAL $ PAID/COMPLETED AMOUNT"), '$', ''), ',', '') AS DOUBLE) AS total_paid_completed_amount
FROM raw_enforcement_actions
WHERE "WDID" IS NOT NULL AND TRIM("WDID") <> '';

-- Dimensions
CREATE OR REPLACE TABLE dim_facility AS
SELECT
  wdid,
  -- choose stable attributes from permits (preferred)
  ANY_VALUE(facility_id)      AS facility_id,
  ANY_VALUE(facility_name)    AS facility_name,
  ANY_VALUE(facility_region)  AS facility_region,
  ANY_VALUE(place_county)     AS place_county,
  ANY_VALUE(place_city)       AS place_city,
  ANY_VALUE(place_zip)        AS place_zip,
  ANY_VALUE(latitude)         AS latitude,
  ANY_VALUE(longitude)        AS longitude
FROM stg_permits_orders
GROUP BY 1;

CREATE OR REPLACE TABLE dim_action_type AS
SELECT DISTINCT
  enforcement_action_type
FROM stg_enforcement_actions
WHERE enforcement_action_type IS NOT NULL AND enforcement_action_type <> '';

-- Date dimension built from enforcement effective dates
CREATE OR REPLACE TABLE dim_time AS
WITH bounds AS (
  SELECT
    MIN(effective_date) AS min_d,
    MAX(effective_date) AS max_d
  FROM stg_enforcement_actions
  WHERE effective_date IS NOT NULL
),
dates AS (
  SELECT * FROM generate_series(
    (SELECT min_d FROM bounds),
    (SELECT max_d FROM bounds),
    INTERVAL 1 DAY
  ) AS t(d)
)
SELECT
  d::DATE AS date_day,
  EXTRACT(YEAR FROM d)::INT AS year,
  EXTRACT(MONTH FROM d)::INT AS month,
  EXTRACT(QUARTER FROM d)::INT AS quarter,
  DATE_TRUNC('month', d)::DATE AS month_start
FROM dates;

-- Fact (one row per enforcement record)
CREATE OR REPLACE TABLE fact_enforcement_actions AS
SELECT
  enforcement_eid,
  wdid,
  reg_measure_id,
  enforcement_action_type,
  effective_date,
  adoption_date,
  termination_date,
  status,
  program,
  program_category,
  total_assessment_amount,
  initial_assessed_amount,
  liability_amount,
  project_amount,
  liability_paid,
  project_completed,
  total_paid_completed_amount,
  title,
  description
FROM stg_enforcement_actions;