-- =============================================================================
-- 01_model.sql  —  Raw → Staging → Dimensions → Fact
--
-- Column names verified from raw_schema.txt:
--
-- raw_permits_orders (lowercase snake_case):
--   wdid, reg_measure_id, reg_measure_type, program_category, facility_id,
--   facility_region, facility_name, place_type, place_address, place_city,
--   place_zip, place_county, latitude_decimal_degrees, longitude_decimal_degrees,
--   effective_date, termination_date, adoption_date, status
--
-- raw_enforcement_actions (UPPER CASE with spaces, must be quoted):
--   "REGION", "FACILITY ID", "FACILITY NAME", "AGENCY NAME", "PLACE TYPE",
--   "PLACE SUBTYPE", "PLACE LATITUDE", "PLACE LONGITUDE", "WDID",
--   "REG MEASURE ID", "REG MEASURE TYPE", "ENFORCEMENT ID (EID)",
--   "ORDER / RESOLUTION NUMBER", "ENFORCEMENT ACTION TYPE", "EFFECTIVE DATE",
--   "ADOPTION / ISSUANCE DATE", "TERMINATION DATE", "STATUS", "TITLE",
--   "DESCRIPTION", "PROGRAM", "PROGRAM CATEGORY", "TOTAL ASSESSMENT AMOUNT",
--   "INITIAL ASSESSED AMOUNT", "LIABILITY $ AMOUNT", "PROJECT $ AMOUNT",
--   "LIABILITY $ PAID", "PROJECT $ COMPLETED", "TOTAL $ PAID/COMPLETED AMOUNT"
-- =============================================================================


-- ---------------------------------------------------------------------------
-- STAGING: permits & orders
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_permits AS
SELECT
    UPPER(TRIM(wdid))                                           AS wdid,
    TRIM(reg_measure_id)                                        AS reg_measure_id,
    TRIM(reg_measure_type)                                      AS reg_measure_type,
    TRIM(program_category)                                      AS program_category,
    TRIM(facility_id)                                           AS facility_id,
    TRIM(facility_region)                                       AS facility_region,
    TRIM(facility_name)                                         AS facility_name,
    TRIM(place_type)                                            AS place_type,
    TRIM(place_address)                                         AS place_address,
    TRIM(place_city)                                            AS place_city,
    TRIM(place_zip)                                             AS place_zip,
    TRIM(place_county)                                          AS place_county,
    TRY_CAST(latitude_decimal_degrees  AS DOUBLE)               AS latitude,
    TRY_CAST(longitude_decimal_degrees AS DOUBLE)               AS longitude,
    TRIM(status)                                                AS permit_status,
    COALESCE(
        TRY_STRPTIME(TRIM(effective_date),   '%m/%d/%Y')::DATE,
        TRY_STRPTIME(TRIM(effective_date),   '%Y-%m-%d')::DATE
    )                                                           AS effective_date,
    COALESCE(
        TRY_STRPTIME(TRIM(termination_date), '%m/%d/%Y')::DATE,
        TRY_STRPTIME(TRIM(termination_date), '%Y-%m-%d')::DATE
    )                                                           AS termination_date,
    COALESCE(
        TRY_STRPTIME(TRIM(adoption_date),    '%m/%d/%Y')::DATE,
        TRY_STRPTIME(TRIM(adoption_date),    '%Y-%m-%d')::DATE
    )                                                           AS adoption_date
FROM raw_permits_orders
WHERE wdid IS NOT NULL
  AND TRIM(wdid) <> '';


-- ---------------------------------------------------------------------------
-- STAGING: enforcement actions
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_enforcement AS
SELECT
    UPPER(TRIM("WDID"))                                         AS wdid,
    TRIM("ENFORCEMENT ID (EID)")                                AS enforcement_eid,
    TRIM("REGION")                                              AS region,
    TRIM("FACILITY ID")                                         AS facility_id,
    TRIM("FACILITY NAME")                                       AS facility_name,
    TRIM("AGENCY NAME")                                         AS agency_name,
    TRIM("PLACE TYPE")                                          AS place_type,
    TRIM("PLACE SUBTYPE")                                       AS place_subtype,
    TRY_CAST("PLACE LATITUDE"  AS DOUBLE)                       AS latitude,
    TRY_CAST("PLACE LONGITUDE" AS DOUBLE)                       AS longitude,
    TRIM("REG MEASURE ID")                                      AS reg_measure_id,
    TRIM("REG MEASURE TYPE")                                    AS reg_measure_type,
    TRIM("ORDER / RESOLUTION NUMBER")                           AS order_resolution_number,
    TRIM("ENFORCEMENT ACTION TYPE")                             AS enforcement_action_type,
    TRIM("STATUS")                                              AS status,
    TRIM("PROGRAM")                                             AS program,
    TRIM("PROGRAM CATEGORY")                                    AS program_category,
    TRIM("TITLE")                                               AS title,
    TRIM("DESCRIPTION")                                         AS description,
    -- Try 4-digit year first, then 2-digit year (12/29/16 -> 2016-12-29).
    -- Dates before 1900 are misparsed 2-digit years (year 16 AD etc.) - set to NULL.
    CASE WHEN COALESCE(
        TRY_STRPTIME(TRIM("EFFECTIVE DATE"), '%m/%d/%Y')::DATE,
        TRY_STRPTIME(TRIM("EFFECTIVE DATE"), '%m/%d/%y')::DATE,
        TRY_STRPTIME(TRIM("EFFECTIVE DATE"), '%Y-%m-%d')::DATE
    ) >= '1900-01-01'
    THEN COALESCE(
        TRY_STRPTIME(TRIM("EFFECTIVE DATE"), '%m/%d/%Y')::DATE,
        TRY_STRPTIME(TRIM("EFFECTIVE DATE"), '%m/%d/%y')::DATE,
        TRY_STRPTIME(TRIM("EFFECTIVE DATE"), '%Y-%m-%d')::DATE
    ) END                                                       AS effective_date,
    CASE WHEN COALESCE(
        TRY_STRPTIME(TRIM("ADOPTION / ISSUANCE DATE"), '%m/%d/%Y')::DATE,
        TRY_STRPTIME(TRIM("ADOPTION / ISSUANCE DATE"), '%m/%d/%y')::DATE,
        TRY_STRPTIME(TRIM("ADOPTION / ISSUANCE DATE"), '%Y-%m-%d')::DATE
    ) >= '1900-01-01'
    THEN COALESCE(
        TRY_STRPTIME(TRIM("ADOPTION / ISSUANCE DATE"), '%m/%d/%Y')::DATE,
        TRY_STRPTIME(TRIM("ADOPTION / ISSUANCE DATE"), '%m/%d/%y')::DATE,
        TRY_STRPTIME(TRIM("ADOPTION / ISSUANCE DATE"), '%Y-%m-%d')::DATE
    ) END                                                       AS adoption_date,
    CASE WHEN COALESCE(
        TRY_STRPTIME(TRIM("TERMINATION DATE"), '%m/%d/%Y')::DATE,
        TRY_STRPTIME(TRIM("TERMINATION DATE"), '%m/%d/%y')::DATE,
        TRY_STRPTIME(TRIM("TERMINATION DATE"), '%Y-%m-%d')::DATE
    ) >= '1900-01-01'
    THEN COALESCE(
        TRY_STRPTIME(TRIM("TERMINATION DATE"), '%m/%d/%Y')::DATE,
        TRY_STRPTIME(TRIM("TERMINATION DATE"), '%m/%d/%y')::DATE,
        TRY_STRPTIME(TRIM("TERMINATION DATE"), '%Y-%m-%d')::DATE
    ) END                                                       AS termination_date,
    TRY_CAST(REPLACE(REPLACE(TRIM("TOTAL ASSESSMENT AMOUNT"),      '$',''),',','') AS DOUBLE) AS total_assessment_amount,
    TRY_CAST(REPLACE(REPLACE(TRIM("INITIAL ASSESSED AMOUNT"),      '$',''),',','') AS DOUBLE) AS initial_assessed_amount,
    TRY_CAST(REPLACE(REPLACE(TRIM("LIABILITY $ AMOUNT"),           '$',''),',','') AS DOUBLE) AS liability_amount,
    TRY_CAST(REPLACE(REPLACE(TRIM("PROJECT $ AMOUNT"),             '$',''),',','') AS DOUBLE) AS project_amount,
    TRY_CAST(REPLACE(REPLACE(TRIM("LIABILITY $ PAID"),             '$',''),',','') AS DOUBLE) AS liability_paid,
    TRY_CAST(REPLACE(REPLACE(TRIM("PROJECT $ COMPLETED"),          '$',''),',','') AS DOUBLE) AS project_completed,
    TRY_CAST(REPLACE(REPLACE(TRIM("TOTAL $ PAID/COMPLETED AMOUNT"),'$',''),',','') AS DOUBLE) AS total_paid_completed
FROM raw_enforcement_actions
WHERE "WDID" IS NOT NULL
  AND TRIM("WDID") <> '';


-- ---------------------------------------------------------------------------
-- DIM: facility
-- Built from both sources via UNION so facilities that appear only in
-- enforcement (no permit record) still get a row.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_facility AS
WITH from_permits AS (
    SELECT
        wdid,
        ANY_VALUE(facility_name)    AS facility_name,
        NULL::VARCHAR               AS agency_name,
        ANY_VALUE(facility_region)  AS region,
        ANY_VALUE(place_county)     AS county,
        ANY_VALUE(place_city)       AS city,
        ANY_VALUE(place_zip)        AS zip_code,
        ANY_VALUE(place_address)    AS place_address,
        ANY_VALUE(place_type)       AS place_type,
        ANY_VALUE(latitude)         AS latitude,
        ANY_VALUE(longitude)        AS longitude
    FROM stg_permits
    GROUP BY wdid
),
from_enforcement AS (
    SELECT
        wdid,
        ANY_VALUE(facility_name)    AS facility_name,
        ANY_VALUE(agency_name)      AS agency_name,
        ANY_VALUE(region)           AS region,
        NULL::VARCHAR               AS county,
        NULL::VARCHAR               AS city,
        NULL::VARCHAR               AS zip_code,
        NULL::VARCHAR               AS place_address,
        ANY_VALUE(place_type)       AS place_type,
        ANY_VALUE(latitude)         AS latitude,
        ANY_VALUE(longitude)        AS longitude
    FROM stg_enforcement
    GROUP BY wdid
),
all_wdids AS (
    SELECT wdid FROM from_permits
    UNION
    SELECT wdid FROM from_enforcement
)
SELECT
    u.wdid,
    COALESCE(p.facility_name, e.facility_name)  AS facility_name,
    COALESCE(e.agency_name,   p.agency_name)    AS agency_name,
    COALESCE(p.region,        e.region)         AS region,
    p.county,
    p.city,
    p.zip_code,
    p.place_address,
    COALESCE(p.place_type,    e.place_type)     AS place_type,
    COALESCE(p.latitude,      e.latitude)       AS latitude,
    COALESCE(p.longitude,     e.longitude)      AS longitude
FROM all_wdids u
LEFT JOIN from_permits     p ON u.wdid = p.wdid
LEFT JOIN from_enforcement e ON u.wdid = e.wdid;


-- ---------------------------------------------------------------------------
-- FACT: one row per enforcement action
-- Removed: severity_rank, is_monetary (not sourced from the dataset)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE fact_enforcement AS
SELECT
    e.enforcement_eid,
    e.wdid,
    e.reg_measure_id,
    e.enforcement_action_type,
    e.effective_date,
    e.adoption_date,
    e.termination_date,
    e.status,
    e.program,
    e.program_category,
    e.region,
    COALESCE(e.total_assessment_amount, 0)  AS total_assessment_amount,
    COALESCE(e.liability_amount,        0)  AS liability_amount,
    COALESCE(e.liability_paid,          0)  AS liability_paid,
    COALESCE(e.project_amount,          0)  AS project_amount,
    COALESCE(e.project_completed,       0)  AS project_completed,
    COALESCE(e.total_paid_completed,    0)  AS total_paid_completed,
    GREATEST(
        COALESCE(e.total_assessment_amount, 0)
        - COALESCE(e.total_paid_completed,  0),
        0
    )                                       AS outstanding_amount,
    e.title,
    e.description
FROM stg_enforcement e;
