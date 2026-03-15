-- =============================================================================
-- 04_text_analysis.sql
-- NLP theme extraction from DESCRIPTION free text in fact_enforcement.
--
-- Approach: keyword/regex classification into 8 violation themes,
-- then correlate each theme with severity rank and penalty amounts.
-- Output: mart_description_themes  (one row per theme)
--         mart_facility_themes     (one row per facility × theme)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Step 1: tag each enforcement action with matching themes
-- A single action can match multiple themes (UNNEST handles multi-label)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_enforcement_themes AS
SELECT
    enforcement_eid,
    wdid,
    effective_date,
    severity_rank,
    is_monetary,
    total_assessment_amount,
    outstanding_amount,
    description,
    UNNEST(ARRAY[
        CASE WHEN REGEXP_MATCHES(LOWER(description),
            'report|smr|submit|annual report|monitoring report|late|overdue|missing report|non.submitt')
             THEN 'Reporting / SMR failure' END,
        CASE WHEN REGEXP_MATCHES(LOWER(description),
            'effluent|exceed|bod|tss|ammonia|nitrate|ph |coliform|chlorine|tds|ec |salinity|turbid')
             THEN 'Effluent exceedance' END,
        CASE WHEN REGEXP_MATCHES(LOWER(description),
            'permit|wdr|npdes|order|condition|provision|requirement|specification')
             THEN 'Permit conditions' END,
        CASE WHEN REGEXP_MATCHES(LOWER(description),
            'spill|overflow|sso|discharge|gallon|untreated|unauthorized discharge|release')
             THEN 'Spill / overflow' END,
        CASE WHEN REGEXP_MATCHES(LOWER(description),
            'operation|maintenance|o&m|log|equipment|pond|aeration|do level|repair|broken')
             THEN 'O&M / operations' END,
        CASE WHEN REGEXP_MATCHES(LOWER(description),
            'groundwater|soil|contamination|cleanup|plume|tce|pce|mtbe|benzene|remediat')
             THEN 'Groundwater / soil contamination' END,
        CASE WHEN REGEXP_MATCHES(LOWER(description),
            'monitor|sampling|frequency|sample|lab|analytical|meter|calibrat')
             THEN 'Monitoring deficiency' END,
        CASE WHEN REGEXP_MATCHES(LOWER(description),
            'penalty|fine|acl|civil liability|assessed|minimum penalty|mmp|complaint|liability')
             THEN 'Financial / ACL' END
    ]) AS theme
FROM fact_enforcement
WHERE description IS NOT NULL
  AND TRIM(description) <> ''
  AND TRIM(description) <> 'UNKNOWN';

-- Remove nulls (actions that matched no theme)
DELETE FROM stg_enforcement_themes WHERE theme IS NULL;


-- ---------------------------------------------------------------------------
-- Step 2: mart_description_themes — aggregate by theme
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart_description_themes AS
SELECT
    theme,
    COUNT(*)                                    AS citation_count,
    ROUND(AVG(severity_rank), 2)                AS avg_severity_rank,
    ROUND(AVG(is_monetary) * 100, 1)            AS pct_monetary,
    ROUND(AVG(total_assessment_amount), 0)      AS avg_assessment,
    SUM(total_assessment_amount)                AS total_assessment,
    SUM(outstanding_amount)                     AS total_outstanding,
    -- pct of actions with this theme that have a severity >= 5 (serious)
    ROUND(AVG(CASE WHEN severity_rank >= 5 THEN 1.0 ELSE 0.0 END) * 100, 1)
                                                AS pct_serious_actions
FROM stg_enforcement_themes
GROUP BY theme
ORDER BY citation_count DESC;


-- ---------------------------------------------------------------------------
-- Step 3: mart_facility_themes — facility-level theme profile
-- Useful for understanding what *kind* of violations each facility tends to get
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart_facility_themes AS
SELECT
    t.wdid,
    df.facility_name,
    df.region,
    df.county,
    t.theme,
    COUNT(*)                                    AS theme_count,
    ROUND(AVG(t.severity_rank), 2)              AS avg_severity_rank,
    SUM(t.total_assessment_amount)              AS total_assessment,
    -- rank this theme within the facility (1 = most common theme for that facility)
    ROW_NUMBER() OVER (
        PARTITION BY t.wdid
        ORDER BY COUNT(*) DESC
    )                                           AS theme_rank_within_facility
FROM stg_enforcement_themes t
LEFT JOIN dim_facility df ON t.wdid = df.wdid
GROUP BY t.wdid, df.facility_name, df.region, df.county, t.theme;


-- ---------------------------------------------------------------------------
-- Step 4: top_theme_per_facility — one row per facility, dominant theme
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE top_theme_per_facility AS
SELECT *
FROM mart_facility_themes
WHERE theme_rank_within_facility = 1;


-- Quick sanity check
SELECT theme, citation_count, avg_severity_rank, pct_monetary, pct_serious_actions
FROM mart_description_themes
ORDER BY citation_count DESC;
