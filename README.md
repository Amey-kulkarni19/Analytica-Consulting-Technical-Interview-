# CA Wastewater Enforcement — Prioritization Pipeline

> **Dataset:** [Surface Water – Water Quality Regulated Facility Information](https://lab.data.ca.gov/dataset/surface-water-water-quality-regulated-facility-information)
> **Source:** California State Water Resources Control Board (CIWQS)
> **Client question:** *"We can only review 25 facilities next quarter. Which should we prioritize, and why?"*

---

## Quickstart

```bash
# 1. Install dependencies (Python 3.9+)
pip install -r requirements.txt

# 2. One command: raw download → model → mart → ML → dashboard
python pipeline.py run && python -m streamlit run app.py
```

The pipeline runs end to end, then the dashboard opens automatically in your browser at `http://localhost:8501`.

> **Windows note:** Use `python -m streamlit` instead of `streamlit` if the command is not found.
> **First run only:** `python pipeline.py run` downloads ~30MB of raw CSVs from CA Open Data.
> On subsequent runs it skips the download if the files already exist.

**Output files after `pipeline.py run`:**
```
data/warehouse.duckdb                    ← queryable DuckDB database (all tables)
data/outputs/top_25_facilities.csv       ← ranked priority list
data/outputs/raw_schema.txt             ← verified column names from both sources
data/marts/mart_facility_monthly.csv    ← enriched facility × month mart
data/marts/mart_facility_monthly.parquet
data/marts/mart_description_themes.csv  ← NLP theme analysis
data/marts/ml_facility_scores.csv       ← ML risk scores (all facilities)
data/marts/top25_with_ml_scores.csv     ← top-25 with both heuristic + ML scores
```

**Re-run individual steps:**
```bash
python pipeline.py extract    # download raw CSVs
python pipeline.py load       # ingest into DuckDB
python pipeline.py profile    # discover join keys
python pipeline.py model      # 01_model.sql  — staging + dims + fact
python pipeline.py mart       # 02_mart.sql   — facility × month mart
python pipeline.py answer     # 03_answer.sql — top-25 priority list
python pipeline.py text       # 04_text_analysis.sql — NLP themes
```

**View results interactively:**
Open `dashboard.html` in any browser no server needed.
The dashboard contains all charts, the NLP theme analysis, and the filterable top-25 table.

---

## Architecture

```
CA Open Data (two CSVs)
  wastewater-enforcement-actions.csv       44,475 rows
  reg_meas_export_wastewaterpermitsorders  105,233 rows
          │
          ▼  pipeline.py load
  warehouse.duckdb
    raw_enforcement_actions    ← verbatim, ALL_VARCHAR
    raw_permits_orders         ← verbatim, ALL_VARCHAR
          │
          ▼  01_model.sql
    stg_enforcement            ← typed, trimmed, dates parsed, money cast
    stg_permits                ← typed, trimmed
    dim_facility               ← 1 row per WDID (coalesced from both sources)
    dim_action_type            ← severity rank 1–7, monetary flag
    dim_time                   ← date span 1987–2019, CA fiscal year
    fact_enforcement           ← 1 row per action, severity/monetary denormalised in
          │
          ▼  02_mart.sql
    mart_facility_monthly      ← facility × month, 33,260 rows
          │
          ├──▶  03_answer.sql → top_25_facilities (exported CSV)
          │
          └──▶  04_text_analysis.sql → mart_description_themes
                                        mart_facility_themes
                                        top_theme_per_facility
```

**What runs when:** `pipeline.py run` executes all steps in order.
Each step is idempotent `CREATE OR REPLACE TABLE` ensures re-runs don't duplicate.
`warehouse.duckdb` persists between runs and can be queried directly:
```bash
duckdb data/warehouse.duckdb "SELECT * FROM top_25_facilities LIMIT 5"
```

---

## Relational Model

### Discovered join keys

| Key | Tables | How discovered |
|-----|--------|----------------|
| `WDID` | enforcement + permits + all dims | Fetched CSV headers from `data.ca.gov`; confirmed present in both raw files. Normalized to `UPPER(TRIM(...))` to handle whitespace/case inconsistencies.|
| `ENFORCEMENT ID (EID)` | fact only | Natural PK confirmed unique per row in enforcement CSV |
| `REG MEASURE ID` | enforcement + permits | Links an enforcement action to its underlying permit; used for lineage |

### Key design decisions

**Two staging tables, not one.** The primary reason is that the two source files represent fundamentally different entities with different grains. **stg_enforcement** is an event log one row per enforcement action issued against a facility. **stg_permits** is a facility registry one row per regulatory measure a facility is enrolled in. They have different columns, different date semantics, and different business meanings. Merging them into a single staging table would create a wide, sparse table where half the columns are NULL depending on source, making the model harder to reason about and maintain.

**`dim_facility` coalesces from both sources.** Facilities appear in enforcement without a corresponding permit record (unregulated sites, expired permits). Building `dim_facility` from permits only the obvious first instinct drops ~6% of WDIDs from the fact table. The UNION + COALESCE pattern ensures full coverage.

**Severity rank assigned at model time.** 
Every enforcement action has a type — "Notice of Violation", "Admin Civil Liability", etc. The severity rank for each type lives in dim_action_type. When we build fact_enforcement, we join dim_action_type right then and copy severity_rank and is_monetary directly onto every row of the fact table.
---

## Client Answer: Top 25 Facilities

### Priority score (0–100)

All six components are min-max normalised to [0, 1] before weighting:

| Factor | Weight | Rationale |
|--------|--------|-----------|
| Recency | 25% | Recent enforcement predicts near-term risk |
| Frequency (12mo) | 25% | Chronic violators signal systemic issues |
| Severity (12mo weighted avg) | 20% | Action type escalation maps to harm potential |
| Outstanding penalties | 15% | Unpaid amounts signal unresolved violations |
| Escalation z-score | 10% | Spike vs 12-month baseline = deteriorating posture |
| Active open orders | 5% | Directly unresolved enforcement |

### Top finding

**College of the Redwoods POTW (Rank 1, score 72.5)** — $3.5M outstanding, active ACL order, max severity score. This is both a compliance failure and a financial recovery priority.

See `FINDINGS.md` for the full narrative and `dashboard.html` for the interactive visualization.

---

## Decision Notes

### Tradeoffs

**Heuristic + ML**
The heuristic score is the primary ranking because it's explainable a regulator can see exactly why a facility ranked where it did. The ML model is a cross-check. Where both agree, high confidence. Where they disagree, that facility is worth a closer look.
**Min-max normalisation**
Scales all six scoring factors to the same 0–1 range so the weights actually mean what they say. The limitation is sensitivity to outliers. Percentile rank would be more robust but harder to explain to a non-technical audience.
**Monthly granularity**
Self-monitoring reports are filed monthly, so enforcement actions cluster on monthly cycles. Weekly or daily aggregation would add noise without adding signal.
**Keyword matching for text analysis**
Fast, reproducible, no external dependencies. An LLM-based classifier would be more powerful but adds cost and API dependency to the pipeline. Good enough for this dataset.

### Assumptions

- WDID is stable. If a facility was sold or its ID reassigned, the model treats it as two facilities. Not verifiable from this data alone.
- Severity ranks are judgment calls. Assigned based on the CIWQS enforcement hierarchy, not learned from data.
- Outstanding = assessed minus paid, floored at zero. Negative values treated as zero.
- Data stops at 2019. Recency scores are relative within that window. Cross-reference with live CIWQS before acting on these rankings.

### What I'd do with more time

1. **Join the inspections file** (37MB, 1987–2021) — inspection ratings are a leading indicator that predates formal enforcement by weeks or months.
2. **Operator-level deduplication** — ranks 18–21 are four buildings likely under one operator. Group by permittee/operator before finalizing the 25 slots.
3. **dbt conversion** — replace the three SQL files with dbt models for lineage, column docs, and `dbt test` assertions.
4. **Live data refresh** — cross-reference with CIWQS API for current enforcement status before finalizing review assignments.
5. **Predictive model** — with inspection outcomes as a target variable, a gradient boost on facility features would improve recall on truly high-risk facilities.

---

## Agentic Tool Usage

Built using **Claude (Sonnet 4.6, Anthropic)** as the primary agentic coding assistant:

I acted as architect. Claude AI acted as implementation assistant producing code to my specification.
