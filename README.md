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
Open `dashboard.html` in any browser — no server needed.
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
    dim_time                   ← date spine 1987–2019, CA fiscal year
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
Each step is idempotent — `CREATE OR REPLACE TABLE` ensures re-runs don't duplicate.
`warehouse.duckdb` persists between runs and can be queried directly:
```bash
duckdb data/warehouse.duckdb "SELECT * FROM top_25_facilities LIMIT 5"
```

---

## Relational Model

### Discovered join keys

| Key | Tables | How discovered |
|-----|--------|----------------|
| `WDID` | enforcement + permits + all dims | Fetched live CSV headers from `data.ca.gov`; confirmed present in both raw files. Normalized to `UPPER(TRIM(...))` to handle whitespace/case inconsistencies. Join match rate: 94%+ |
| `ENFORCEMENT ID (EID)` | fact only | Natural PK — confirmed unique per row in enforcement CSV |
| `REG MEASURE ID` | enforcement + permits | Links an enforcement action to its underlying permit; used for lineage |

### Key design decisions

**Two staging tables, not one.** The permits and enforcement CSVs use completely different column naming conventions (`"UPPER CASE WITH SPACES"` vs `lowercase_snake_case`). A single staging layer would require unmaintainable conditional logic. Two clean staging tables that coalesce into shared dims is cleaner.

**`dim_facility` coalesces from both sources.** Facilities appear in enforcement without a corresponding permit record (unregulated sites, expired permits). Building `dim_facility` from permits only — the obvious first instinct — drops ~6% of WDIDs from the fact table. The UNION + COALESCE pattern ensures full coverage.

**Severity rank assigned at model time.** `dim_action_type` carries `severity_rank` (1–7) and `is_monetary` flag, and these are joined into `fact_enforcement` at build time. This means `mart_facility_monthly` and `03_answer.sql` never need to re-join the dimension — they read pre-computed severity directly from the fact.

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

- **Heuristic scoring vs. ML model.** A gradient-boosted classifier trained on "received ACL within 12 months" would outperform this scoring formula. The heuristic was chosen because it's auditable and explainable to a regulator — a black-box model score is harder to defend in an enforcement context.

- **Min-max normalisation vs. percentile rank.** Min-max is sensitive to outliers (one facility with 10× the actions of any other compresses everyone else to near-zero on frequency). Percentile rank normalisation would be more robust. The tradeoff is interpretability — "you're in the 95th percentile for frequency" is less intuitive to a client than "your normalised frequency score is 0.82".

- **Monthly mart granularity.** Weekly would add noise without signal — SMRs are monthly reports, so violations and actions cluster on monthly cycles. Daily would make the escalation z-score calculation unstable.

- **NLP via keyword patterns vs. embeddings.** Regex is fast, explainable, and doesn't require a model deployment. For a production system, sentence-transformer embeddings + k-means clustering would discover themes the regex misses. The regex approach was chosen for reproducibility with no external API calls.

### Assumptions

- `WDID` is stable over time. Facility mergers, acquisitions, or WDID reassignments would introduce false matches — not verifiable from this dataset alone.
- Severity ranks for action types are assigned based on the published CIWQS enforcement hierarchy, not inferred from the data.
- "Outstanding amount" = `TOTAL ASSESSMENT AMOUNT` − `TOTAL $ PAID/COMPLETED AMOUNT`, floored at 0. Negative values (over-payment) are treated as zero.
- The dataset covers through early 2019. Recency scores are relative within that window.

### What I'd do with more time

1. **Join the inspections file** (37MB, 1987–2021) — inspection ratings are a leading indicator that predates formal enforcement by weeks or months.
2. **Operator-level deduplication** — ranks 18–21 are four buildings likely under one operator. Group by permittee/operator before finalizing the 25 slots.
3. **dbt conversion** — replace the three SQL files with dbt models for lineage, column docs, and `dbt test` assertions.
4. **Live data refresh** — cross-reference with CIWQS API for current enforcement status before finalizing review assignments.
5. **Predictive model** — with inspection outcomes as a target variable, a gradient boost on facility features would improve recall on truly high-risk facilities.

---

## Agentic Tool Usage

Built using **Claude (Sonnet 4.6, Anthropic)** as the primary agentic coding assistant throughout:

**Schema discovery before writing code.** Claude fetched the live CSV from `data.ca.gov` to verify exact column names before writing any SQL. This revealed that the permits file uses `lowercase_snake_case` while the enforcement file uses `"UPPER CASE WITH SPACES"` — a mismatch that would have caused silent failures if assumed from documentation.

**Iterative debugging on real errors.** Three rounds of SQL fixes were made from actual DuckDB error messages (binder error on `GROUP BY 1`, `place_subtype` column not in `dim_facility`, normalisation scale mismatch in scoring). Claude diagnosed each from the error text and proposed targeted fixes rather than rewriting from scratch.

**Data-driven dashboard.** After loading and profiling the `mart_facility_monthly` data, Claude computed all chart statistics (region breakdowns, year-over-year trends, severity by facility type) and generated the complete `dashboard.html` — Chart.js charts, NLP theme bars, filterable priority table — as a single client-ready file.

**Critical review of outputs.** Claude reviewed the `top_25_facilities.csv` results and flagged: (1) the recency signal is effectively flat because the data stops at 2019, (2) ranks 18–21 are four buildings from one operator sweep and shouldn't consume 4 review slots, (3) rank 15 is a drinking water treatment plant, not a wastewater facility.

Human judgment was applied at every step: validating join logic, reviewing results for anomalies, and adjusting scoring weights to reflect regulatory domain priorities.
