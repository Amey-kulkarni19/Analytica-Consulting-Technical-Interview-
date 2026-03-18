# CA Wastewater Enforcement — Prioritization Pipeline

**Dataset:** CA State Water Resources Control Board (CIWQS) — public enforcement and permits data  
**Client question:** *"We can only review 25 facilities next quarter. Which should we prioritize, and why?"*

---

## Quickstart

```bash
pip install -r requirements.txt
python pipeline.py run
```

First run downloads ~30 MB of raw CSV. Subsequent runs skip the download.

---

## Business problem

The water board issues thousands of enforcement actions each year but has limited staff for facility reviews. This pipeline structures the raw enforcement and permit data into a simple relational model, then surfaces the 25 facilities with the largest unresolved financial liability and the most persistent enforcement history.

---

## Data sources and join key

| File | Rows | What it contains |
|---|---|---|
| `wastewater-enforcement-actions.csv` | ~44,500 | One row per enforcement action |
| `reg_meas_export_wastewaterpermitsorders.csv` | ~105,000 | One row per permit or regulatory order |

The two files are linked by **WDID** (Waste Discharger Identification Number) — the water board's facility identifier. This was confirmed by inspecting the column headers in `data/outputs/raw_schema.txt`. The profiler also computes the actual WDID match rate between the two files and writes it to `data/outputs/join_report.txt`. Some facilities appear in enforcement but have no permit record — they are kept in the model rather than dropped.

All WDID values are normalised to `UPPER(TRIM(...))` before joining to handle whitespace and case inconsistencies in the raw data.

---

## Data model

The model has **6 tables** across three layers.

**Staging (2 tables)** — `stg_enforcement` and `stg_permits`. The raw CSVs cleaned up: dates parsed, money columns cast from text to numbers, WDID normalised. No analysis happens here. They are kept as two separate tables because they have different schemas and different grains — one is an event log, one is a registry.

**Dimensions (2 tables)**

`dim_facility` — one row per WDID with facility name, region, county, and place type. Built from both staging tables via UNION so that facilities with no permit record still appear.

`dim_action_type` — one row per distinct enforcement action type with a single derived column: `is_formal`. Formal actions (orders, penalties, cease and desist, referrals) carry legal weight and create binding obligations. Informal actions (oral communications, staff letters, notices of violation) do not. This is a structural distinction sourced from how the enforcement process works, not a subjective severity judgment.

Currently `is_formal` is denormalised onto `fact_enforcement` and surfaces in the top 25 table so a reviewer can see how many of a facility's actions were legally binding vs informal notices. The broader value is in what it enables: filtering facilities by formal action rate, comparing regions by escalation patterns, or flagging facilities where enforcement repeatedly reaches formal actions without resolution. Keeping this logic in the data model rather than the dashboard means it is available to any query against the warehouse.

**Fact (1 table)** — `fact_enforcement`. One row per enforcement action. Stores the financial columns (assessed, paid, outstanding), dates, action type, and status. Primary key is the Enforcement ID (EID). Foreign key to `dim_facility` via WDID.

**Mart (1 table)** — `mart_facility_monthly`. Rolls `fact_enforcement` up to one row per facility per month with counts and totals pre-aggregated. The dashboard and answer query both read from here.

```
dim_facility    ──<  fact_enforcement  >──  dim_action_type
    WDID (PK)            WDID (FK)              enforcement_action_type (PK)
                         EID  (PK)
                         enforcement_action_type (FK)

fact_enforcement  ──<  mart_facility_monthly
    WDID                    WDID
    effective_date          month
```

---

## Ranking logic

Facilities are ranked by three observable dimensions in order:

1. **Outstanding balance** — total assessed penalties minus total paid, floored at zero
2. **Total enforcement actions** — count of all actions across the dataset
3. **Most recent action date** — recency as a tiebreaker

No composite score. Each dimension is shown as a separate column so the reasoning is transparent and auditable.

---

## Data quality checks

| Check | Result |
|---|---|
| WDID null rate in enforcement | 0 of 35,954 rows — no nulls |
| Join match rate (WDID) | Computed at runtime — see `data/outputs/join_report.txt` |
| Date range | 1990–2019 after filtering misparsed dates (some raw values like `12/29/16` parsed as year 16 AD — treated as NULL) |
| Zero assessment amount | 33,109 of 35,954 rows — expected, most action types carry no financial penalty |
| Zero outstanding balance | 35,592 of 35,954 rows — outstanding balance ranking is driven by a small number of high-value facilities |
| Negative outstanding balance | Floored to zero via `GREATEST(..., 0)` |
| Region values | 12 distinct codes (1, 2, 3, 4, 5F, 5R, 5S, 6A, 6B, 7, 8, 9) plus 436 permits-only facilities labelled NA — excluded from regional charts |
| Duplicate EIDs | Confirmed unique after normalisation |

---

## Why DuckDB

Single file, zero server setup, handles the full dataset in seconds. A reviewer can reproduce everything with one `pip install` and one command. In production this would be replaced by a managed warehouse with incremental loads and access controls.

---

## What I would do with more time

- Convert SQL to dbt for lineage, column docs, and data tests
- Join the inspections file (~37 MB, same data portal) — inspection ratings are a leading indicator that precedes formal enforcement
- Investigate whether multiple WDIDs share a single operator — the dataset has an agency name field that was not used in this model
- Cross-reference against the live CIWQS API, as the dataset cuts off at 2019

---

## What I intentionally left out

- **Severity ranking** — the dataset lists action types but provides no official severity hierarchy. I chose not to assign ranks that I cannot source from the data or documentation.
- **ML risk scoring** — added complexity without changing the top-25 outcome in a meaningful way.

---

## Agentic tool usage

Built using **Claude (Sonnet 4.6, Anthropic)** as the primary coding assistant. I acted as architect and decision-maker; Claude generated implementation code to my specification.
