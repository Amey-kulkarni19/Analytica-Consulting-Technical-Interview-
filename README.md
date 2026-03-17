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

The water board issues thousands of enforcement actions each year but has limited staff for facility reviews. This pipeline structures the raw data for enforcement and permit into a simple relational model, then surfaces the 25 facilities with the largest unresolved financial liability and the most persistent enforcement history.

---

## Data sources and join key

| File | Rows | What it contains |
|---|---|---|
| `wastewater-enforcement-actions.csv` | ~44,500 | One row per enforcement action |
| `reg_meas_export_wastewaterpermitsorders.csv` | ~105,000 | One row per permit or regulatory order |

The two files are linked by **WDID** (Waste Discharger Identification Number) — the water board's facility identifier. This was discovered by inspecting column headers across both files and confirmed with a join profiler (checkout function "step_profile()") that found a ~86.4%% match rate. The remaining ~13.6% are facilities that appear in enforcement but have no corresponding permit record; they are kept in the model rather than dropped.

All WDID values are normalised to `UPPER(TRIM(...))` before joining to handle whitespace and case inconsistencies in the raw data.

---

## Data model

The model has **5 tables** across three layers.

**Staging (2 tables)** — `stg_enforcement` and `stg_permits`. The raw CSVs cleaned up: dates parsed, money columns cast from text to numbers, WDID normalised. No analysis happens here. They are kept as two separate tables because they have different schemas and different grains one is an event log, one is a registry.
4
**Dimension (1 table)** — `dim_facility`. One row per WDID with facility name, region, county, and place type. Built from both staging tables via UNION so that facilities with no permit record still appear. Without this, ~13.6% of enforcement records would have no facility metadata.

**Fact (1 table)** — `fact_enforcement`. One row per enforcement action. Stores the financial columns (assessed, paid, outstanding), dates, action type, and status. Primary key is the Enforcement ID (EID). Foreign key to `dim_facility` via WDID.

**Mart (1 table)** — `mart_facility_monthly`. Rolls `fact_enforcement` up to one row per facility per month with counts and totals pre-aggregated. The dashboard and answer query both read from here.

```
dim_facility  ──<  fact_enforcement
    WDID (PK)           WDID (FK)
                        EID  (PK)

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

---

## Data quality checks

| Check | Result |
|---|---|
| WDID null rate in enforcement | < 0 of 35,954 rows no nulls |
| Join match rate (WDID) | Computed at runtime — see data/outputs/join_report.txt |
| Date range | 1987–2019, no future dates |
| Zero assessment amount | 33,109 of 35,954 rows expected, most action types carry no financial penalty |
| Zero outstanding balance | 35,592 of 35,954 rows outstanding balance ranking is driven by a small number of high-value facilities |
| Negative outstanding balance | Floored to zero via `GREATEST(..., 0)` |
| Duplicate EIDs | Confirmed unique after normalisation |
| Region values | 12 distinct codes (1, 2, 3, 4, 5F, 5R, 5S, 6A, 6B, 7, 8, 9) plus 436 permits-only facilities labelled NA — excluded from regional charts |

---

## Why DuckDB

Single file, zero server setup, handles the full dataset in seconds. A reviewer can reproduce everything with one `pip install` and one command. In production this would be replaced by a managed warehouse with incremental loads and access controls.

---

## What I would do with more time

- Convert SQL to dbt for lineage, column docs, and data tests
- Join the inspections file (~37 MB, same data portal) inspection ratings are a leading indicator that precedes formal enforcement
- Investigate whether multiple WDIDs share a single operator the dataset has an agency name field that was not used in this model 
- Cross-reference against the live CIWQS API, as the dataset cuts off at 2019

---

## What I intentionally left out

- **Severity ranking** — the dataset lists action types but provides no official severity hierarchy.
- **ML risk scoring** — added complexity without changing the top-25 outcome in a meaningful way.

---

## Agentic tool usage

Built using **Claude (Sonnet 4.6, Anthropic)** as the primary coding assistant. I acted as architect and decision-maker; Claude generated implementation code to my specification.
