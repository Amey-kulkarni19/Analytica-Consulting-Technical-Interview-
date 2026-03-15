# Findings: CA Wastewater Enforcement — Facility Prioritization

**Prepared for:** Interview Panel (as client)
**Data source:** CA State Water Resources Control Board — CIWQS Enforcement Actions & Permits/Orders
**Coverage:** 12,540 regulated facilities · 35,835 enforcement actions · 1998–2019
**Pipeline:** `python pipeline.py run` → `warehouse.duckdb` → `data/outputs/top_25_facilities.csv`

---

## Executive Summary

We analyzed 20+ years of California wastewater enforcement data to recommend the 25 facilities that warrant priority review next quarter. Our model scores every facility across six risk dimensions — recency, frequency, severity, outstanding penalties, escalation trend, and open orders — and normalizes them to a common scale so no single factor dominates.

**The top recommendation is College of the Redwoods POTW (Humboldt County, Region 1)** — it holds $3.5M in outstanding penalties under an active Admin Civil Liability order with maximum-severity (7/7) actions. This is simultaneously a compliance failure and a financial recovery opportunity.

---

## Finding 1: Enforcement Is Escalating in Seriousness

Average action severity rose from **2.4 in 2000 to 3.4 by 2016** — a 40% increase over 16 years. More telling: the share of monetary enforcement actions (ACLs, MMPs, Cleanup & Abatement Orders) nearly **tripled from 5% in 2000 to 21% by 2016**.

This is not simply more enforcement — it's heavier enforcement. Regulators are escalating from informal letters and NOVs to formal financial penalties at a higher rate than at any point in the dataset. Facilities that received only informal letters five years ago are now receiving ACLs.

**Implication:** A facility's *current* severity profile is more predictive of near-term risk than its historical action count. The scoring model weights severity at 20% and the escalation trend at 10% to capture this.

---

## Finding 2: Region 5S Is the Compliance Hotspot

Region 5S (Central Valley — Sacramento/San Joaquin area) accounts for **18% of all enforcement actions** — more than any other region — and holds **$8.3M in outstanding penalties**. 10 of the top 25 priority facilities are in Region 5S.

Region 1 (North Coast, primarily Humboldt County) has a disproportionately high outstanding penalty burden relative to its action count — $7.2M outstanding on only 1,821 total actions. This suggests a small number of high-value, unresolved orders are driving the liability exposure.

**Implication:** A region-specific review approach makes logistical sense. The 10 Region 5S facilities on the priority list could be handled in a single field review cycle.

---

## Finding 3: Reporting Failures Are the Most Common Violation — But Spills Are the Most Severe

NLP analysis of 44,000+ enforcement description records reveals two distinct violation profiles:

| Theme | Volume | Avg Severity | % Monetary |
|---|---|---|---|
| Reporting / SMR failure | Highest (44% of actions) | 2.6 / 7 | Low |
| Spill / overflow | Moderate (13%) | 4.1 / 7 | Moderate |
| Financial / ACL | Lowest (5%) | **5.8 / 7** | High |
| Effluent exceedance | High (31%) | 3.9 / 7 | Moderate |

Reporting failures are by far the most common enforcement theme but carry relatively low severity — these are administrative compliance issues, often addressable through compliance assistance rather than formal enforcement. Spills and ACL-related violations are the highest-severity actions and the ones that carry financial consequences.

**Implication:** Facilities dominated by reporting violations may be candidates for compliance assistance programs rather than field review. Facilities with spill or ACL histories should be prioritized for in-person inspection.

---

## Finding 4: Four LA Apartment Buildings Should Be Reviewed as One Case

Ranks 18–21 (8383 Wilshire, Babylon Apartments, 6500 Wilshire, Beverly Atrium) are four Los Angeles residential buildings that received simultaneous Admin Civil Liability actions in December 2018, all with severity 7. They scored identically (59.1) and represent a single landlord/operator compliance sweep — not four independent compliance failures.

**Implication:** These four should be assigned as a single case review rather than consuming four of the 25 available review slots.

---

## Finding 5: $45M in Outstanding Penalties — 23% of Total Assessed

Of $196M in total assessed penalties across all facilities, **$45M (23%) remains unpaid**. The top 5 facilities by outstanding balance account for $15M alone:

| Facility | Region | Outstanding |
|---|---|---|
| Victorville SD CS | 6B | $6.3M |
| College of the Redwoods POTW | 1 | $3.5M |
| Harbor View Mutual Water Co | 5S | $2.8M |
| County San. District Joint Outfall CS | 4 | $1.3M |
| Malaga CWD WWTF | 5F | $1.2M |

Several of these are not in the top-25 priority list because their enforcement activity has been relatively quiet recently. However, they represent significant outstanding financial liability that warrants separate collections-focused follow-up.

---

## Methodology

### Data Model
- **`fact_enforcement`** — 44,475 enforcement actions (1 row per action)
- **`dim_facility`** — 12,540 unique facilities (coalesced from enforcement + permits sources using `WDID` as the join key)
- **`dim_action_type`** — 17 action types with assigned severity ranks (1–7) and monetary flags
- **`dim_time`** — full date spine with CA fiscal year (Jul–Jun)
- **`mart_facility_monthly`** — 33,260 rows (facility × month aggregates)
- **`mart_description_themes`** — NLP theme summary (8 themes × severity/penalty stats)

### Join Key Discovery
`WDID` (Water Discharge Identification Number) was identified as the primary join key by fetching and inspecting both raw CSV headers. It is present in both the enforcement actions and permits/orders source files. After trimming and upper-casing, the join match rate exceeded 94%.

### Priority Scoring
Six normalized components (all min-max scaled to [0,1]):

| Component | Weight | Method |
|---|---|---|
| Recency | 25% | Days since last action, inverted |
| Frequency | 25% | Actions in trailing 12 months |
| Severity | 20% | Weighted avg severity rank, 12 months |
| Outstanding penalties | 15% | Unpaid assessed minus paid |
| Escalation trend | 10% | Z-score: last-3mo rate vs prior-12mo baseline |
| Active open orders | 5% | Count of Active-status actions |

### NLP Approach
Eight violation themes were defined using domain-informed keyword patterns applied to the `DESCRIPTION` field via `REGEXP_MATCHES`. Each action can match multiple themes. Themes are correlated with severity rank and monetary action rates to distinguish administrative from substantive violations.

---

## What We'd Do With More Time

1. **Live data refresh** — dataset covers through 2019. Cross-reference with CIWQS API for current enforcement status before finalizing review assignments.
2. **Operator-level deduplication** — facilities 18–21 above reveal that multiple entries may share an operator. Grouping by operator/permittee would improve prioritization for large multi-facility operators.
3. **Predictive model** — with a target variable (e.g., "received ACL within 12 months of NOV"), a gradient boosted model on facility features would outperform the heuristic scoring formula.
4. **Inspections data join** — the CIWQS inspections CSV (37MB, 1987–2021) contains inspection ratings (Satisfactory/Unsatisfactory) that would add a leading indicator independent of formal enforcement.
5. **dbt conversion** — convert the three SQL files to dbt models for lineage tracking, column-level documentation, and automated testing.

---

## Agentic Tool Usage

This analysis was built using **Claude (Sonnet 4.6, Anthropic)** as the primary agentic coding assistant:

- **Schema discovery:** Claude fetched the live enforcement CSV directly from `data.ca.gov` to verify exact column names (enforcement uses `"UPPER CASE WITH SPACES"`, permits uses `lowercase_snake_case`) before writing any SQL — no assumptions from documentation.
- **Bug detection:** Claude identified that `GROUP BY 1` caused a DuckDB binder error when a source column and its alias shared the same name, and that min-max normalisation was missing from the scoring model (causing frequency to dominate due to unbounded log scale).
- **Iterative debugging:** Three rounds of SQL fixes were made based on actual runtime errors from the user's local environment — Claude adjusted each fix based on the real error message rather than guessing.
- **Dashboard generation:** Claude wrote the full Chart.js interactive dashboard HTML from the analysed data, including the NLP theme visualization and the filterable priority table.

Human oversight was applied throughout: validating join match rates, reviewing the top-25 output for anomalies (the LA apartment cluster, the drinking water plant flag), and adjusting scoring weights to reflect domain-appropriate priorities.
