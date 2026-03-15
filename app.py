"""
app.py — CA Wastewater Enforcement Prioritization Dashboard
Run: python -m streamlit run app.py

Reads live from data/warehouse.duckdb — no hardcoded numbers.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).resolve().parent / "data" / "warehouse.duckdb"

st.set_page_config(
    page_title="CA Wastewater Enforcement — Prioritization",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Styling ───────────────────────────────────────────────────────────────────

st.markdown("""
<style>
  [data-testid="stMetricValue"] { font-size: 1.8rem; font-weight: 700; }
  [data-testid="stMetricLabel"] { font-size: 0.75rem; text-transform: uppercase;
                                   letter-spacing: .05em; color: #6b7280; }
  .section-header { font-size: 0.7rem; font-weight: 600; text-transform: uppercase;
                    letter-spacing: .08em; color: #9ca3af; margin: 1.5rem 0 0.5rem; }
  .stDataFrame { font-size: 13px; }
</style>
""", unsafe_allow_html=True)

# ── DB connection ─────────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        st.error(f"Database not found at {DB_PATH}. Run `python pipeline.py run` first.")
        st.stop()
    return duckdb.connect(str(DB_PATH), read_only=True)

con = get_conn()

# ── Data loaders (cached) ─────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_mart() -> pd.DataFrame:
    df = con.execute("SELECT * FROM mart_facility_monthly").df()
    df["month"] = pd.to_datetime(df["month"])
    df["year"]  = df["month"].dt.year
    return df

@st.cache_data(ttl=300)
def load_top25() -> pd.DataFrame:
    return con.execute("SELECT * FROM top_25_facilities").df()

@st.cache_data(ttl=300)
def load_ml_scores() -> pd.DataFrame | None:
    tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_name = 'ml_facility_scores'
    """).df()
    if tables.empty:
        return None
    return con.execute("SELECT * FROM ml_facility_scores").df()

@st.cache_data(ttl=300)
def load_top25_ml() -> pd.DataFrame | None:
    tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_name IN ('top_25_facilities','ml_facility_scores')
    """).df()
    if len(tables) < 2:
        return None
    return con.execute("""
        SELECT
            t.priority_rank,
            ROUND(t.priority_score_pct, 1)  AS heuristic_score,
            ROUND(m.ml_risk_pct, 1)          AS ml_score,
            t.wdid, t.facility_name, t.county, t.region, t.place_type,
            t.why_prioritized, t.actions_12mo,
            ROUND(t.avg_severity_12mo, 2)    AS avg_severity,
            t.outstanding_penalties,
            t.current_active_actions
        FROM top_25_facilities t
        LEFT JOIN ml_facility_scores m ON t.wdid = m.wdid
        ORDER BY t.priority_rank
    """).df()

mart    = load_mart()
top25   = load_top25()
ml      = load_ml_scores()
t25_ml  = load_top25_ml()

# ── Sidebar filters ───────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 💧 Wastewater Enforcement")
    st.markdown("CA State Water Resources Control Board · CIWQS · 1998–2019")
    st.divider()

    st.markdown("### Filters")
    regions = sorted(mart["region"].dropna().unique().tolist())
    sel_regions = st.multiselect("Region", regions, default=regions,
                                  help="SWRCB Regional Water Board regions")

    year_min = int(mart["year"].min())
    year_max = int(mart["year"].max())
    sel_years = st.slider("Year range", year_min, year_max,
                           (max(year_min, 1999), year_max))

    place_types = sorted(mart["place_type"].dropna().unique().tolist())
    sel_types = st.multiselect("Facility type", place_types, default=place_types)

    st.divider()
    ml_available = ml is not None
    st.markdown(f"**ML model:** {'✅ trained' if ml_available else '⚠️ not yet run'}")
    if not ml_available:
        st.caption("Run `python 05_ml.py` to add ML risk scores")
    st.divider()
    st.caption("Pipeline: Python · DuckDB · scikit-learn")
    st.caption("Dashboard: Streamlit · Plotly")
    st.caption("Built with Claude (Anthropic)")

# ── Apply filters ─────────────────────────────────────────────────────────────

filtered = mart[
    mart["region"].isin(sel_regions) &
    mart["year"].between(sel_years[0], sel_years[1]) &
    mart["place_type"].isin(sel_types)
]

# ── Header ────────────────────────────────────────────────────────────────────

st.title("CA Wastewater Enforcement — Facility Prioritization")
st.caption(
    "Data: CA State Water Resources Control Board (CIWQS) · "
    f"Showing {sel_years[0]}–{sel_years[1]} · "
    f"{len(sel_regions)} of {len(regions)} regions selected"
)

# ── KPI row ───────────────────────────────────────────────────────────────────

total_actions     = int(filtered["enforcement_count"].sum())
total_outstanding = filtered["total_outstanding"].sum()
total_assessed    = filtered["total_assessment"].sum()
avg_severity      = filtered["avg_severity_rank"].mean()
unique_facs       = filtered["wdid"].nunique()
monetary_rate     = filtered["monetary_count"].sum() / max(total_actions, 1) * 100

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Facilities",          f"{unique_facs:,}")
k2.metric("Enforcement Actions", f"{total_actions:,}")
k3.metric("Total Assessed",      f"${total_assessed/1e6:.1f}M")
k4.metric("Outstanding",         f"${total_outstanding/1e6:.1f}M",
          delta=f"{total_outstanding/max(total_assessed,1)*100:.0f}% unpaid",
          delta_color="inverse")
k5.metric("Avg Severity",        f"{avg_severity:.2f} / 7")
k6.metric("Monetary Action Rate",f"{monetary_rate:.1f}%")

st.divider()

# ── Tab layout ────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Trends", "🗺️ Regional Analysis", "🎯 Priority Facilities", "🤖 ML Model"
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — TRENDS
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown('<div class="section-header">Enforcement volume over time</div>',
                unsafe_allow_html=True)

    yearly = (filtered
              .groupby("year")
              .agg(
                  total_actions=("enforcement_count", "sum"),
                  monetary=("monetary_count", "sum"),
                  avg_sev=("avg_severity_rank", "mean"),
                  assessed=("total_assessment", "sum"),
              )
              .reset_index())
    yearly["non_monetary"] = yearly["total_actions"] - yearly["monetary"]
    yearly["monetary_pct"] = (yearly["monetary"] / yearly["total_actions"].clip(lower=1) * 100).round(1)

    col1, col2 = st.columns(2)

    with col1:
        fig = go.Figure()
        fig.add_bar(x=yearly["year"], y=yearly["non_monetary"],
                    name="Non-monetary", marker_color="#93c5fd")
        fig.add_bar(x=yearly["year"], y=yearly["monetary"],
                    name="Monetary (ACL/MMP)", marker_color="#1d4ed8")
        fig.update_layout(
            barmode="stack", title="Enforcement actions by year",
            xaxis_title=None, yaxis_title="Actions",
            legend=dict(orientation="h", y=1.1),
            height=340, margin=dict(t=50, b=30, l=40, r=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = go.Figure()
        fig2.add_scatter(x=yearly["year"], y=yearly["avg_sev"].round(2),
                         mode="lines+markers", name="Avg severity",
                         line=dict(color="#dc2626", width=2),
                         marker=dict(size=5))
        fig2.add_scatter(x=yearly["year"], y=yearly["monetary_pct"],
                         mode="lines+markers", name="% monetary actions",
                         line=dict(color="#f97316", width=2, dash="dot"),
                         marker=dict(size=5), yaxis="y2")
        fig2.update_layout(
            title="Severity trend & monetary action rate",
            yaxis=dict(title="Avg severity (1–7)", range=[1, 7]),
            yaxis2=dict(title="% monetary", overlaying="y",
                        side="right", range=[0, 40]),
            legend=dict(orientation="h", y=1.1),
            height=340, margin=dict(t=50, b=30, l=40, r=60),
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Assessment over time
    st.markdown('<div class="section-header">Penalty assessment over time</div>',
                unsafe_allow_html=True)
    fig3 = px.area(yearly, x="year", y="assessed",
                   title="Total assessed penalties ($) by year",
                   labels={"assessed": "Assessed ($)", "year": "Year"},
                   color_discrete_sequence=["#6366f1"])
    fig3.update_yaxes(tickprefix="$", tickformat=",.0f")
    fig3.update_layout(height=280, margin=dict(t=40, b=30, l=60, r=20))
    st.plotly_chart(fig3, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — REGIONAL ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown('<div class="section-header">By region</div>',
                unsafe_allow_html=True)

    by_region = (filtered
                 .groupby("region")
                 .agg(
                     facilities=("wdid", "nunique"),
                     actions=("enforcement_count", "sum"),
                     outstanding=("total_outstanding", "sum"),
                     avg_sev=("avg_severity_rank", "mean"),
                     monetary=("monetary_count", "sum"),
                 )
                 .reset_index()
                 .sort_values("actions", ascending=False))
    by_region["monetary_pct"] = (by_region["monetary"] /
                                  by_region["actions"].clip(lower=1) * 100).round(1)

    c1, c2 = st.columns(2)
    with c1:
        fig = px.bar(by_region, x="region", y="actions",
                     color="avg_sev",
                     color_continuous_scale="RdYlBu_r",
                     range_color=[1, 7],
                     title="Enforcement actions by region (colour = avg severity)",
                     labels={"actions": "Actions", "region": "Region",
                             "avg_sev": "Avg severity"})
        fig.update_layout(height=360, margin=dict(t=50, b=30))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig2 = px.bar(by_region, x="region",
                      y="outstanding",
                      title="Outstanding penalties by region ($)",
                      color_discrete_sequence=["#f97316"],
                      labels={"outstanding": "Outstanding ($)", "region": "Region"})
        fig2.update_yaxes(tickprefix="$", tickformat=",.0f")
        fig2.update_layout(height=360, margin=dict(t=50, b=30))
        st.plotly_chart(fig2, use_container_width=True)

    # Facility type breakdown
    st.markdown('<div class="section-header">By facility type</div>',
                unsafe_allow_html=True)
    by_type = (filtered
               .groupby("place_type")
               .agg(
                   facilities=("wdid", "nunique"),
                   actions=("enforcement_count", "sum"),
                   avg_sev=("avg_severity_rank", "mean"),
                   monetary_rate=("monetary_count",
                                  lambda x: x.sum() / max(filtered.loc[x.index, "enforcement_count"].sum(), 1) * 100),
               )
               .reset_index()
               .sort_values("actions", ascending=False)
               .head(12))

    fig3 = px.scatter(by_type, x="actions", y="avg_sev",
                      size="facilities", color="monetary_rate",
                      hover_name="place_type",
                      color_continuous_scale="Oranges",
                      title="Facility type: volume vs severity (size = # facilities, colour = % monetary)",
                      labels={"actions": "Total actions", "avg_sev": "Avg severity",
                              "monetary_rate": "% monetary"})
    fig3.update_layout(height=400, margin=dict(t=50, b=30))
    st.plotly_chart(fig3, use_container_width=True)

    # Region summary table
    st.markdown('<div class="section-header">Region summary table</div>',
                unsafe_allow_html=True)
    display_reg = by_region.copy()
    display_reg["outstanding"] = display_reg["outstanding"].apply(
        lambda x: f"${x:,.0f}")
    display_reg["avg_sev"] = display_reg["avg_sev"].round(2)
    display_reg.columns = ["Region", "Facilities", "Actions",
                            "Outstanding", "Avg Severity", "Monetary", "% Monetary"]
    st.dataframe(display_reg, hide_index=True, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — PRIORITY FACILITIES
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown('<div class="section-header">Top 25 priority facilities for Q1 review</div>',
                unsafe_allow_html=True)
    st.caption(
        "Scored across 6 dimensions: recency (25%), frequency (25%), severity (20%), "
        "outstanding penalties (15%), escalation trend (10%), active orders (5%)"
    )

    display_data = t25_ml if t25_ml is not None else top25

    # Filters
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        reg_opts = ["All"] + sorted(display_data["region"].dropna().unique().tolist())
        sel_reg = st.selectbox("Filter by region", reg_opts)
    with fc2:
        why_opts = ["All"] + sorted(display_data["why_prioritized"].dropna().unique().tolist())
        sel_why = st.selectbox("Filter by driver", why_opts)
    with fc3:
        min_score = st.slider("Min heuristic score",
                              float(display_data["priority_rank"].min() * 0),
                              float(display_data.get("heuristic_score",
                                    display_data.get("priority_score_pct", pd.Series([100]))).max()),
                              0.0) if "heuristic_score" in display_data.columns or \
                                      "priority_score_pct" in display_data.columns else 0.0

    tbl = display_data.copy()
    if sel_reg != "All":
        tbl = tbl[tbl["region"] == sel_reg]
    if sel_why != "All":
        tbl = tbl[tbl["why_prioritized"] == sel_why]

    # Format for display
    score_col = "heuristic_score" if "heuristic_score" in tbl.columns else "priority_score_pct"
    if "outstanding_penalties" in tbl.columns:
        tbl["outstanding_penalties"] = tbl["outstanding_penalties"].apply(
            lambda x: f"${x:,.0f}" if pd.notna(x) and x > 0 else "—")

    st.dataframe(
        tbl,
        hide_index=True,
        use_container_width=True,
        column_config={
            "priority_rank":        st.column_config.NumberColumn("Rank", width="small"),
            score_col:              st.column_config.ProgressColumn(
                                        "Heuristic score", min_value=0, max_value=100,
                                        format="%.1f"),
            "ml_score":             st.column_config.ProgressColumn(
                                        "ML score", min_value=0, max_value=100,
                                        format="%.1f") if "ml_score" in tbl.columns else None,
            "facility_name":        st.column_config.TextColumn("Facility", width="large"),
            "why_prioritized":      st.column_config.TextColumn("Primary driver"),
            "outstanding_penalties":st.column_config.TextColumn("Outstanding $"),
            "avg_severity":         st.column_config.NumberColumn("Avg severity", format="%.2f"),
            "current_active_actions":st.column_config.NumberColumn("Active orders"),
        }
    )

    # Score comparison chart (heuristic vs ML)
    if t25_ml is not None and "ml_score" in t25_ml.columns:
        st.markdown('<div class="section-header">Heuristic score vs ML risk score</div>',
                    unsafe_allow_html=True)
        st.caption("Facilities where the two scores diverge significantly may warrant closer review")

        fig_cmp = go.Figure()
        names = t25_ml["facility_name"].str[:30]
        fig_cmp.add_bar(x=names, y=t25_ml["heuristic_score"],
                        name="Heuristic score", marker_color="#3b82f6", opacity=0.8)
        fig_cmp.add_bar(x=names, y=t25_ml["ml_score"],
                        name="ML risk score", marker_color="#ef4444", opacity=0.8)
        fig_cmp.update_layout(
            barmode="group",
            xaxis_tickangle=-45,
            yaxis_title="Score (0–100)",
            legend=dict(orientation="h", y=1.05),
            height=420, margin=dict(t=30, b=120, l=40, r=20),
        )
        st.plotly_chart(fig_cmp, use_container_width=True)
    else:
        st.info("Run `python 05_ml.py` to add ML risk scores to this chart.", icon="🤖")

    # Outstanding penalties chart
    st.markdown('<div class="section-header">Outstanding penalties — top 25</div>',
                unsafe_allow_html=True)
    raw_top25 = load_top25()
    fig_money = px.bar(
        raw_top25.sort_values("outstanding_penalties", ascending=True).tail(15),
        x="outstanding_penalties", y="facility_name",
        orientation="h",
        color="priority_score_pct",
        color_continuous_scale="RdYlBu_r",
        title="Outstanding unpaid penalties ($) — top 15 of priority-25",
        labels={"outstanding_penalties": "Outstanding ($)",
                "facility_name": "", "priority_score_pct": "Priority score"},
    )
    fig_money.update_xaxes(tickprefix="$", tickformat=",.0f")
    fig_money.update_layout(height=420, margin=dict(t=50, b=30, l=220, r=40))
    st.plotly_chart(fig_money, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — ML MODEL
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("Machine Learning — Monetary Action Prediction")
    # st.markdown("""
    # A **Gradient Boosted Classifier** (scikit-learn) trained to predict whether a facility
    # will receive a **monetary enforcement action** (ACL, MMP, or Cleanup & Abatement Order)
    # in the following 12 months.

    # **Training approach:**
    # - Features: 13 trailing metrics per facility per year (action counts, severity,
    #   outstanding penalties, escalation ratio, recency)
    # - Target: did the facility receive a monetary action in the next 12 months?
    # - **Temporal split**: trained on data before 2016, tested on 2016+ to prevent data leakage
    # - No future information leaks into past predictions

    # **Why this matters:** The heuristic scoring formula uses weights chosen by the analyst.
    # The ML model *learns* which features actually predict future monetary enforcement from
    # historical patterns — a useful cross-check on the heuristic ranking.
    # """)

    if ml is not None:
        st.success("ML model has been trained and scores are available.", icon="✅")
        st.divider()

        col1, col2, col3 = st.columns(3)
        col1.metric("Facilities scored", f"{len(ml):,}")
        col2.metric("Highest ML risk", f"{ml['ml_risk_pct'].max():.1f}%")
        col3.metric("Median ML risk", f"{ml['ml_risk_pct'].median():.1f}%")

        st.markdown('<div class="section-header">ML risk score distribution</div>',
                    unsafe_allow_html=True)
        fig_dist = px.histogram(ml, x="ml_risk_pct", nbins=40,
                                title="Distribution of ML risk scores across all facilities",
                                labels={"ml_risk_pct": "ML risk score (%)"},
                                color_discrete_sequence=["#ef4444"])
        fig_dist.update_layout(height=300, margin=dict(t=50, b=30))
        st.plotly_chart(fig_dist, use_container_width=True)

        st.markdown('<div class="section-header">Top 20 facilities by ML risk score</div>',
                    unsafe_allow_html=True)
        top_ml = ml.head(20)[["facility_name", "region", "county",
                                "ml_risk_pct", "ml_percentile",
                                "total_actions_24mo", "avg_severity_24mo",
                                "total_outstanding", "monetary_rate_24mo"]].copy()
        top_ml["total_outstanding"] = top_ml["total_outstanding"].apply(
            lambda x: f"${x:,.0f}" if x > 0 else "—")
        top_ml["monetary_rate_24mo"] = (top_ml["monetary_rate_24mo"] * 100).round(1)
        top_ml.columns = ["Facility", "Region", "County", "ML Score (%)",
                           "Percentile", "Actions (24mo)", "Avg Severity",
                           "Outstanding $", "Monetary Rate (%)"]
        st.dataframe(top_ml, hide_index=True, use_container_width=True,
                     column_config={
                         "ML Score (%)": st.column_config.ProgressColumn(
                             "ML Score (%)", min_value=0, max_value=100, format="%.1f"),
                     })

        st.markdown('<div class="section-header">Feature importance</div>',
                    unsafe_allow_html=True)
        st.info(
            "Feature importances are printed to the console when you run `python 05_ml.py`. "
            "The top predictors are typically: monetary_rate_24mo, avg_severity_24mo, "
            "total_outstanding, and escalation_ratio.",
            icon="ℹ️"
        )
    else:
        st.warning(
            "ML model has not been run yet. To train and score all facilities:",
            icon="⚠️"
        )
        st.code("python 05_ml.py", language="bash")
        st.markdown("""
        This will:
        1. Engineer 13 features per facility per year from `mart_facility_monthly`
        2. Train a Gradient Boosted Classifier with a temporal train/test split
        3. Print model evaluation metrics (ROC-AUC, precision/recall)
        4. Write `ml_facility_scores` table to `warehouse.duckdb`
        5. Export `data/marts/ml_facility_scores.csv` and `top25_with_ml_scores.csv`
        """)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Data: CA State Water Resources Control Board (CIWQS) · "
    "Pipeline: Python + DuckDB · "
    "ML: scikit-learn Gradient Boosted Classifier · "
    "Dashboard: Streamlit + Plotly · "
    "Built with Claude (Anthropic Sonnet 4.6)"
)
