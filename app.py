"""
app.py - CA Wastewater Enforcement Prioritization Dashboard
Run: python -m streamlit run app.py
Requires: python pipeline.py run
"""

from __future__ import annotations
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path(__file__).resolve().parent / "data" / "warehouse.duckdb"

st.set_page_config(
    page_title="CA Wastewater Enforcement",
    page_icon="",
    layout="wide",
)

@st.cache_resource
def get_conn() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        st.error("Database not found. Run `python pipeline.py run` first.")
        st.stop()
    return duckdb.connect(str(DB_PATH), read_only=True)

con = get_conn()

@st.cache_data(ttl=300)
def load_mart() -> pd.DataFrame:
    df = con.execute("SELECT * EXCLUDE (year) FROM mart_facility_monthly").df()
    df["month"] = pd.to_datetime(df["month"])
    return df

@st.cache_data(ttl=300)
def load_priority() -> pd.DataFrame:
    return con.execute("SELECT * FROM priority_facilities ORDER BY rank").df()

mart     = load_mart()
priority = load_priority()

# Official SWRCB region names for display in tables.
# 5S/5F/5R are sub-offices of Region 5 (Central Valley).
# 6A/6B are sub-offices of Region 6 (Lahontan).
REGION_LABELS = {
    "1":  "1 - North Coast",
    "2":  "2 - San Francisco Bay",
    "3":  "3 - Central Coast",
    "4":  "4 - Los Angeles",
    "5S": "5S - Central Valley (Sacramento)",
    "5F": "5F - Central Valley (Fresno)",
    "5R": "5R - Central Valley (Redding)",
    "6A": "6A - Lahontan (South Lake Tahoe)",
    "6B": "6B - Lahontan (Victorville)",
    "7":  "7 - Colorado River",
    "8":  "8 - Santa Ana",
    "9":  "9 - San Diego",
}

# For charts: consolidate sub-offices so Region 5 and 6 appear as single bars.
REGION_CHART_LABELS = {
    "1":  "1 - North Coast",
    "2":  "2 - San Francisco Bay",
    "3":  "3 - Central Coast",
    "4":  "4 - Los Angeles",
    "5S": "5 - Central Valley",
    "5F": "5 - Central Valley",
    "5R": "5 - Central Valley",
    "6A": "6 - Lahontan",
    "6B": "6 - Lahontan",
    "7":  "7 - Colorado River",
    "8":  "8 - Santa Ana",
    "9":  "9 - San Diego",
}

def label_region(df, col="region"):
    """Apply full sub-office labels - for tables."""
    df = df.copy()
    df[col] = df[col].map(REGION_LABELS).fillna(df[col])
    return df

def chart_region(df, col="region"):
    """Consolidate sub-offices into parent region - for charts."""
    df = df.copy()
    df[col] = df[col].map(REGION_CHART_LABELS).fillna(df[col])
    return df

st.title("CA Wastewater Enforcement - Facility Prioritization")
st.caption("Source: CA State Water Resources Control Board (CIWQS) - Dataset covers enforcement actions through 2019")
st.divider()

tab_overview, tab_top25, tab_region = st.tabs(["Overview", "Top 25 facilities", "By region"])

# -- TAB 1: OVERVIEW ------------------------------------------------------------

with tab_overview:

    total_facilities  = mart["wdid"].nunique()
    total_actions     = int(mart["enforcement_count"].sum())
    total_assessed    = mart["total_assessment"].sum()
    total_outstanding = mart["total_outstanding"].sum()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Facilities in dataset",    f"{total_facilities:,}")
    k2.metric("Total enforcement actions", f"{total_actions:,}")
    k3.metric("Total assessed penalties",  f"${total_assessed / 1e6:.1f}M")
    k4.metric("Outstanding balance",       f"${total_outstanding / 1e6:.1f}M")



# -- TAB 2: TOP 25 --------------------------------------------------------------

with tab_top25:

    st.subheader("Top 25 facilities recommended for review")
    st.markdown(
        "Ranked by **outstanding balance** first, then **total enforcement actions**, "
        "then **most recent action date**. No composite score - each dimension is a "
        "separate column so the reasoning is transparent."
    )

    display = label_region(priority.copy())
    display["total_assessed"]      = display["total_assessed"].apply(lambda x: f"${x:,.0f}")
    display["total_paid"]          = display["total_paid"].apply(lambda x: f"${x:,.0f}")
    display["outstanding_balance"] = display["outstanding_balance"].apply(lambda x: f"${x:,.0f}")
    display["most_recent_action"]  = pd.to_datetime(
        display["most_recent_action"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    st.dataframe(
        display[[
            "rank", "facility_name", "region", "county",
            "total_actions", "actions_last_3yr",
            "most_recent_action", "total_assessed", "total_paid", "outstanding_balance",
        ]].rename(columns={
            "rank":               "Rank",
            "facility_name":      "Facility",
            "region":             "Region",
            "county":             "County",
            "total_actions":      "Total actions",
            "actions_last_3yr":   "Actions (last 3yr)",
            "most_recent_action": "Most recent",
            "total_assessed":     "Assessed",
            "total_paid":         "Paid",
            "outstanding_balance":"Outstanding",
        }),
        hide_index=True,
        use_container_width=True,
    )

    st.divider()

    fig2 = px.bar(
        priority.sort_values("outstanding_balance", ascending=True),
        x="outstanding_balance",
        y="facility_name",
        orientation="h",
        title="Outstanding balance by facility (top 25)",
        labels={"outstanding_balance": "Outstanding ($)", "facility_name": ""},
        color_discrete_sequence=["#dc2626"],
    )
    fig2.update_xaxes(tickprefix="$", tickformat=",.0f")
    fig2.update_layout(height=620, margin=dict(t=40, b=20, l=240, r=20))
    st.plotly_chart(fig2, use_container_width=True)

# -- TAB 3: BY REGION -----------------------------------------------------------

with tab_region:

    st.subheader("Enforcement by SWRCB region")

    # Apply consolidated chart labels BEFORE groupby so sub-offices
    # (5S, 5F, 5R) roll up into "5 - Central Valley" as a single bar.
    mart_chart = mart[mart["region"].notna() & (mart["region"] != "NA")].copy()
    mart_chart = chart_region(mart_chart)

    by_region = (
        mart_chart
        .groupby("region")
        .agg(
            facilities  = ("wdid",             "nunique"),
            actions     = ("enforcement_count", "sum"),
            outstanding = ("total_outstanding", "sum"),
        )
        .reset_index()
        .sort_values("actions", ascending=False)
    )

    # Sub-office breakdown for the summary table (5S/5F/5R shown separately)
    mart_table = mart[mart["region"].notna() & (mart["region"] != "NA")].copy()
    mart_table = label_region(mart_table)
    by_region_table = (
        mart_table
        .groupby("region")
        .agg(
            facilities  = ("wdid",             "nunique"),
            actions     = ("enforcement_count", "sum"),
            outstanding = ("total_outstanding", "sum"),
        )
        .reset_index()
        .sort_values("actions", ascending=False)
    )

    c1, c2 = st.columns(2)

    with c1:
        fig3 = px.bar(
            by_region, x="region", y="actions",
            title="Total enforcement actions by region",
            labels={"region": "Region", "actions": "Actions"},
            color_discrete_sequence=["#1d4ed8"],
        )
        fig3.update_layout(height=340, margin=dict(t=40, b=40))
        st.plotly_chart(fig3, use_container_width=True)

    with c2:
        fig4 = px.bar(
            by_region, x="region", y="outstanding",
            title="Outstanding balance by region ($)",
            labels={"region": "Region", "outstanding": "Outstanding ($)"},
            color_discrete_sequence=["#dc2626"],
        )
        fig4.update_yaxes(tickprefix="$", tickformat=",.0f")
        fig4.update_layout(height=340, margin=dict(t=40, b=40))
        st.plotly_chart(fig4, use_container_width=True)

    st.divider()
    st.caption("Table shows sub-office breakdown for Regions 5 and 6.")
    summary = by_region_table.copy()
    summary["outstanding"] = summary["outstanding"].apply(lambda x: f"${x:,.0f}")
    summary.columns = ["Region", "Facilities", "Total actions", "Outstanding balance"]
    st.dataframe(summary, hide_index=True, use_container_width=True)

st.divider()
st.caption("Data: CA State Water Resources Control Board (CIWQS) - Pipeline: Python + DuckDB - Dashboard: Streamlit + Plotly")
