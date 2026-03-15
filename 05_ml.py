"""
05_ml.py
Trains a gradient boosted classifier to predict which facilities are likely
to receive a monetary enforcement action (ACL / MMP / Cleanup & Abatement Order).

Features are engineered from mart_facility_monthly aggregates.
Model is evaluated with a temporal train/test split (train on pre-2016, test on 2016+)
to avoid data leakage — we never let future enforcement inform past predictions.

Outputs:
  - Prints classification report + AUC to console
  - Writes ml_facility_scores table to warehouse.duckdb
  - Exports data/marts/ml_facility_scores.csv
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    average_precision_score,
)
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

ROOT     = Path(__file__).resolve().parent
DB_PATH  = ROOT / "data" / "warehouse.duckdb"
MART_DIR = ROOT / "data" / "marts"
MART_DIR.mkdir(parents=True, exist_ok=True)

TRAIN_CUTOFF = 2016   # train on actions before this year, test on >= this year
RANDOM_STATE = 42


def load_features(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Build a facility-level feature matrix from mart_facility_monthly.

    Target: did this facility receive at least one monetary action
    in the FOLLOWING 12 months? (forward-looking label)

    Features: trailing 24-month aggregates up to the label window.
    """
    print("[ml] Engineering features from mart_facility_monthly...")

    # Pull raw mart
    df = con.execute("SELECT * FROM mart_facility_monthly").df()
    df["month"] = pd.to_datetime(df["month"])
    df = df.sort_values(["wdid", "month"])

    records = []

    # For each facility, for each year, compute trailing features
    # and look ahead 12 months for the target label
    for wdid, grp in df.groupby("wdid"):
        grp = grp.set_index("month").sort_index()

        # Snapshot years: 2000 to 2017 (need 12mo lookback + 12mo lookahead)
        for year in range(2000, 2018):
            snapshot = pd.Timestamp(f"{year}-01-01")
            lookback_start = snapshot - pd.DateOffset(months=24)
            lookahead_end  = snapshot + pd.DateOffset(months=12)

            trailing = grp.loc[lookback_start:snapshot]
            ahead    = grp.loc[snapshot:lookahead_end]

            if len(trailing) < 3:   # need some history
                continue

            # ── Features ──────────────────────────────────────────────────
            feat = {
                "wdid":    wdid,
                "year":    year,
                "facility_name":  grp["facility_name"].iloc[0],
                "region":         grp["region"].iloc[0],
                "county":         grp["county"].iloc[0] if "county" in grp.columns else None,
                "place_type":     grp["place_type"].iloc[0],

                # Volume
                "total_actions_24mo":    trailing["enforcement_count"].sum(),
                "total_actions_12mo":    trailing.last("12ME")["enforcement_count"].sum()
                                         if len(trailing.last("12ME")) else 0,
                "total_actions_3mo":     trailing.last("3ME")["enforcement_count"].sum()
                                         if len(trailing.last("3ME")) else 0,

                # Severity
                "avg_severity_24mo":     trailing["avg_severity_rank"].mean(),
                "max_severity_24mo":     trailing["max_severity_rank"].max(),
                "avg_severity_12mo":     trailing.last("12ME")["avg_severity_rank"].mean()
                                         if len(trailing.last("12ME")) else 0,

                # Money
                "total_outstanding":     trailing["total_outstanding"].sum(),
                "total_assessment_24mo": trailing["total_assessment"].sum(),
                "monetary_count_24mo":   trailing["monetary_count"].sum(),
                "monetary_rate_24mo":    (trailing["monetary_count"].sum() /
                                          max(trailing["enforcement_count"].sum(), 1)),

                # Active orders
                "active_count_latest":   trailing["active_count"].iloc[-1]
                                         if len(trailing) else 0,

                # Recency (months since last action)
                "months_since_last":     (snapshot - grp.index[grp["enforcement_count"] > 0].max()).days / 30
                                         if len(grp[grp["enforcement_count"] > 0]) else 99,

                # Escalation: last 6mo avg vs prior 18mo avg
                "escalation_ratio":      (
                    trailing.last("6ME")["enforcement_count"].mean() /
                    max(trailing.iloc[:-6]["enforcement_count"].mean(), 0.01)
                ) if len(trailing) > 6 else 1.0,
            }

            # ── Target ────────────────────────────────────────────────────
            feat["target_monetary_12mo"] = int(
                ahead["monetary_count"].sum() > 0
            )

            records.append(feat)

    features_df = pd.DataFrame(records).fillna(0)
    print(f"[ml] Feature matrix: {len(features_df):,} rows × {len(features_df.columns)} columns")
    print(f"[ml] Target positive rate: {features_df['target_monetary_12mo'].mean():.1%}")
    return features_df


FEATURE_COLS = [
    "total_actions_24mo", "total_actions_12mo", "total_actions_3mo",
    "avg_severity_24mo", "max_severity_24mo", "avg_severity_12mo",
    "total_outstanding", "total_assessment_24mo",
    "monetary_count_24mo", "monetary_rate_24mo",
    "active_count_latest", "months_since_last", "escalation_ratio",
]


def train_and_evaluate(features_df: pd.DataFrame) -> tuple[Pipeline, pd.DataFrame]:
    """Temporal train/test split — train on pre-2016, test on 2016+."""

    train = features_df[features_df["year"] <  TRAIN_CUTOFF]
    test  = features_df[features_df["year"] >= TRAIN_CUTOFF]

    X_train = train[FEATURE_COLS]
    y_train = train["target_monetary_12mo"]
    X_test  = test[FEATURE_COLS]
    y_test  = test["target_monetary_12mo"]

    print(f"\n[ml] Train: {len(train):,} rows ({y_train.mean():.1%} positive)")
    print(f"[ml] Test:  {len(test):,} rows  ({y_test.mean():.1%} positive)")

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", GradientBoostingClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            min_samples_leaf=20,
            random_state=RANDOM_STATE,
        )),
    ])

    print("\n[ml] Training gradient boosted classifier...")
    model.fit(X_train, y_train)

    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = (y_prob >= 0.5).astype(int)

    auc_roc = roc_auc_score(y_test, y_prob)
    auc_pr  = average_precision_score(y_test, y_prob)

    print(f"\n[ml] === Model Evaluation (test set: {TRAIN_CUTOFF}+) ===")
    print(f"     ROC-AUC:  {auc_roc:.3f}")
    print(f"     PR-AUC:   {auc_pr:.3f}  (baseline = {y_test.mean():.3f})")
    print("\n" + classification_report(y_test, y_pred,
          target_names=["No monetary action", "Monetary action"]))

    # Feature importance
    clf = model.named_steps["clf"]
    importances = pd.DataFrame({
        "feature":    FEATURE_COLS,
        "importance": clf.feature_importances_,
    }).sort_values("importance", ascending=False)
    print("[ml] Top feature importances:")
    print(importances.to_string(index=False))

    return model, features_df


def score_all_facilities(
    model: Pipeline,
    features_df: pd.DataFrame,
    con: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """
    Score every facility at its most recent snapshot year.
    Join with top_25_facilities to add ml_risk_score alongside heuristic score.
    """
    print("\n[ml] Scoring all facilities at latest snapshot...")

    # Latest snapshot per facility
    latest = (features_df
              .sort_values("year")
              .groupby("wdid")
              .last()
              .reset_index())

    X = latest[FEATURE_COLS].fillna(0)
    latest["ml_risk_score"] = model.predict_proba(X)[:, 1]
    latest["ml_risk_pct"]   = (latest["ml_risk_score"] * 100).round(1)

    # Percentile rank
    latest["ml_percentile"] = (
        latest["ml_risk_score"].rank(pct=True) * 100
    ).round(1)

    out = latest[[
        "wdid", "facility_name", "region", "county", "place_type", "year",
        "ml_risk_score", "ml_risk_pct", "ml_percentile",
        "total_actions_24mo", "avg_severity_24mo", "total_outstanding",
        "monetary_rate_24mo", "escalation_ratio",
    ]].sort_values("ml_risk_score", ascending=False)

    # Write back to DuckDB
    con.execute("CREATE OR REPLACE TABLE ml_facility_scores AS SELECT * FROM out")

    # Join with heuristic top-25 if it exists
    if con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = 'top_25_facilities'
    """).fetchone()[0] > 0:
        combined = con.execute("""
            SELECT
                t.priority_rank,
                t.priority_score_pct   AS heuristic_score,
                m.ml_risk_pct          AS ml_score,
                t.wdid,
                t.facility_name,
                t.county,
                t.region,
                t.place_type,
                t.why_prioritized,
                t.actions_12mo,
                t.avg_severity_12mo,
                t.outstanding_penalties,
                t.current_active_actions
            FROM top_25_facilities t
            LEFT JOIN ml_facility_scores m ON t.wdid = m.wdid
            ORDER BY t.priority_rank
        """).df()
        combined.to_csv(MART_DIR / "top25_with_ml_scores.csv", index=False)
        print(f"[ml] Exported top25_with_ml_scores.csv")

    return out


def main() -> None:
    if not DB_PATH.exists():
        raise RuntimeError(
            f"Database not found: {DB_PATH}\n"
            "Run: python pipeline.py run   (to build the full pipeline first)"
        )

    con = duckdb.connect(str(DB_PATH))
    try:
        features_df         = load_features(con)
        model, features_df  = train_and_evaluate(features_df)
        scores              = score_all_facilities(model, features_df, con)

        scores.to_csv(MART_DIR / "ml_facility_scores.csv", index=False)
        print(f"\n[ml] Wrote ml_facility_scores.csv ({len(scores):,} facilities)")
        print("[ml] Complete. Run: python -m streamlit run app.py")
    finally:
        con.close()


if __name__ == "__main__":
    main()
