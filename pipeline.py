from __future__ import annotations

import argparse
import csv
import hashlib
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

import duckdb

# -----------------------------
# Paths / Config
# -----------------------------

ROOT = Path(__file__).resolve().parent

DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
MARTS_DIR = DATA_DIR / "marts"
OUTPUTS_DIR = DATA_DIR / "outputs"
DB_PATH = DATA_DIR / "warehouse.duckdb"

SQL_DIR = ROOT / "src" / "sql"

DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; takehome-pipeline/1.0)"


@dataclass(frozen=True)
class DownloadSpec:
    filename: str
    url: str


# NOTE: These URLs can change. If a download fails, open the dataset landing page and copy the latest "Download CSV" links.
DOWNLOADS: Tuple[DownloadSpec, ...] = (
    DownloadSpec(
        filename="reg_meas_export_wastewaterpermitsorders_2026-03-10.csv",
        # Example resource page often associated with Permits & Orders; you may need to replace with current 'download' link
        url="https://data.ca.gov/dataset/449407ec-22a5-412b-b934-2cc01481bb06/resource/2446e10e-8682-4d7a-952e-07ffe20d4950/download/reg_meas_export_wastewaterpermitsorders_2026-03-10.csv",
    ),
    DownloadSpec(
        filename="wastewater-enforcement-actions.csv",
        # Sometimes the download filename differs; replace with the current 'download' link if needed
        url="https://data.ca.gov/dataset/449407ec-22a5-412b-b934-2cc01481bb06/resource/05295f0e-70ab-410e-81b5-7adbdac3b314/download/wastewater-enforcement-actions.csv",
    ),
)


# -----------------------------
# Utilities
# -----------------------------


def ensure_dirs() -> None:
    for p in (DATA_DIR, RAW_DIR, MARTS_DIR, OUTPUTS_DIR, SQL_DIR):
        p.mkdir(parents=True, exist_ok=True)


def connect_db() -> duckdb.DuckDBPyConnection:
    ensure_dirs()
    return duckdb.connect(str(DB_PATH))


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def download_file(url: str, out_path: Path, *, user_agent: str = DEFAULT_USER_AGENT) -> None:
    headers = {"User-Agent": user_agent}
    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req) as resp:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(resp.read())
    except Exception as e:
        raise RuntimeError(
            f"Download failed.\n"
            f"File: {out_path.name}\n"
            f"URL:  {url}\n\n"
            f"Fix:\n"
            f"  1) Open the dataset landing page in your browser.\n"
            f"  2) Click the exact resource (Permits & Orders / Enforcement Actions).\n"
            f"  3) Copy the latest 'Download CSV' link.\n"
            f"  4) Replace the URL in DOWNLOADS in pipeline.py.\n\n"
            f"Error: {e}"
        )


def run_sql_file(con: duckdb.DuckDBPyConnection, sql_path: Path) -> None:
    sql = sql_path.read_text(encoding="utf-8")
    con.execute(sql)


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    q = """
    SELECT COUNT(*)::INT AS n
    FROM information_schema.tables
    WHERE table_schema = 'main' AND table_name = ?
    """
    return con.execute(q, [table_name]).fetchone()[0] > 0


def export_table(con: duckdb.DuckDBPyConnection, table_name: str, out_base: Path) -> None:
    """
    Exports a DuckDB table to both CSV and Parquet.
    out_base is a path without extension, e.g. data/marts/mart_facility_monthly
    """
    out_base.parent.mkdir(parents=True, exist_ok=True)
    csv_path = out_base.with_suffix(".csv")
    pq_path = out_base.with_suffix(".parquet")

    con.execute(f"COPY {table_name} TO '{csv_path.as_posix()}' (HEADER, DELIMITER ',');")
    con.execute(f"COPY {table_name} TO '{pq_path.as_posix()}' (FORMAT PARQUET);")


def export_query(con: duckdb.DuckDBPyConnection, query: str, out_csv: Path) -> None:
    """
    Exports the results of an arbitrary SELECT query to CSV.
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    # DuckDB allows COPY (SELECT ...) TO ...
    con.execute(f"COPY ({query}) TO '{out_csv.as_posix()}' (HEADER, DELIMITER ',');")


def print_rowcount(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    n = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
    print(f"[count] {table_name}: {n}")


# -----------------------------
# Pipeline Steps
# -----------------------------


def step_extract(force: bool = False) -> None:
    ensure_dirs()
    for spec in DOWNLOADS:
        out_path = RAW_DIR / spec.filename
        if out_path.exists() and out_path.stat().st_size > 0 and not force:
            print(f"[extract] exists, skipping: {out_path.name} (sha256={sha256_file(out_path)[:10]}...)")
            continue

        if out_path.exists() and force:
            out_path.unlink(missing_ok=True)

        print(f"[extract] downloading: {out_path.name}")
        download_file(spec.url, out_path)
        print(f"[extract] done: {out_path.name} (sha256={sha256_file(out_path)[:10]}...)")

    print("[extract] complete")


def step_load() -> None:
    ensure_dirs()
    con = connect_db()
    try:
        # Raw table names (keep stable)
        raw_permits = "raw_permits_orders"
        raw_enforcement = "raw_enforcement_actions"

        permits_path = (RAW_DIR / "reg_meas_export_wastewaterpermitsorders_2026-03-10.csv")
        enforcement_path = (RAW_DIR / "wastewater-enforcement-actions.csv")

        if not permits_path.exists() or permits_path.stat().st_size == 0:
            raise RuntimeError(f"Missing {permits_path}. Run: python pipeline.py extract")
        if not enforcement_path.exists() or enforcement_path.stat().st_size == 0:
            raise RuntimeError(f"Missing {enforcement_path}. Run: python pipeline.py extract")

        print(f"[load] creating {raw_permits} from {permits_path.name}")
        con.execute(f"""
            CREATE OR REPLACE TABLE {raw_permits} AS
            SELECT * FROM read_csv_auto('{permits_path.as_posix()}', ALL_VARCHAR=TRUE);
        """)

        print(f"[load] creating {raw_enforcement} from {enforcement_path.name}")
        con.execute(f"""
            CREATE OR REPLACE TABLE {raw_enforcement} AS
            SELECT * FROM read_csv_auto('{enforcement_path.as_posix()}', ALL_VARCHAR=TRUE, ignore_errors=true,
        strict_mode=false);
        """)

        print_rowcount(con, raw_permits)
        print_rowcount(con, raw_enforcement)

        # Persist a quick schema snapshot for debugging/review
        schema_out = OUTPUTS_DIR / "raw_schema.txt"
        with open(schema_out, "w", encoding="utf-8") as f:
            for t in (raw_permits, raw_enforcement):
                f.write(f"== {t} ==\n")
                cols = con.execute(f"DESCRIBE {t};").fetchall()
                for c in cols:
                    f.write(f"{c[0]}\t{c[1]}\n")
                f.write("\n")
        print(f"[load] wrote schema snapshot: {schema_out}")

        print("[load] complete")
    finally:
        con.close()


def step_profile_keys() -> None:
    """
    Produces a lightweight join-key discovery report.
    You will likely adjust candidate keys after inspecting raw columns.
    """
    con = connect_db()
    try:
        permits = "raw_permits_orders"
        enforcement = "raw_enforcement_actions"
        if not table_exists(con, permits) or not table_exists(con, enforcement):
            raise RuntimeError("Raw tables not found. Run: python pipeline.py extract && python pipeline.py load")

        # You MUST inspect OUTPUTS_DIR/raw_schema.txt and update these candidates to real column names.
        candidate_keys = [
            # Common facility identifiers (examples)
            "wdid",
            "facility_id",
            "place_id",
            "ciwqs_place_id",
            "reg_measure_id",
        ]

        report_path = OUTPUTS_DIR / "join_report.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("Join Key Discovery Report\n")
            f.write("=========================\n\n")
            f.write("NOTE: Update candidate_keys in pipeline.py to match actual raw column names.\n\n")

            for key in candidate_keys:
                # Does the column exist?
                col_check_q = """
                SELECT
                  SUM(CASE WHEN column_name = ? THEN 1 ELSE 0 END) AS exists_in_permits,
                  (SELECT SUM(CASE WHEN column_name = ? THEN 1 ELSE 0 END)
                   FROM information_schema.columns
                   WHERE table_name = ?) AS exists_in_enforcement
                FROM information_schema.columns
                WHERE table_name = ?
                """
                exists_permits, exists_enf = con.execute(
                    col_check_q, [key, key, enforcement, permits]
                ).fetchone()

                if exists_permits == 0 or exists_enf == 0:
                    continue

                # Compute distinctness
                permits_stats = con.execute(
                    f"SELECT COUNT(*) AS n, COUNT(DISTINCT {key}) AS d FROM {permits} WHERE {key} IS NOT NULL AND {key} <> '';"
                ).fetchone()
                enf_stats = con.execute(
                    f"SELECT COUNT(*) AS n, COUNT(DISTINCT {key}) AS d FROM {enforcement} WHERE {key} IS NOT NULL AND {key} <> '';"
                ).fetchone()

                # Join match rate
                join_stats = con.execute(
                    f"""
                    SELECT
                      COUNT(*) AS total_enf,
                      SUM(CASE WHEN p.{key} IS NOT NULL THEN 1 ELSE 0 END) AS matched
                    FROM {enforcement} e
                    LEFT JOIN (SELECT DISTINCT {key} FROM {permits}) p
                      ON e.{key} = p.{key}
                    WHERE e.{key} IS NOT NULL AND e.{key} <> '';
                    """
                ).fetchone()

                total_enf, matched = join_stats
                match_rate = (matched / total_enf) if total_enf else 0.0

                f.write(f"Key: {key}\n")
                f.write(f"  permits: n={permits_stats[0]}, distinct={permits_stats[1]}\n")
                f.write(f"  enforce: n={enf_stats[0]}, distinct={enf_stats[1]}\n")
                f.write(f"  join match: matched={matched} / total_enf={total_enf} ({match_rate:.2%})\n\n")

        print(f"[profile] wrote: {report_path}")
        print("[profile] TIP: open data/outputs/raw_schema.txt to find the real key column names.")
    finally:
        con.close()


def step_model() -> None:
    con = connect_db()
    try:
        sql_path = SQL_DIR / "01_model.sql"
        if not sql_path.exists():
            raise RuntimeError(f"Missing SQL file: {sql_path}")
        print(f"[model] running {sql_path}")
        run_sql_file(con, sql_path)
        print("[model] complete")
    finally:
        con.close()


def step_mart() -> None:
    con = connect_db()
    try:
        sql_path = SQL_DIR / "02_mart.sql"
        if not sql_path.exists():
            raise RuntimeError(f"Missing SQL file: {sql_path}")
        print(f"[mart] running {sql_path}")
        run_sql_file(con, sql_path)

        # If your 02_mart.sql creates a table named 'mart_facility_monthly', export it.
        mart_table = "mart_facility_monthly"
        if table_exists(con, mart_table):
            export_table(con, mart_table, MARTS_DIR / mart_table)
            print(f"[mart] exported {mart_table} to {MARTS_DIR}")
        else:
            print(f"[mart] NOTE: {mart_table} not found. Update mart_table name or export in SQL.")
        print("[mart] complete")
    finally:
        con.close()


def step_answer() -> None:
    con = connect_db()
    try:
        sql_path = SQL_DIR / "03_answer.sql"
        if not sql_path.exists():
            raise RuntimeError(f"Missing SQL file: {sql_path}")
        print(f"[answer] running {sql_path}")
        run_sql_file(con, sql_path)

        # Convention: 03_answer.sql creates a table/view 'top_25_facilities'
        answer_table = "top_25_facilities"
        if table_exists(con, answer_table):
            out_csv = OUTPUTS_DIR / f"{answer_table}.csv"
            con.execute(f"COPY {answer_table} TO '{out_csv.as_posix()}' (HEADER, DELIMITER ',');")
            print(f"[answer] exported {answer_table} -> {out_csv}")
        else:
            print(f"[answer] NOTE: {answer_table} not found. Either create it in 03_answer.sql or export via export_query().")

        print("[answer] complete")
    finally:
        con.close()


def step_run_all(force_download: bool = False) -> None:
    ensure_dirs()
    step_extract(force=force_download)
    step_load()
    step_profile_keys()
    # The next steps depend on you writing the SQL files after inspecting schema/join keys.
    if (SQL_DIR / "01_model.sql").exists():
        step_model()
    else:
        print("[run] skipping model: src/sql/01_model.sql not found yet")
    if (SQL_DIR / "02_mart.sql").exists():
        step_mart()
    else:
        print("[run] skipping mart: src/sql/02_mart.sql not found yet")
    if (SQL_DIR / "03_answer.sql").exists():
        step_answer()
    else:
        print("[run] skipping answer: src/sql/03_answer.sql not found yet")
    print("[done] pipeline complete")


# -----------------------------
# CLI
# -----------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Option B take-home pipeline (DuckDB).")
    parser.add_argument(
        "command",
        choices=["extract", "load", "profile", "model", "mart", "answer", "run"],
        help="Pipeline step to run",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download raw CSVs even if they already exist",
    )
    args = parser.parse_args()

    if args.command == "extract":
        step_extract(force=args.force_download)
    elif args.command == "load":
        step_load()
    elif args.command == "profile":
        step_profile_keys()
    elif args.command == "model":
        step_model()
    elif args.command == "mart":
        step_mart()
    elif args.command == "answer":
        step_answer()
    elif args.command == "run":
        step_run_all(force_download=args.force_download)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)