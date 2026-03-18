from __future__ import annotations

import argparse
import hashlib
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import duckdb

# -- Paths ----------------------------------------------------------------------

ROOT       = Path(__file__).resolve().parent
DATA_DIR   = ROOT / "data"
RAW_DIR    = DATA_DIR / "raw"
MARTS_DIR  = DATA_DIR / "marts"
OUTPUTS_DIR = DATA_DIR / "outputs"
DB_PATH    = DATA_DIR / "warehouse.duckdb"
SQL_DIR    = ROOT / "src" / "sql"

DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; takehome-pipeline/1.0)"

# -- Download specs -------------------------------------------------------------

@dataclass(frozen=True)
class DownloadSpec:
    filename: str
    url: str

DOWNLOADS: Tuple[DownloadSpec, ...] = (
    DownloadSpec(
        filename="reg_meas_export_wastewaterpermitsorders.csv",
        url="https://data.ca.gov/dataset/449407ec-22a5-412b-b934-2cc01481bb06/resource/2446e10e-8682-4d7a-952e-07ffe20d4950/download/reg_meas_export_wastewaterpermitsorders_2026-03-10.csv",
    ),
    DownloadSpec(
        filename="wastewater-enforcement-actions.csv",
        url="https://data.ca.gov/dataset/b2bda258-6433-49a0-958a-09b06c926f76/resource/64f25cad-2e10-4a66-8368-79293f56c2f1/download/wastewater-enforcement-actions_2026-02-24.csv",
    ),
)

# -- Utilities ------------------------------------------------------------------

def ensure_dirs() -> None:
    for p in (DATA_DIR, RAW_DIR, MARTS_DIR, OUTPUTS_DIR, SQL_DIR):
        p.mkdir(parents=True, exist_ok=True)


def connect_db() -> duckdb.DuckDBPyConnection:
    ensure_dirs()
    return duckdb.connect(str(DB_PATH))


def sha256_file(path: Path, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def download_file(url: str, out_path: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
    try:
        with urllib.request.urlopen(req) as resp:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(resp.read())
    except Exception as e:
        raise RuntimeError(
            f"Download failed for {out_path.name}.\n"
            f"URL: {url}\n"
            f"If the URL has expired, open the dataset landing page on data.ca.gov, "
            f"copy the current Download CSV link, and update DOWNLOADS in pipeline.py.\n"
            f"Error: {e}"
        )


def run_sql_file(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    con.execute(path.read_text(encoding="utf-8"))


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    n = con.execute(
        "SELECT COUNT(*)::INT FROM information_schema.tables "
        "WHERE table_schema='main' AND table_name=?", [name]
    ).fetchone()[0]
    return n > 0


def rowcount(con: duckdb.DuckDBPyConnection, name: str) -> None:
    n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    print(f"  {name}: {n:,} rows")


def export_table(con: duckdb.DuckDBPyConnection, table: str, base: Path) -> None:
    base.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY {table} TO '{base.with_suffix('.csv').as_posix()}' (HEADER, DELIMITER ',')")

# -- Pipeline steps -------------------------------------------------------------

def step_extract(force: bool = False) -> None:
    ensure_dirs()
    for spec in DOWNLOADS:
        path = RAW_DIR / spec.filename
        if path.exists() and path.stat().st_size > 0 and not force:
            print(f"[extract] skip (exists): {spec.filename}  sha256={sha256_file(path)[:10]}...")
            continue
        if path.exists() and force:
            path.unlink()
        print(f"[extract] downloading: {spec.filename}")
        download_file(spec.url, path)
        print(f"[extract] done  sha256={sha256_file(path)[:10]}...")
    print("[extract] complete\n")


def step_load() -> None:
    ensure_dirs()
    con = connect_db()
    try:
        permits_path    = RAW_DIR / "reg_meas_export_wastewaterpermitsorders.csv"
        enforcement_path = RAW_DIR / "wastewater-enforcement-actions.csv"

        for p in (permits_path, enforcement_path):
            if not p.exists() or p.stat().st_size == 0:
                raise RuntimeError(f"Missing raw file: {p.name} - run: python pipeline.py extract")

        print("[load] loading raw_permits_orders ...")
        con.execute(f"""
            CREATE OR REPLACE TABLE raw_permits_orders AS
            SELECT * FROM read_csv_auto('{permits_path.as_posix()}', ALL_VARCHAR=TRUE)
        """)

        print("[load] loading raw_enforcement_actions ...")
        con.execute(f"""
            CREATE OR REPLACE TABLE raw_enforcement_actions AS
            SELECT * FROM read_csv_auto(
                '{enforcement_path.as_posix()}',
                ALL_VARCHAR=TRUE, ignore_errors=true, strict_mode=false
            )
        """)

        rowcount(con, "raw_permits_orders")
        rowcount(con, "raw_enforcement_actions")

        # Write column schema snapshot - useful during review
        schema_out = OUTPUTS_DIR / "raw_schema.txt"
        with open(schema_out, "w", encoding="utf-8") as f:
            for t in ("raw_permits_orders", "raw_enforcement_actions"):
                f.write(f"=== {t} ===\n")
                for col in con.execute(f"DESCRIBE {t}").fetchall():
                    f.write(f"  {col[0]}\t{col[1]}\n")
                f.write("\n")
        print(f"[load] schema snapshot -> {schema_out}\n")
        print("[load] complete\n")
    finally:
        con.close()


def step_profile() -> None:
    """
    Writes the actual column names from both raw tables to join_report.txt.
    WDID is confirmed as the join key by inspecting these column lists.
    Also computes the actual WDID match rate between the two tables.
    """
    con = connect_db()
    try:
        for t in ("raw_permits_orders", "raw_enforcement_actions"):
            if not table_exists(con, t):
                raise RuntimeError(f"Table {t} not found - run: python pipeline.py load")

        report = OUTPUTS_DIR / "join_report.txt"
        with open(report, "w", encoding="utf-8") as f:
            # Write actual column names from each table
            for t in ("raw_permits_orders", "raw_enforcement_actions"):
                cols = [row[0] for row in con.execute(f"DESCRIBE {t}").fetchall()]
                f.write(f"=== {t} ===\n")
                for c in cols:
                    f.write(f"  {c}\n")
                f.write("\n")

            # WDID match rate: permits uses lowercase 'wdid',
            # enforcement uses 'WDID' (upper case with spaces - already normalised in staging)
            # We check match between the two raw tables using the actual column names
            permits_wdid_col    = "wdid"
            enforcement_wdid_col = "WDID"

            p_cols = [r[0] for r in con.execute("DESCRIBE raw_permits_orders").fetchall()]
            e_cols = [r[0] for r in con.execute("DESCRIBE raw_enforcement_actions").fetchall()]

            if permits_wdid_col in p_cols and enforcement_wdid_col in e_cols:
                matched, total = con.execute(f"""
                    SELECT
                        SUM(CASE WHEN p.{permits_wdid_col} IS NOT NULL THEN 1 ELSE 0 END),
                        COUNT(*)
                    FROM raw_enforcement_actions e
                    LEFT JOIN (
                        SELECT DISTINCT UPPER(TRIM({permits_wdid_col})) AS wdid
                        FROM raw_permits_orders
                        WHERE {permits_wdid_col} IS NOT NULL AND TRIM({permits_wdid_col}) <> ''
                    ) p ON UPPER(TRIM(e."{enforcement_wdid_col}")) = p.wdid
                    WHERE e."{enforcement_wdid_col}" IS NOT NULL
                      AND TRIM(e."{enforcement_wdid_col}") <> ''
                """).fetchone()
                rate = matched / total if total else 0
                f.write(f"WDID join match: {matched:,} / {total:,}  ({rate:.1%})\n")
                f.write(f"Unmatched (enforcement rows with no permit record): {total-matched:,}\n")
            else:
                f.write("WDID column not found under expected names - check column list above\n")

        print(f"[profile] -> {report}")
        print("[profile] complete\n")
    finally:
        con.close()


def step_model() -> None:
    con = connect_db()
    try:
        sql = SQL_DIR / "01_model.sql"
        if not sql.exists():
            raise RuntimeError(f"Missing: {sql}")
        print(f"[model] running {sql.name} ...")
        run_sql_file(con, sql)
        for t in ("stg_enforcement", "stg_permits", "dim_facility", "dim_action_type", "fact_enforcement"):
            if table_exists(con, t):
                rowcount(con, t)
        print("[model] complete\n")
    finally:
        con.close()


def step_mart() -> None:
    con = connect_db()
    try:
        sql = SQL_DIR / "02_mart.sql"
        if not sql.exists():
            raise RuntimeError(f"Missing: {sql}")
        print(f"[mart] running {sql.name} ...")
        run_sql_file(con, sql)
        if table_exists(con, "mart_facility_monthly"):
            rowcount(con, "mart_facility_monthly")
            export_table(con, "mart_facility_monthly", MARTS_DIR / "mart_facility_monthly")
            print(f"[mart] exported -> {MARTS_DIR}")
        print("[mart] complete\n")
    finally:
        con.close()


def step_answer() -> None:
    con = connect_db()
    try:
        sql = SQL_DIR / "03_answer.sql"
        if not sql.exists():
            raise RuntimeError(f"Missing: {sql}")
        print(f"[answer] running {sql.name} ...")
        run_sql_file(con, sql)
        out = OUTPUTS_DIR / "priority_facilities.csv"
        con.execute(
            f"COPY (SELECT * FROM priority_facilities ORDER BY outstanding_balance DESC) "
            f"TO '{out.as_posix()}' (HEADER, DELIMITER ',')"
        )
        rowcount(con, "priority_facilities")
        print(f"[answer] exported -> {out}")
        print("[answer] complete\n")
    finally:
        con.close()


def step_run_all(force_download: bool = False) -> None:
    ensure_dirs()
    step_extract(force=force_download)
    step_load()
    step_profile()
    for name, fn in [("01_model.sql", step_model),
                     ("02_mart.sql",  step_mart),
                     ("03_answer.sql", step_answer)]:
        if (SQL_DIR / name).exists():
            fn()
        else:
            print(f"[run] skipping {name} - file not found\n")
    print("[done] pipeline complete\n")
    print("[dashboard] launching - open http://localhost:8501 in your browser")
    print("[dashboard] press Ctrl+C to stop\n")
    import subprocess
    import threading, webbrowser, time
    def _open_browser():
        time.sleep(2)
        webbrowser.open("http://localhost:8501")
    threading.Thread(target=_open_browser, daemon=True).start()
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(ROOT / "app.py"),
         "--server.headless", "true"],
        check=True,
    )

# -- CLI ------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="CA Wastewater Enforcement pipeline")
    parser.add_argument(
        "command",
        choices=["extract", "load", "profile", "model", "mart", "answer", "run"],
    )
    parser.add_argument("--force-download", action="store_true",
                        help="Re-download even if files already exist")
    args = parser.parse_args()

    dispatch = {
        "extract": lambda: step_extract(force=args.force_download),
        "load":    step_load,
        "profile": step_profile,
        "model":   step_model,
        "mart":    step_mart,
        "answer":  step_answer,
        "run":     lambda: step_run_all(force_download=args.force_download),
    }
    dispatch[args.command]()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
