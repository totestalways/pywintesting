#!/usr/bin/env python3
"""
Compare two Oracle tables (from two different DBs) with datacompy,
print a report, save the report to disk, and export both tables to CSV.

Requires:
  - pandas
  - datacompy
  - sqlalchemy
  - oracledb (preferred) or cx_Oracle

Note: For very large tables, consider filtering (WHERE) or sampling since
datacompy compares in-memory DataFrames.
"""

import os
import sys
import datetime
from pathlib import Path

import pandas as pd

# Prefer the modern 'oracledb' driver; fall back to cx_Oracle if needed.
try:
    import oracledb  # noqa: F401
    ORA_DIALECT = "oracle+oracledb"
except Exception:
    try:
        import cx_Oracle  # noqa: F401
        ORA_DIALECT = "oracle+cx_oracle"
    except Exception:
        print("ERROR: Install 'oracledb' (preferred) or 'cx_Oracle'.", file=sys.stderr)
        sys.exit(1)

from sqlalchemy import create_engine, text
import datacompy

# -----------------------------
# >>>>>>> USER INPUTS <<<<<<<
# -----------------------------

CONFIG = {
    "db1": {
        "user": "USER1",
        "password": "PASS1",
        "host": "db1.host.example.com",
        "port": 1521,
        "service_name": "ORCLPDB1",
    },
    "db2": {
        "user": "USER2",
        "password": "PASS2",
        "host": "db2.host.example.com",
        "port": 1521,
        "service_name": "ORCLPDB2",
    },
    # Fully qualified table names (SCHEMA.TABLE). You can add a column list if desired.
    "table1": {
        "fqtn": "SCHEMA_A.MY_TABLE_A",
        # Optional: list of columns to select; empty means '*'
        "columns": [],  # e.g., ["ID", "NAME", "AMOUNT", "UPDATED_AT"]
        # Optional: WHERE clause (without the 'WHERE' keyword)
        "where": "",    # e.g., "UPDATED_AT >= SYSDATE - 1"
        # Optional: Limit rows for sanity (Oracle 12c+ syntax)
        "limit": None,  # e.g., 10000
    },
    "table2": {
        "fqtn": "SCHEMA_B.MY_TABLE_B",
        "columns": [],
        "where": "",
        "limit": None,
    },
    # Comparison settings
    # Use either:
    #  - a list of join columns with identical names in both tables: ["ID"]
    #  - a mapping dict if the key columns differ: {"ID_A": "ID_B", "DATE_A": "DATE_B"}
    "join_columns": ["ID"],

    # Optional: treat column names uniformly
    "cast_column_names_lower": True,   # datacompy option too (keeps report simpler)
    "ignore_case": False,
    "ignore_spaces": False,

    # Tolerances for numeric comparisons
    "abs_tol": 0,
    "rel_tol": 0,

    # Output folder (timestamped sub-folder gets created)
    "output_dir": "./oracle_compare_output",

    # CSV export options
    "csv_index": False,
    "csv_na_rep": "",  # how to represent NaN in CSV
}

# -----------------------------
# Helpers
# -----------------------------

def oracle_url(user, password, host, port, service_name) -> str:
    # SQLAlchemy URL for oracledb/cx_Oracle
    # Service-name style:
    return f"{ORA_DIALECT}://{user}:{password}@{host}:{port}/?service_name={service_name}"

def build_select(fqtn: str, columns=None, where: str = "", limit=None) -> str:
    if columns:
        sel = ", ".join(columns)
    else:
        sel = "*"

    base = f"SELECT {sel} FROM {fqtn}"
    if where and where.strip():
        base += f" WHERE {where.strip()}"

    # Oracle 12c+ supports FETCH FIRST N ROWS ONLY
    if limit is not None:
        base += f" FETCH FIRST {int(limit)} ROWS ONLY"
    return base

def ensure_output_dir(base_dir: str) -> Path:
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = Path(base_dir).expanduser().resolve() / f"run_{ts}"
    out.mkdir(parents=True, exist_ok=True)
    return out

# -----------------------------
# Main
# -----------------------------

def main():
    out_dir = ensure_output_dir(CONFIG["output_dir"])
    print(f"Output directory: {out_dir}")

    # Create engines
    url1 = oracle_url(**CONFIG["db1"])
    url2 = oracle_url(**CONFIG["db2"])

    engine1 = create_engine(url1)
    engine2 = create_engine(url2)

    # Build queries
    q1 = build_select(
        CONFIG["table1"]["fqtn"],
        CONFIG["table1"]["columns"],
        CONFIG["table1"]["where"],
        CONFIG["table1"]["limit"],
    )
    q2 = build_select(
        CONFIG["table2"]["fqtn"],
        CONFIG["table2"]["columns"],
        CONFIG["table2"]["where"],
        CONFIG["table2"]["limit"],
    )

    print("\nQuery DB1:")
    print(q1)
    print("\nQuery DB2:")
    print(q2)

    # Read DataFrames
    with engine1.connect() as conn1, engine2.connect() as conn2:
        df1 = pd.read_sql(text(q1), conn1)
        df2 = pd.read_sql(text(q2), conn2)

    if CONFIG["cast_column_names_lower"]:
        df1.columns = [c.lower() for c in df1.columns]
        df2.columns = [c.lower() for c in df2.columns]

        # If join_columns provided as mapping, normalize keys to lower as well
        join_cols = CONFIG["join_columns"]
        if isinstance(join_cols, dict):
            join_cols = {k.lower(): v.lower() for k, v in join_cols.items()}
        elif isinstance(join_cols, list):
            join_cols = [c.lower() for c in join_cols]
    else:
        join_cols = CONFIG["join_columns"]

    # Save CSVs
    csv1 = out_dir / "table1.csv"
    csv2 = out_dir / "table2.csv"
    df1.to_csv(csv1, index=CONFIG["csv_index"], na_rep=CONFIG["csv_na_rep"])
    df2.to_csv(csv2, index=CONFIG["csv_index"], na_rep=CONFIG["csv_na_rep"])
    print(f"\nSaved CSVs:\n  {csv1}\n  {csv2}")

    # Compare with datacompy
    compare = datacompy.Compare(
        df1=df1,
        df2=df2,
        join_columns=join_cols,                 # list or dict supported
        df1_name="DB1_TABLE",
        df2_name="DB2_TABLE",
        ignore_spaces=CONFIG["ignore_spaces"],
        ignore_case=CONFIG["ignore_case"],
        cast_column_names_lower=CONFIG["cast_column_names_lower"],
        abs_tol=CONFIG["abs_tol"],
        rel_tol=CONFIG["rel_tol"],
    )

    # Print quick summary to stdout
    print("\n==== datacompy Summary ====")
    print(f"Match? {compare.matches()}")
    print(f"Rows only in DB1_TABLE: {compare.df1_unq_rows.shape[0]}")
    print(f"Rows only in DB2_TABLE: {compare.df2_unq_rows.shape[0]}")
    print(f"Columns compared: {sorted(list(compare.intersect_columns()))}")

    # Full report
    report_text = compare.report()
    print("\n==== Full Report ====\n")
    print(report_text)

    # Save report to files
    rpt_txt = out_dir / "datacompy_report.txt"
    rpt_html = out_dir / "datacompy_report.html"
    with open(rpt_txt, "w", encoding="utf-8") as f:
        f.write(report_text)
    # Simple HTML wrapper
    with open(rpt_html, "w", encoding="utf-8") as f:
        f.write("<!doctype html><meta charset='utf-8'><title>datacompy report</title><pre>")
        f.write(pd.io.common.stringify_path(report_text))
        f.write("</pre>")

    print(f"\nSaved reports:\n  {rpt_txt}\n  {rpt_html}")

    # Optionally also dump unique rows and mismatches for offline inspection
    df1_only_csv = out_dir / "rows_only_in_db1.csv"
    df2_only_csv = out_dir / "rows_only_in_db2.csv"
    df_mismatch_csv = out_dir / "mismatched_rows.csv"

    compare.df1_unq_rows.to_csv(df1_only_csv, index=False)
    compare.df2_unq_rows.to_csv(df2_only_csv, index=False)
    # Mismatched rows (joined on keys) if any:
    try:
        mismatches = compare.all_mismatch()
        if not mismatches.empty:
            mismatches.to_csv(df_mismatch_csv, index=False)
            print(f"\nSaved mismatches: {df_mismatch_csv}")
    except Exception:
        # Older datacompy versions may not have all_mismatch(); ignore gracefully.
        pass

    print("\nDone.")

if __name__ == "__main__":
    # Fail fast if output dir not writable
    out_parent = Path(CONFIG["output_dir"]).expanduser()
    if not os.access(out_parent.parent if out_parent.is_file() else out_parent, os.W_OK):
        print(f"ERROR: Output directory not writable: {out_parent}", file=sys.stderr)
        sys.exit(2)
    main()
