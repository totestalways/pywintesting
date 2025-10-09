# hourly_counts.py
# Simple, no-args script (edit CONFIG). Uses pandas + python-oracledb.
# Creates:
#  - {SCHEMA}.{TABLE}_counts_20251009.csv           (per-table hourly counts)
#  - all_tables_hourly_counts_20251009.csv          (all tables, long form)
#  - totals_all_tables_20251009.csv                 (table, total_count)
#  - top10_tables_by_total_count_20251009.csv       (Top 10 tables)
#  - errors_20251009.csv                            (if any failures)
#
# Notes:
# - Uses bind variables for timestamps to avoid ORA-01861 literal mismatch.
# - Buckets by hour via TRUNC(CAST(time_col AS DATE), 'HH24').
# - Skips tables missing the configured time column.

import datetime as dt
from pathlib import Path
import pandas as pd
import oracledb

# ---------- CONFIG ----------
HOST = "your-db-host"
PORT = 1521
SERVICE_NAME = "your_service_name"

DB_USER = "YOUR_USER"
DB_PASSWORD = "YOUR_PASSWORD"

# Oracle schema/owner (dictionary views expect UPPERCASE)
SCHEMA = "YOUR_SCHEMA"

# Default time column used to filter & group.
# Override per-table below if needed.
TIME_COLUMN_DEFAULT = "TIME"  # e.g. CREATED_AT, EVENT_TS, INSERT_TS

# Per-table overrides: {"TABLE_NAME": "COLUMN_NAME"} all UPPERCASE
TIME_COLUMN_MAP = {
    # "ORDERS": "CREATED_AT",
    # "LOG_EVENTS": "EVENT_TS",
}

# Day to scan: start-of-day inclusive; end = next midnight (exclusive)
DAY_STR = "20251009"  # YYYYMMDD
# Output folder
OUT_DIR = Path("oracle_hourly_counts_out")
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ----------------------------


def _dsn():
    return oracledb.makedsn(HOST, PORT, service_name=SERVICE_NAME)


def _day_bounds(day_str: str):
    day = dt.datetime.strptime(day_str, "%Y%m%d")
    start = dt.datetime(day.year, day.month, day.day, 0, 0, 0, 0)
    end_excl = start + dt.timedelta(days=1)
    return start, end_excl


def _get_tables(conn, owner: str):
    sql = """
        SELECT table_name
        FROM   all_tables
        WHERE  owner = :owner
        ORDER  BY table_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=owner.upper())
        return [r[0] for r in cur.fetchall()]


def _has_time_col(conn, owner: str, table: str, col: str) -> bool:
    sql = """
        SELECT 1
        FROM   all_tab_columns
        WHERE  owner = :owner
          AND  table_name = :table_name
          AND  column_name = :col
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=owner.upper(), table_name=table.upper(), col=col.upper())
        return cur.fetchone() is not None


def _hourly_counts(conn, owner: str, table: str, time_col: str, start_ts: dt.datetime, end_ts_excl: dt.datetime):
    """
    Returns DataFrame ['hour','count'] using GROUP BY TRUNC(CAST(time_col AS DATE),'HH24').
    """
    q_owner = f'"{owner.upper()}"'
    q_table = f'"{table.upper()}"'
    # Column left unquoted (assumes normal identifier). If yours was created quoted/mixed-case, quote it here.
    sql = f"""
        SELECT TRUNC(CAST(t.{time_col} AS DATE), 'HH24') AS hour_bucket,
               COUNT(*) AS cnt
        FROM   {q_owner}.{q_table} t
        WHERE  t.{time_col} >= :start_ts
          AND  t.{time_col} <  :end_ts
        GROUP  BY TRUNC(CAST(t.{time_col} AS DATE), 'HH24')
        ORDER  BY hour_bucket
    """
    with conn.cursor() as cur:
        cur.execute(sql, start_ts=start_ts, end_ts=end_ts_excl)
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["hour", "count"])
    df = pd.DataFrame(rows, columns=["hour", "count"])
    df["hour"] = pd.to_datetime(df["hour"])
    df["count"] = df["count"].astype("int64")
    return df


def main():
    start_ts, end_ts_excl = _day_bounds(DAY_STR)
    hours = pd.date_range(start=start_ts, end=end_ts_excl, freq="H", inclusive="left")
    hours_df = pd.DataFrame({"hour": hours})

    all_rows = []     # to build an aggregated CSV: schema, table, hour, count
    totals = []       # (table, total_count)
    errors = []       # (table, error)

    print("Connecting to Oracle ...")
    conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=_dsn())
    try:
        print(f"Fetching tables from schema {SCHEMA} ...")
        tables = _get_tables(conn, SCHEMA)
        print(f"Found {len(tables)} tables.")

        for tbl in tables:
            time_col = TIME_COLUMN_MAP.get(tbl.upper(), TIME_COLUMN_DEFAULT).upper()

            if not _has_time_col(conn, SCHEMA, tbl, time_col):
                print(f"- Skipping {tbl}: missing time column {time_col}")
                continue

            print(f"- Counting per hour for {tbl} (column {time_col}) ...")
            try:
                df_counts = _hourly_counts(conn, SCHEMA, tbl, time_col, start_ts, end_ts_excl)
                # Join onto the full 24-hour frame to include zeros for missing hours
                df = hours_df.merge(df_counts, on="hour", how="left")
                df["count"] = df["count"].fillna(0).astype("int64")

                # Save per-table CSV
                per_table_csv = OUT_DIR / f"{SCHEMA.upper()}.{tbl.upper()}_counts_{DAY_STR}.csv"
                df.to_csv(per_table_csv, index=False)

                # Accumulate for aggregated CSV
                tmp = df.copy()
                tmp.insert(0, "table", tbl.upper())
                tmp.insert(0, "schema", SCHEMA.upper())
                all_rows.append(tmp)

                # Totals
                totals.append((tbl.upper(), int(df["count"].sum())))

            except Exception as e:
                msg = str(e)
                print(f"  ERROR on {tbl}: {msg}")
                errors.append((tbl.upper(), msg))

        # Write aggregated CSV: all tables x 24 hours
        if all_rows:
            agg_df = pd.concat(all_rows, ignore_index=True)
            agg_csv = OUT_DIR / f"all_tables_hourly_counts_{DAY_STR}.csv"
            agg_df.to_csv(agg_csv, index=False)

        # Write totals for all tables
        if totals:
            totals_df = pd.DataFrame(totals, columns=["table", "total_count"]).sort_values(
                "total_count", ascending=False
            )
            totals_csv = OUT_DIR / f"totals_all_tables_{DAY_STR}.csv"
            totals_df.to_csv(totals_csv, index=False)

            # Top 10
            top10_df = totals_df.head(10)
            top10_csv = OUT_DIR / f"top10_tables_by_total_count_{DAY_STR}.csv"
            top10_df.to_csv(top10_csv, index=False)

            print("\nTop 10 tables by total rows:")
            print(top10_df.to_string(index=False))

        if errors:
            err_df = pd.DataFrame(errors, columns=["table", "error"])
            err_csv = OUT_DIR / f"errors_{DAY_STR}.csv"
            err_df.to_csv(err_csv, index=False)
            print(f"\nSome tables failed; see {err_csv.name}")

        print(f"\nDone. Files in: {OUT_DIR.resolve()}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
