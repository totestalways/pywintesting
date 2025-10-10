# hourly_counts_string_windows.py
# pip install oracledb pandas
#
# Assumes your TIME column is VARCHAR2 with lexicographically sortable timestamps
# like '20251009-00:00:00.000000' (YYYYMMDD-HH24:MI:SS.FF6).
# We bind *strings* for :window_start and :window_end.

import datetime as dt
from pathlib import Path
import pandas as pd
import oracledb

# ========== CONFIG ==========
HOST = "your-db-host"
PORT = 1521
SERVICE_NAME = "your_service_name"  # e.g. "orclpdb1"
DB_USER = "YOUR_USER"
DB_PASSWORD = "YOUR_PASSWORD"

SCHEMA = "YOUR_SCHEMA"        # Oracle owner (UPPERCASE)
TIME_COLUMN = "TIME"          # Literal column name used in SQL

# Day to scan (inclusive start; exclusive end = next midnight)
DAY_STR = "20251009"          # YYYYMMDD
# String format for binds -> must match the way TIME values are stored
TIME_FORMAT = "%Y%m%d-%H:%M:%S.%f"

OUT_DIR = Path("oracle_hourly_counts_out")
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ===========================


def _dsn():
    return oracledb.makedsn(HOST, PORT, service_name=SERVICE_NAME)


def _day_bounds(day_str: str):
    d = dt.datetime.strptime(day_str, "%Y%m%d")
    start = d.replace(hour=0, minute=0, second=0, microsecond=0)
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
      SELECT 1 FROM all_tab_columns
      WHERE owner=:owner AND table_name=:t AND column_name=:c
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=owner.upper(), t=table.upper(), c=col.upper())
        return cur.fetchone() is not None


def _count_window(conn, owner: str, table: str,
                  window_start_str: str, window_end_str: str) -> int:
    """
    Count rows in [window_start_str, window_end_str) using *string* binds.
    SQL uses the literal column TIME and no arithmetic.
    """
    q_owner = f'"{owner.upper()}"'
    q_table = f'"{table.upper()}"'
    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM   {q_owner}.{q_table} t
        WHERE  t.{TIME_COLUMN} >= :window_start
          AND  t.{TIME_COLUMN} <  :window_end
    """
    with conn.cursor() as cur:
        cur.execute(sql, window_start=window_start_str, window_end=window_end_str)
        return int(cur.fetchone()[0])


def main():
    start_ts, end_ts_excl = _day_bounds(DAY_STR)
    hours = list(pd.date_range(start=start_ts, end=end_ts_excl, freq="H", inclusive="left"))

    all_rows = []   # schema, table, hour, count
    totals = []     # (table, total_count)
    errors = []     # (table, error_message)

    print("Connecting to Oracle ...")
    conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=_dsn())
    try:
        tables = _get_tables(conn, SCHEMA)
        print(f"Found {len(tables)} tables in {SCHEMA}.")

        for tbl in tables:
            if not _has_time_col(conn, SCHEMA, tbl, TIME_COLUMN):
                print(f"- Skipping {tbl}: missing time column {TIME_COLUMN}")
                continue

            print(f"- {tbl}: counting hourly using column {TIME_COLUMN} (string binds) ...")
            try:
                per_table_data = []
                total = 0

                for h in hours:
                    h_next = h + dt.timedelta(hours=1)
                    # Format both bounds as strings (e.g., '20251009-13:00:00.000000')
                    ws = h.strftime(TIME_FORMAT)
                    we = h_next.strftime(TIME_FORMAT)

                    cnt = _count_window(conn, SCHEMA, tbl, ws, we)
                    per_table_data.append((h, cnt))
                    total += cnt

                # Save per-table CSV
                df = pd.DataFrame(per_table_data, columns=["hour", "count"])
                df.to_csv(OUT_DIR / f"{SCHEMA.upper()}.{tbl.upper()}_counts_{DAY_STR}.csv", index=False)

                # Aggregate for the big CSV
                tmp = df.copy()
                tmp.insert(0, "table", tbl.upper())
                tmp.insert(0, "schema", SCHEMA.upper())
                all_rows.append(tmp)

                # Totals for Top-10
                totals.append((tbl.upper(), total))

            except Exception as e:
                msg = str(e)
                print(f"  ERROR {tbl}: {msg}")
                errors.append((tbl.upper(), msg))

        # Write aggregated CSV
        if all_rows:
            agg_df = pd.concat(all_rows, ignore_index=True)
            agg_df.to_csv(OUT_DIR / f"all_tables_hourly_counts_{DAY_STR}.csv", index=False)

        # Totals and Top-10
        if totals:
            totals_df = pd.DataFrame(totals, columns=["table", "total_count"]).sort_values(
                "total_count", ascending=False
            )
            totals_df.to_csv(OUT_DIR / f"totals_all_tables_{DAY_STR}.csv", index=False)
            totals_df.head(10).to_csv(OUT_DIR / f"top10_tables_by_total_count_{DAY_STR}.csv", index=False)
            print("\nTop 10 tables by total rows:")
            print(totals_df.head(10).to_string(index=False))

        # Errors
        if errors:
            pd.DataFrame(errors, columns=["table", "error"]).to_csv(
                OUT_DIR / f"errors_{DAY_STR}.csv", index=False
            )
            print("\nSome tables failed; see errors CSV.")

        print(f"\nDone. Files in: {OUT_DIR.resolve()}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
