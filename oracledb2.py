# hourly_counts_bound_ends.py
# pip install oracledb pandas
#
# What it creates (in ./oracle_hourly_counts_out):
#  - {SCHEMA}.{TABLE}_counts_20251009.csv          (per-table hourly counts: hour,count)
#  - all_tables_hourly_counts_20251009.csv         (schema,table,hour,count for all tables)
#  - totals_all_tables_20251009.csv                (table,total_count)
#  - top10_tables_by_total_count_20251009.csv      (Top 10 by total_count)
#  - errors_20251009.csv                           (if any failures)
#
# Notes
# - No CLI args: edit CONFIG and run.
# - 1-hour windows are computed in Python; SQL receives :window_start and :window_end.
# - If your time column isn't DATE/TIMESTAMP, set TIME_KIND_MAP/TEXT_MASK_MAP accordingly.

import datetime as dt
from pathlib import Path
import pandas as pd
import oracledb

# ========== CONFIG ==========
HOST = "your-db-host"
PORT = 1521
SERVICE_NAME = "your_service_name"  # e.g. "orclpdb1" or "xe"

DB_USER = "YOUR_USER"
DB_PASSWORD = "YOUR_PASSWORD"

SCHEMA = "YOUR_SCHEMA"          # Oracle owner (dictionary views expect UPPERCASE)

# Default time column name (UPPERCASE). Override per table with TIME_COLUMN_MAP.
TIME_COLUMN_DEFAULT = "TIME"
TIME_COLUMN_MAP = {
    # "ORDERS": "CREATED_AT",
    # "EVENTS": "EVENT_TS",
}

# Tell the script how to interpret non-native time columns.
# Allowed kinds:
#   "NATIVE"               -> DATE / TIMESTAMP / TIMESTAMP WITH (LOCAL) TIME ZONE
#   "TEXT_MASK"            -> VARCHAR2/CHAR; will apply TO_TIMESTAMP with a mask
#   "EPOCH_SECONDS"        -> NUMBER epoch seconds since 1970-01-01
#   "EPOCH_MILLIS"         -> NUMBER epoch milliseconds
#   "EPOCH_MICROS"         -> NUMBER epoch microseconds
#   "YYYYMMDDHH24MISS"     -> NUMBER like 20251009123059
#   "YYYYMMDDHH24MISSFF6"  -> NUMBER like 20251009123059ffffff (microseconds)
TIME_KIND_DEFAULT = "NATIVE"
TIME_KIND_MAP = {
    # "RAWLOG": "TEXT_MASK",
    # "EVT_MS": "EPOCH_MILLIS",
    # "LEGACY": "YYYYMMDDHH24MISS",
}

# For TEXT_MASK tables, provide a TO_TIMESTAMP mask (per table or default)
TEXT_MASK_DEFAULT = "YYYYMMDD-HH24:MI:SS.FF6"  # matches '20251009-00:00:00.000000'
TEXT_MASK_MAP = {
    # "RAWLOG": "YYYY-MM-DD\"T\"HH24:MI:SS.FF3",
}

# Day to scan (inclusive start; exclusive end = next midnight)
DAY_STR = "20251009"  # YYYYMMDD

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


def _get_col_meta(conn, owner: str, table: str, col: str):
    sql = """
        SELECT data_type, data_length, data_precision, data_scale
        FROM   all_tab_columns
        WHERE  owner = :owner
          AND  table_name = :table_name
          AND  column_name = :col
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=owner.upper(), table_name=table.upper(), col=col.upper())
        return cur.fetchone()  # or None


def _time_kind_for(table: str, data_type: str):
    # Respect explicit per-table mapping first.
    kind = TIME_KIND_MAP.get(table.upper())
    if kind:
        return kind

    dtp = (data_type or "").upper()
    if dtp in ("DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"):
        return "NATIVE"
    elif dtp in ("VARCHAR2", "CHAR", "NCHAR", "NVARCHAR2"):
        return "TEXT_MASK"
    elif dtp == "NUMBER":
        # Default assumption for NUMBER is epoch seconds unless overridden.
        return "EPOCH_SECONDS"
    return TIME_KIND_DEFAULT


def _expr_ts(table: str, col: str, data_type: str):
    """
    Build a SQL expression that converts t.<col> to TIMESTAMP based on the column kind.
    Return a string like "TO_TIMESTAMP(t.COL,'YYYY..')" or "CAST(t.COL AS TIMESTAMP)".
    """
    tname = table.upper()
    c = col.upper()
    kind = _time_kind_for(tname, data_type)

    if kind == "NATIVE":
        return f"CAST(t.{c} AS TIMESTAMP)"

    if kind == "TEXT_MASK":
        mask = TEXT_MASK_MAP.get(tname, TEXT_MASK_DEFAULT).replace("'", "''")
        return f"TO_TIMESTAMP(t.{c}, '{mask}')"

    if kind == "EPOCH_SECONDS":
        return ("TO_TIMESTAMP('1970-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') "
                f"+ NUMTODSINTERVAL(t.{c}, 'SECOND')")

    if kind == "EPOCH_MILLIS":
        return ("TO_TIMESTAMP('1970-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') "
                f"+ NUMTODSINTERVAL(t.{c}/1000, 'SECOND')")

    if kind == "EPOCH_MICROS":
        return ("TO_TIMESTAMP('1970-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') "
                f"+ NUMTODSINTERVAL(t.{c}/1000000, 'SECOND')")

    if kind == "YYYYMMDDHH24MISS":
        return f"TO_TIMESTAMP(LPAD(TO_CHAR(t.{c}),14,'0'),'YYYYMMDDHH24MISS')"

    if kind == "YYYYMMDDHH24MISSFF6":
        return f"TO_TIMESTAMP(LPAD(TO_CHAR(t.{c}),20,'0'),'YYYYMMDDHH24MISSFF6')"

    # Fallback (acts like NATIVE)
    return f"CAST(t.{c} AS TIMESTAMP)"


def _count_window(conn, owner: str, table: str, time_col: str,
                  window_start: dt.datetime, window_end: dt.datetime) -> int:
    """
    Count rows in [window_start, window_end) for the given table/column.
    Uses a *bound* :window_start and :window_end (no arithmetic in SQL).
    """
    q_owner = f'"{owner.upper()}"'
    q_table = f'"{table.upper()}"'

    meta = _get_col_meta(conn, owner, table, time_col)
    if meta is None:
        raise RuntimeError(f"Column {time_col} not found on {owner}.{table}")
    data_type = meta[0]

    expr_ts = _expr_ts(table, time_col, data_type)

    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM   {q_owner}.{q_table} t
        WHERE  {expr_ts} >= :window_start
          AND  {expr_ts} <  :window_end
    """
    with conn.cursor() as cur:
        cur.execute(sql, window_start=window_start, window_end=window_end)
        return int(cur.fetchone()[0])


def _has_time_col(conn, owner: str, table: str, col: str) -> bool:
    sql = """
      SELECT 1 FROM all_tab_columns
      WHERE owner=:owner AND table_name=:t AND column_name=:c
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=owner.upper(), t=table.upper(), c=col.upper())
        return cur.fetchone() is not None


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
            time_col = TIME_COLUMN_MAP.get(tbl.upper(), TIME_COLUMN_DEFAULT).upper()

            if not _has_time_col(conn, SCHEMA, tbl, time_col):
                print(f"- Skipping {tbl}: missing time column {time_col}")
                continue

            print(f"- {tbl}: counting hourly using {time_col} ...")
            try:
                per_table_data = []
                total = 0
                for h in hours:
                    h_next = h + dt.timedelta(hours=1)
                    cnt = _count_window(conn, SCHEMA, tbl, time_col, h.to_pydatetime(), h_next.to_pydatetime())
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
