# hourly_counts_robust.py
# pip install oracledb pandas

import datetime as dt
from pathlib import Path
import pandas as pd
import oracledb

# ========== CONFIG ==========
HOST = "your-db-host"
PORT = 1521
SERVICE_NAME = "your_service_name"
DB_USER = "YOUR_USER"
DB_PASSWORD = "YOUR_PASSWORD"
SCHEMA = "YOUR_SCHEMA"  # Oracle owner

# Default time column name (UPPERCASE). Override per-table below.
TIME_COLUMN_DEFAULT = "TIME"

# Per-table time column overrides (all UPPERCASE)
TIME_COLUMN_MAP = {
    # "ORDERS": "CREATED_AT",
    # "LOGS": "EVENT_TS",
}

# Tell the script how to interpret non-native time columns.
# Allowed kinds:
#   "NATIVE" (DATE/TIMESTAMP/TIMESTAMP WITH (LOCAL) TIME ZONE)
#   "TEXT_MASK" (VARCHAR2 to TO_TIMESTAMP)
#   "EPOCH_SECONDS" | "EPOCH_MILLIS" | "EPOCH_MICROS" (NUMBER epoch)
#   "YYYYMMDDHH24MISS" (NUMBER)
#   "YYYYMMDDHH24MISSFF6" (NUMBER)
TIME_KIND_DEFAULT = "NATIVE"
TIME_KIND_MAP = {
    # "EVENTS": "EPOCH_MILLIS",
    # "RAWLOG": "TEXT_MASK",
    # "LEGACY": "YYYYMMDDHH24MISS",
}

# For TEXT_MASK tables, provide a TO_TIMESTAMP mask (per table or default)
TEXT_MASK_DEFAULT = "YYYYMMDD-HH24:MI:SS.FF6"  # matches 20251009-00:00:00.000000
TEXT_MASK_MAP = {
    # "RAWLOG": "YYYY-MM-DD\"T\"HH24:MI:SS.FF3",
}

# Day to scan (YYYYMMDD). End is next midnight (exclusive).
DAY_STR = "20251009"

OUT_DIR = Path("oracle_hourly_counts_out")
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ============================


def _dsn():
    return oracledb.makedsn(HOST, PORT, service_name=SERVICE_NAME)


def _day_bounds(day_str: str):
    d = dt.datetime.strptime(day_str, "%Y%m%d")
    start = dt.datetime(d.year, d.month, d.day, 0, 0, 0, 0)
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
        row = cur.fetchone()
    return row  # (DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE) or None


def _time_kind_for(table: str, data_type: str):
    # If user overrode, trust it; else infer basics.
    kind = TIME_KIND_MAP.get(table.upper())
    if kind:
        return kind

    dtp = (data_type or "").upper()
    if dtp in ("DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"):
        return "NATIVE"
    elif dtp in ("VARCHAR2", "CHAR", "NCHAR", "NVARCHAR2"):
        return "TEXT_MASK"
    elif dtp == "NUMBER":
        # Fall back to epoch seconds if not configured.
        return "EPOCH_SECONDS"
    else:
        return TIME_KIND_DEFAULT


def _expr_ts(owner: str, table: str, col: str, data_type: str):
    """
    Returns a SQL expression that converts t.<col> to TIMESTAMP safely.
    We don't bind masks/kinds; they come from the config above.
    """
    tname = table.upper()
    kind = _time_kind_for(tname, data_type)

    # Fully qualified table already used outside; we only need "t.<col>" here.
    c = col.upper()

    if kind == "NATIVE":
        # Works for DATE and TIMESTAMP* types; cast for consistent typing.
        return f"CAST(t.{c} AS TIMESTAMP)"

    if kind == "TEXT_MASK":
        mask = TEXT_MASK_MAP.get(tname, TEXT_MASK_DEFAULT).replace("'", "''")
        return f"TO_TIMESTAMP(t.{c}, '{mask}')"

    if kind == "EPOCH_SECONDS":
        # 1970-01-01 + seconds
        return ("TO_TIMESTAMP('1970-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') "
                f"+ NUMTODSINTERVAL(t.{c}, 'SECOND')")

    if kind == "EPOCH_MILLIS":
        return ("TO_TIMESTAMP('1970-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') "
                f"+ NUMTODSINTERVAL(t.{c}/1000, 'SECOND')")

    if kind == "EPOCH_MICROS":
        return ("TO_TIMESTAMP('1970-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') "
                f"+ NUMTODSINTERVAL(t.{c}/1000000, 'SECOND')")

    if kind == "YYYYMMDDHH24MISS":
        # Ensure leading zeros by padding to 14
        return f"TO_TIMESTAMP(LPAD(TO_CHAR(t.{c}),14,'0'),'YYYYMMDDHH24MISS')"

    if kind == "YYYYMMDDHH24MISSFF6":
        # Pad to 20
        return f"TO_TIMESTAMP(LPAD(TO_CHAR(t.{c}),20,'0'),'YYYYMMDDHH24MISSFF6')"

    # Fallback: treat as native (may still fail if truly numeric/text without config)
    return f"CAST(t.{c} AS TIMESTAMP)"


def _hourly_counts(conn, owner: str, table: str, time_col: str, start_ts: dt.datetime, end_ts_excl: dt.datetime):
    q_owner = f'"{owner.upper()}"'
    q_table = f'"{table.upper()}"'

    meta = _get_col_meta(conn, owner, table, time_col)
    if meta is None:
        raise RuntimeError(f"Column {time_col} not found on {owner}.{table}")
    data_type = meta[0]

    expr_ts = _expr_ts(owner, table, time_col, data_type)

    # Bucket by hour: TRUNC(CAST(.. AS DATE),'HH24') gives DATE; cast back to TIMESTAMP for pretty output
    sql = f"""
        SELECT CAST(TRUNC(CAST({expr_ts} AS DATE), 'HH24') AS TIMESTAMP) AS hour_bucket,
               COUNT(*) AS cnt
        FROM   {q_owner}.{q_table} t
        WHERE  {expr_ts} >= :start_ts
          AND  {expr_ts} <  :end_ts
        GROUP  BY CAST(TRUNC(CAST({expr_ts} AS DATE), 'HH24') AS TIMESTAMP)
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

    all_rows = []
    totals = []
    errors = []

    print("Connecting to Oracle ...")
    conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=_dsn())
    try:
        tables = _get_tables(conn, SCHEMA)
        print(f"Found {len(tables)} tables in {SCHEMA}.")

        for tbl in tables:
            time_col = TIME_COLUMN_MAP.get(tbl.upper(), TIME_COLUMN_DEFAULT).upper()

            try:
                df_counts = _hourly_counts(conn, SCHEMA, tbl, time_col, start_ts, end_ts_excl)

                # Ensure full 24-hour coverage
                df = hours_df.merge(df_counts, on="hour", how="left")
                df["count"] = df["count"].fillna(0).astype("int64")

                # Per-table CSV
                per_table_csv = OUT_DIR / f"{SCHEMA.upper()}.{tbl.upper()}_counts_{DAY_STR}.csv"
                df.to_csv(per_table_csv, index=False)

                # Aggregate rows
                tmp = df.copy()
                tmp.insert(0, "table", tbl.upper())
                tmp.insert(0, "schema", SCHEMA.upper())
                all_rows.append(tmp)

                totals.append((tbl.upper(), int(df["count"].sum())))

            except Exception as e:
                msg = str(e)
                print(f"ERROR {tbl}: {msg}")
                errors.append((tbl.upper(), msg))

        if all_rows:
            agg_df = pd.concat(all_rows, ignore_index=True)
            agg_df.to_csv(OUT_DIR / f"all_tables_hourly_counts_{DAY_STR}.csv", index=False)

        if totals:
            totals_df = pd.DataFrame(totals, columns=["table", "total_count"]).sort_values(
                "total_count", ascending=False
            )
            totals_df.to_csv(OUT_DIR / f"totals_all_tables_{DAY_STR}.csv", index=False)
            totals_df.head(10).to_csv(OUT_DIR / f"top10_tables_by_total_count_{DAY_STR}.csv", index=False)
            print("\nTop 10 tables:")
            print(totals_df.head(10).to_string(index=False))

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
