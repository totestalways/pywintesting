Skip to content
Chat history

You said:
can you write me a very simple script which does the following:

1. connects to a oracle database
2. retrieves all the tables from a certain schema
3. runs queries on all the tables for how many entries they had at each time of the day - each hour (I can pass the table column that has the time)
ChatGPT said:

Thought for 59s
Absolutely—here’s a small, self-contained Python script that:

connects to Oracle,

lists all tables in a given schema that have a specific time column,

and for each such table, prints row counts grouped by hour.

It uses the modern python-oracledb driver (thin mode, so no Instant Client needed).

#!/usr/bin/env python3
"""
oracle_hourly_counts.py

Connects to an Oracle DB, finds all tables in a schema that have a given time column,
and prints how many rows each table has per hour.

Requirements:
    pip install python-oracledb

Example:
    python oracle_hourly_counts.py \
        --host db.example.com --port 1521 --service-name ORCLPDB1 \
        --user scott --password tiger \
        --schema SALES \
        --time-column CREATED_AT \
        --start-date 2025-10-01 --end-date 2025-10-09

Notes:
- The time column can be DATE, TIMESTAMP, or TIMESTAMP WITH (LOCAL) TIME ZONE.
- Results are grouped by TRUNC(CAST(time_column AS DATE), 'HH') (start of hour).
- Date filters are optional.
"""

import argparse
import datetime as dt
import re
import sys
import oracledb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Hourly row counts per table in an Oracle schema.")
    p.add_argument("--host", required=True, help="Oracle host")
    p.add_argument("--port", type=int, default=1521, help="Oracle port (default: 1521)")
    p.add_argument("--service-name", required=True, help="Oracle service name (e.g., ORCLPDB1)")
    p.add_argument("--user", required=True, help="DB username")
    p.add_argument("--password", required=True, help="DB password")
    p.add_argument("--schema", required=True, help="Schema/owner to scan (e.g., SALES)")
    p.add_argument("--time-column", required=True, help="Time column present in the target tables")
    p.add_argument("--start-date", help="Optional start date (YYYY-MM-DD) inclusive")
    p.add_argument("--end-date", help="Optional end date (YYYY-MM-DD) exclusive")
    return p.parse_args()


IDENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")  # simple Oracle identifier check


def assert_safe_identifier(name: str, kind: str):
    if not IDENT_RE.match(name):
        raise ValueError(f"Unsafe {kind} identifier: {name!r}. Use unquoted, simple names.")


def build_dsn(host: str, port: int, service_name: str) -> str:
    # Thin mode accepts "host:port/service_name"
    return f"{host}:{port}/{service_name}"


def parse_iso_date(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    # Interpret as midnight in naive local time; Oracle driver will send as TIMESTAMP
    return dt.datetime.strptime(s, "%Y-%m-%d")


def find_tables_with_column(conn, schema: str, column: str) -> list[str]:
    sql = """
        SELECT t.table_name
        FROM all_tables t
        WHERE t.owner = :owner
          AND EXISTS (
                SELECT 1
                FROM all_tab_columns c
                WHERE c.owner = t.owner
                  AND c.table_name = t.table_name
                  AND c.column_name = :col
          )
        ORDER BY t.table_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=schema, col=column)
        return [r[0] for r in cur.fetchall()]


def hourly_counts_for_table(conn, schema: str, table: str, time_col: str,
                            start_dt: dt.datetime | None, end_dt: dt.datetime | None):
    """
    Groups by the start of the hour using TRUNC(CAST(time_col AS DATE), 'HH').
    """
    # We can't bind identifiers, so we whitelist-check and then inject safely.
    assert_safe_identifier(schema, "schema")
    assert_safe_identifier(table, "table")
    assert_safe_identifier(time_col, "column")

    base_sql = f"""
        SELECT TRUNC(CAST({time_col} AS DATE), 'HH') AS hour_start,
               COUNT(*) AS row_count
        FROM {schema}.{table}
        WHERE {time_col} IS NOT NULL
    """

    binds = {}
    if start_dt is not None:
        base_sql += f" AND {time_col} >= :start_dt"
        binds["start_dt"] = start_dt
    if end_dt is not None:
        base_sql += f" AND {time_col} < :end_dt"
        binds["end_dt"] = end_dt

    base_sql += """
        GROUP BY TRUNC(CAST({col} AS DATE), 'HH')
        ORDER BY hour_start
    """.format(col=time_col)

    with conn.cursor() as cur:
        cur.execute(base_sql, binds)
        return cur.fetchall()  # list of (hour_start, row_count)


def main():
    args = parse_args()

    # Normalize to Oracle dictionary casing (unquoted identifiers are stored uppercase)
    schema = args.schema.upper()
    time_col = args.time_column.upper()

    start_dt = parse_iso_date(args.start_date)
    end_dt = parse_iso_date(args.end_date)

    dsn = build_dsn(args.host, args.port, args.service_name)
    try:
        # Thin mode (no Instant Client needed). Remove "mode" to default to thin.
        conn = oracledb.connect(user=args.user, password=args.password, dsn=dsn)
    except Exception as e:
        print(f"❌ Failed to connect: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        tables = find_tables_with_column(conn, schema, time_col)
        if not tables:
            print(f"ℹ️ No tables in schema {schema} contain column {time_col}.")
            return

        print(f"Found {len(tables)} table(s) in {schema} with column {time_col}.\n")

        for tbl in tables:
            print(f"=== {schema}.{tbl} ===")
            try:
                rows = hourly_counts_for_table(conn, schema, tbl, time_col, start_dt, end_dt)
                if not rows:
                    print("  (no data)")
                else:
                    for hour_start, cnt in rows:
                        # hour_start is a datetime coming from Oracle DATE
                        # Print in ISO without seconds for clarity
                        stamp = hour_start.strftime("%Y-%m-%d %H:00")
                        print(f"  {stamp}  ->  {cnt}")
            except Exception as e:
                print(f"  ⚠️ Query failed: {e}")
            print()  # blank line between tables
    finally:
        conn.close()


if __name__ == "__main__":
    main()
Quick setup
Install the driver:

pip install python-oracledb
Run the script with your connection info, schema, and time column:

python oracle_hourly_counts.py \
  --host db.example.com --port 1521 --service-name ORCLPDB1 \
  --user YOUR_USER --password YOUR_PASS \
  --schema YOUR_SCHEMA \
  --time-column CREATED_AT \
  --start-date 2025-10-01 --end-date 2025-10-09
Notes & tweaks
The script only processes tables that actually have the given time column (case-insensitive; it uppercases to match Oracle’s data dictionary).

If you skip --start-date/--end-date, it scans all available data.

Grouping uses TRUNC(CAST(col AS DATE), 'HH'), which works for DATE and TIMESTAMP* types.

Results reflect the database session time zone. If you need a specific time zone, we can set ALTER SESSION SET time_zone = 'UTC' right after connecting.

If you want CSV output per table or a single consolidated CSV, say the word and I’ll extend it.

You said:
I do not want it to use arguments
also have it save in a csv all hte times for each table
ChatGPT said:

Thought for 17s
Got it—no CLI args, and save everything into a single CSV. Here’s a simple, drop-in Python script:

#!/usr/bin/env python3
"""
Simple Oracle hourly counts per table (single CSV output).

- Connects to Oracle (python-oracledb, thin mode).
- Finds all *tables* in a schema that contain a given time column.
- For each table, counts rows grouped by hour and writes a single CSV:
    columns: schema, table_name, hour_start, row_count

Setup:
    pip install python-oracledb

Edit the CONFIG section below and run:
    python oracle_hourly_counts_noargs.py
"""

import csv
import datetime as dt
import re
import sys

import oracledb  # pip install python-oracledb

# ----------------------------- CONFIG ---------------------------------
DB_HOST       = "db.example.com"
DB_PORT       = 1521
DB_SERVICE    = "ORCLPDB1"
DB_USER       = "SCOTT"
DB_PASSWORD   = "tiger"

SCHEMA        = "SALES"         # schema/owner to scan (unquoted identifier)
TIME_COLUMN   = "CREATED_AT"    # time column present in the target tables (unquoted)

# Optional date range filters (inclusive start, exclusive end). Set to None to skip.
START_DATE    = None            # e.g. "2025-10-01" or None
END_DATE      = None            # e.g. "2025-10-09" or None

OUTPUT_CSV    = "hourly_counts.csv"
# ----------------------------------------------------------------------


IDENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")  # simple Oracle identifier check


def assert_safe_identifier(name: str, kind: str):
    if not IDENT_RE.match(name):
        raise ValueError(f"Unsafe {kind} identifier: {name!r}. Use simple, unquoted names.")


def dsn(host: str, port: int, service: str) -> str:
    # Thin mode DSN format
    return f"{host}:{port}/{service}"


def parse_iso_date(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    return dt.datetime.strptime(s, "%Y-%m-%d")


def find_tables_with_column(conn, schema: str, column: str) -> list[str]:
    """
    Returns table names in `schema` that contain `column`.
    Limits to real tables (excludes views) by joining ALL_TABLES.
    """
    sql = """
        SELECT t.table_name
        FROM all_tables t
        WHERE t.owner = :owner
          AND EXISTS (
                SELECT 1
                FROM all_tab_columns c
                WHERE c.owner = t.owner
                  AND c.table_name = t.table_name
                  AND c.column_name = :col
          )
        ORDER BY t.table_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=schema, col=column)
        return [r[0] for r in cur.fetchall()]


def hourly_counts(conn, schema: str, table: str, time_col: str,
                  start_dt: dt.datetime | None, end_dt: dt.datetime | None):
    """
    Returns rows of (hour_start, row_count) for the given table.
    Groups by start of hour using TRUNC(CAST(time_col AS DATE), 'HH').
    """
    assert_safe_identifier(schema, "schema")
    assert_safe_identifier(table, "table")
    assert_safe_identifier(time_col, "column")

    sql = f"""
        SELECT TRUNC(CAST({time_col} AS DATE), 'HH') AS hour_start,
               COUNT(*) AS row_count
        FROM {schema}.{table}
        WHERE {time_col} IS NOT NULL
    """

    binds = {}
    if start_dt is not None:
        sql += f" AND {time_col} >= :start_dt"
        binds["start_dt"] = start_dt
    if end_dt is not None:
        sql += f" AND {time_col} < :end_dt"
        binds["end_dt"] = end_dt

    sql += f"""
        GROUP BY TRUNC(CAST({time_col} AS DATE), 'HH')
        ORDER BY hour_start
    """

    with conn.cursor() as cur:
        cur.execute(sql, binds)
        return cur.fetchall()  # list of (datetime, int)


def main():
    # Normalize identifiers to dictionary case (Oracle stores unquoted identifiers uppercase)
    schema = SCHEMA.upper()
    time_col = TIME_COLUMN.upper()

    start_dt = parse_iso_date(START_DATE)
    end_dt = parse_iso_date(END_DATE)

    # Connect (thin mode; no Instant Client required)
    try:
        conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn(DB_HOST, DB_PORT, DB_SERVICE))
    except Exception as e:
        print(f"❌ Failed to connect: {e}", file=sys.stderr)
        sys.exit(2)

    # Uncomment to force session time zone (optional):
    # with conn.cursor() as cur:
    #     cur.execute("ALTER SESSION SET time_zone = 'UTC'")

    try:
        tables = find_tables_with_column(conn, schema, time_col)
        if not tables:
            print(f"ℹ️ No tables in schema {schema} contain column {time_col}.")
            return

        print(f"Found {len(tables)} table(s) with column {time_col}. Writing {OUTPUT_CSV} ...")

        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["schema", "table_name", "hour_start", "row_count"])

            for tbl in tables:
                try:
                    rows = hourly_counts(conn, schema, tbl, time_col, start_dt, end_dt)
                    for hour_start, cnt in rows:
                        # Format hour_start like "YYYY-MM-DD HH:00"
                        if isinstance(hour_start, dt.datetime):
                            stamp = hour_start.strftime("%Y-%m-%d %H:00")
                        else:
                            # Fallback if driver returns date
                            stamp = dt.datetime.combine(hour_start, dt.time()).strftime("%Y-%m-%d %H:00")
                        writer.writerow([schema, tbl, stamp, cnt])
                except Exception as e:
                    # Log per-table errors but continue with the rest
                    print(f"  ⚠️ {schema}.{tbl}: {e}", file=sys.stderr)

        print("✅ Done.")
        print(f"CSV saved to: {OUTPUT_CSV}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
How to use
pip install python-oracledb

Edit the CONFIG block (host, service, user/password, SCHEMA, TIME_COLUMN, optional START/END dates).

Run python oracle_hourly_counts_noargs.py.

You’ll get a single hourly_counts.csv with rows like:

schema,table_name,hour_start,row_count
SALES,ORDERS,2025-10-01 09:00,123
SALES,ORDERS,2025-10-01 10:00,98
...
If you prefer one CSV per table instead (e.g., SALES_ORDERS.csv, SALES_INVOICES.csv), say the word and I’ll switch the writer to split files automatically.

You said:
separate csvs please
ChatGPT said:

Thought for 26s
Perfect—here’s the split-CSV version (no CLI args). It writes one CSV per table into an output folder.

#!/usr/bin/env python3
"""
Oracle hourly counts per table → separate CSVs.

- Connects to Oracle (python-oracledb, thin mode).
- Finds all *tables* in a schema that contain a given time column.
- For each such table, writes "<SCHEMA>_<TABLE>.csv" with:
      hour_start,row_count
  where hour_start is truncated to the start of the hour.

Setup:
    pip install python-oracledb

Use:
    1) Edit the CONFIG section.
    2) python oracle_hourly_counts_split.py
"""

import csv
import datetime as dt
import os
import re
import sys
from pathlib import Path

import oracledb  # pip install python-oracledb

# ----------------------------- CONFIG ---------------------------------
DB_HOST       = "db.example.com"
DB_PORT       = 1521
DB_SERVICE    = "ORCLPDB1"
DB_USER       = "SCOTT"
DB_PASSWORD   = "tiger"

SCHEMA        = "SALES"         # schema/owner to scan (unquoted identifier)
TIME_COLUMN   = "CREATED_AT"    # time column in target tables (unquoted)

# Optional date range filters (inclusive start, exclusive end). Set to None to scan all.
START_DATE    = None            # e.g. "2025-10-01" or None
END_DATE      = None            # e.g. "2025-10-09" or None

# Optional: force a specific session time zone (e.g., "UTC", "Europe/Bucharest").
SESSION_TIME_ZONE = None        # e.g. "UTC" or None

OUTPUT_DIR    = "hourly_counts" # folder where CSVs will be saved
# ----------------------------------------------------------------------


IDENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")  # simple Oracle identifier check


def assert_safe_identifier(name: str, kind: str):
    if not IDENT_RE.match(name):
        raise ValueError(f"Unsafe {kind} identifier: {name!r}. Use simple, unquoted names.")


def dsn(host: str, port: int, service: str) -> str:
    # Thin mode DSN format
    return f"{host}:{port}/{service}"


def parse_iso_date(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    return dt.datetime.strptime(s, "%Y-%m-%d")


def find_tables_with_column(conn, schema: str, column: str) -> list[str]:
    """
    Returns table names in `schema` that contain `column`.
    Limits to real tables (excludes views) by using ALL_TABLES.
    """
    sql = """
        SELECT t.table_name
        FROM all_tables t
        WHERE t.owner = :owner
          AND EXISTS (
                SELECT 1
                FROM all_tab_columns c
                WHERE c.owner = t.owner
                  AND c.table_name = t.table_name
                  AND c.column_name = :col
          )
        ORDER BY t.table_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=schema, col=column)
        return [r[0] for r in cur.fetchall()]


def hourly_counts(conn, schema: str, table: str, time_col: str,
                  start_dt: dt.datetime | None, end_dt: dt.datetime | None):
    """
    Returns rows of (hour_start, row_count) for the given table.
    Groups by the start of the hour using TRUNC(CAST(time_col AS DATE), 'HH').
    """
    assert_safe_identifier(schema, "schema")
    assert_safe_identifier(table, "table")
    assert_safe_identifier(time_col, "column")

    sql = f"""
        SELECT TRUNC(CAST({time_col} AS DATE), 'HH') AS hour_start,
               COUNT(*) AS row_count
        FROM {schema}.{table}
        WHERE {time_col} IS NOT NULL
    """

    binds = {}
    if start_dt is not None:
        sql += f" AND {time_col} >= :start_dt"
        binds["start_dt"] = start_dt
    if end_dt is not None:
        sql += f" AND {time_col} < :end_dt"
        binds["end_dt"] = end_dt

    sql += f"""
        GROUP BY TRUNC(CAST({time_col} AS DATE), 'HH')
        ORDER BY hour_start
    """

    with conn.cursor() as cur:
        cur.arraysize = 1000
        cur.execute(sql, binds)
        return cur.fetchall()  # list of (datetime, int)


def safe_csv_name(schema: str, table: str) -> str:
    base = f"{schema}_{table}"
    # Keep letters, digits, underscore, hyphen, dot
    return re.sub(r"[^A-Za-z0-9._-]", "_", base) + ".csv"


def main():
    # Normalize identifiers to dictionary case (Oracle stores unquoted identifiers uppercase)
    schema = SCHEMA.upper()
    time_col = TIME_COLUMN.upper()

    start_dt = parse_iso_date(START_DATE)
    end_dt = parse_iso_date(END_DATE)

    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Connect (thin mode; no Instant Client required)
    try:
        conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn(DB_HOST, DB_PORT, DB_SERVICE))
    except Exception as e:
        print(f"❌ Failed to connect: {e}", file=sys.stderr)
        sys.exit(2)

    # Optional: set session time zone
    if SESSION_TIME_ZONE:
        try:
            with conn.cursor() as cur:
                cur.execute("ALTER SESSION SET time_zone = :tz", tz=SESSION_TIME_ZONE)
        except Exception as e:
            print(f"⚠️ Could not set session time zone ({SESSION_TIME_ZONE}): {e}", file=sys.stderr)

    try:
        tables = find_tables_with_column(conn, schema, time_col)
        if not tables:
            print(f"ℹ️ No tables in schema {schema} contain column {time_col}.")
            return

        print(f"Found {len(tables)} table(s) with column {time_col}. Writing CSVs to {out_dir.resolve()}")

        for tbl in tables:
            try:
                rows = hourly_counts(conn, schema, tbl, time_col, start_dt, end_dt)
                csv_path = out_dir / safe_csv_name(schema, tbl)
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(["hour_start", "row_count"])
                    for hour_start, cnt in rows:
                        # Format hour_start like "YYYY-MM-DD HH:00"
                        if isinstance(hour_start, dt.datetime):
                            stamp = hour_start.strftime("%Y-%m-%d %H:00")
                        else:
                            stamp = dt.datetime.combine(hour_start, dt.time()).strftime("%Y-%m-%d %H:00")
                        w.writerow([stamp, cnt])
                print(f"  ✅ {schema}.{tbl} → {csv_path.name} ({len(rows)} rows)")
            except Exception as e:
                print(f"  ⚠️ {schema}.{tbl}: {e}", file=sys.stderr)

        print("Done.")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
Notes

Edit the CONFIG block; it outputs to hourly_counts/SCHEMA_TABLE.csv.

If you want the CSVs to also include the table name in the header, change the header to ["table_name","hour_start","row_count"] and write tbl as the first column on each row.

You said:
can you also make it so that it writes a top 10 tables with the biggest counts
ChatGPT said:

Thought for 11s
Absolutely—here’s the split-CSV script updated to also produce a Top 10 tables by total rows (within the optional date range). It writes one CSV per table plus a summary CSV.

#!/usr/bin/env python3
"""
Oracle hourly counts per table → separate CSVs + Top 10 summary.

- Connects to Oracle (python-oracledb, thin mode).
- Finds all tables in a schema that contain a given time column.
- For each table:
    • counts rows grouped by hour and writes "<SCHEMA>_<TABLE>.csv"
      with columns: hour_start,row_count
- After processing all tables:
    • writes "top10_tables_by_total_count.csv" containing the 10 tables
      with the highest TOTAL rows across the selected period:
      schema,table_name,total_rows,hours_covered,first_hour,last_hour,peak_hour_start,peak_hour_count

Setup:
    pip install python-oracledb

Use:
    1) Edit the CONFIG section.
    2) python oracle_hourly_counts_with_top10.py
"""

import csv
import datetime as dt
import re
import sys
from pathlib import Path

import oracledb  # pip install python-oracledb

# ----------------------------- CONFIG ---------------------------------
DB_HOST       = "db.example.com"
DB_PORT       = 1521
DB_SERVICE    = "ORCLPDB1"
DB_USER       = "SCOTT"
DB_PASSWORD   = "tiger"

SCHEMA        = "SALES"         # schema/owner to scan (unquoted identifier)
TIME_COLUMN   = "CREATED_AT"    # time column in target tables (unquoted)

# Optional date range filters (inclusive start, exclusive end). Set to None to scan all.
START_DATE    = None            # e.g. "2025-10-01" or None
END_DATE      = None            # e.g. "2025-10-09" or None

# Optional: force a specific session time zone (e.g., "UTC", "Europe/Bucharest").
SESSION_TIME_ZONE = None        # e.g. "UTC" or None

OUTPUT_DIR    = "hourly_counts" # folder where CSVs will be saved
# ----------------------------------------------------------------------


IDENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")  # simple Oracle identifier check


def assert_safe_identifier(name: str, kind: str):
    if not IDENT_RE.match(name):
        raise ValueError(f"Unsafe {kind} identifier: {name!r}. Use simple, unquoted names.")


def dsn(host: str, port: int, service: str) -> str:
    # Thin mode DSN format
    return f"{host}:{port}/{service}"


def parse_iso_date(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    return dt.datetime.strptime(s, "%Y-%m-%d")


def find_tables_with_column(conn, schema: str, column: str) -> list[str]:
    """
    Returns table names in `schema` that contain `column`.
    Limits to real tables (excludes views) by using ALL_TABLES.
    """
    sql = """
        SELECT t.table_name
        FROM all_tables t
        WHERE t.owner = :owner
          AND EXISTS (
                SELECT 1
                FROM all_tab_columns c
                WHERE c.owner = t.owner
                  AND c.table_name = t.table_name
                  AND c.column_name = :col
          )
        ORDER BY t.table_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=schema, col=column)
        return [r[0] for r in cur.fetchall()]


def hourly_counts(conn, schema: str, table: str, time_col: str,
                  start_dt: dt.datetime | None, end_dt: dt.datetime | None):
    """
    Returns rows of (hour_start, row_count) for the given table.
    Groups by the start of the hour using TRUNC(CAST(time_col AS DATE), 'HH').
    """
    assert_safe_identifier(schema, "schema")
    assert_safe_identifier(table, "table")
    assert_safe_identifier(time_col, "column")

    sql = f"""
        SELECT TRUNC(CAST({time_col} AS DATE), 'HH') AS hour_start,
               COUNT(*) AS row_count
        FROM {schema}.{table}
        WHERE {time_col} IS NOT NULL
    """

    binds = {}
    if start_dt is not None:
        sql += f" AND {time_col} >= :start_dt"
        binds["start_dt"] = start_dt
    if end_dt is not None:
        sql += f" AND {time_col} < :end_dt"
        binds["end_dt"] = end_dt

    sql += f"""
        GROUP BY TRUNC(CAST({time_col} AS DATE), 'HH')
        ORDER BY hour_start
    """

    with conn.cursor() as cur:
        cur.arraysize = 1000
        cur.execute(sql, binds)
        return cur.fetchall()  # list of (datetime/date, int)


def fmt_hour(value) -> str:
    """Format Oracle DATE/TIMESTAMP returned to Python as ISO 'YYYY-MM-DD HH:00'."""
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:00")
    # value could be datetime.date
    return dt.datetime.combine(value, dt.time()).strftime("%Y-%m-%d %H:00")


def main():
    # Normalize identifiers to dictionary case (Oracle stores unquoted identifiers uppercase)
    schema = SCHEMA.upper()
    time_col = TIME_COLUMN.upper()

    start_dt = parse_iso_date(START_DATE)
    end_dt = parse_iso_date(END_DATE)

    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Connect (thin mode; no Instant Client required)
    try:
        conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn(DB_HOST, DB_PORT, DB_SERVICE))
    except Exception as e:
        print(f"❌ Failed to connect: {e}", file=sys.stderr)
        sys.exit(2)

    # Optional: set session time zone
    if SESSION_TIME_ZONE:
        try:
            with conn.cursor() as cur:
                cur.execute("ALTER SESSION SET time_zone = :tz", tz=SESSION_TIME_ZONE)
        except Exception as e:
            print(f"⚠️ Could not set session time zone ({SESSION_TIME_ZONE}): {e}", file=sys.stderr)

    per_table_stats = []  # collect totals for Top 10

    try:
        tables = find_tables_with_column(conn, schema, time_col)
        if not tables:
            print(f"ℹ️ No tables in schema {schema} contain column {time_col}.")
            return

        print(f"Found {len(tables)} table(s) with column {time_col}. Writing CSVs to {out_dir.resolve()}")

        for tbl in tables:
            try:
                rows = hourly_counts(conn, schema, tbl, time_col, start_dt, end_dt)
                csv_path = out_dir / (f"{schema}_{tbl}.csv")
                # Write per-table CSV
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(["hour_start", "row_count"])
                    for hour_start, cnt in rows:
                        w.writerow([fmt_hour(hour_start), cnt])

                # Aggregate stats for Top 10
                total_rows = sum(cnt for _, cnt in rows) if rows else 0
                hours_covered = len(rows)
                first_hour = fmt_hour(rows[0][0]) if rows else ""
                last_hour = fmt_hour(rows[-1][0]) if rows else ""
                if rows:
                    peak_hour_start, peak_hour_count = max(rows, key=lambda r: r[1])
                    peak_hour_start = fmt_hour(peak_hour_start)
                else:
                    peak_hour_start, peak_hour_count = "", 0

                per_table_stats.append({
                    "schema": schema,
                    "table_name": tbl,
                    "total_rows": total_rows,
                    "hours_covered": hours_covered,
                    "first_hour": first_hour,
                    "last_hour": last_hour,
                    "peak_hour_start": peak_hour_start,
                    "peak_hour_count": peak_hour_count,
                })

                print(f"  ✅ {schema}.{tbl} → {csv_path.name} ({hours_covered} hourly rows, total={total_rows})")

            except Exception as e:
                print(f"  ⚠️ {schema}.{tbl}: {e}", file=sys.stderr)

        # Build Top 10 summary by total_rows
        per_table_stats.sort(key=lambda d: d["total_rows"], reverse=True)
        top10 = per_table_stats[:10]

        summary_path = out_dir / "top10_tables_by_total_count.csv"
        with open(summary_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "schema", "table_name", "total_rows",
                "hours_covered", "first_hour", "last_hour",
                "peak_hour_start", "peak_hour_count",
            ])
            for rec in top10:
                w.writerow([
                    rec["schema"], rec["table_name"], rec["total_rows"],
                    rec["hours_covered"], rec["first_hour"], rec["last_hour"],
                    rec["peak_hour_start"], rec["peak_hour_count"],
                ])

        # Also print a quick console leaderboard
        print("\nTop 10 tables by TOTAL rows:")
        for i, rec in enumerate(top10, 1):
            print(f"{i:2d}. {rec['schema']}.{rec['table_name']}: {rec['total_rows']} rows "
                  f"(peak {rec['peak_hour_count']} @ {rec['peak_hour_start']})")

        print(f"\n✅ Summary saved to: {summary_path.resolve()}")
        print("Done.")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
What “biggest counts” means here
I ranked tables by total rows across the selected period (or entire table if no dates set).

The summary also shows the peak hourly count and when it happened, plus coverage info.

If you’d rather rank by peak hourly count instead of total, say the word and I’ll switch the ranking (or include both rankings as separate CSVs).

You said:
can tyou update the script so that the time format from which we will query would be this:
20251009-00:00:00.000000
ChatGPT said:

Thought for 32s
Sure—updated to accept start/end timestamps in the exact format YYYYMMDD-HH:MM:SS.ffffff (e.g. 20251009-00:00:00.000000). It still writes one CSV per table and a Top-10 summary.

#!/usr/bin/env python3
"""
Oracle hourly counts per table → separate CSVs + Top 10 summary.

- Connects to Oracle (python-oracledb, thin mode).
- Finds all tables in a schema that contain a given time column.
- Optional time window filters now accept the exact format:
      YYYYMMDD-HH:MM:SS.ffffff   (example: 20251009-00:00:00.000000)
- For each table:
    • counts rows grouped by hour and writes "<SCHEMA>_<TABLE>.csv"
      with columns: hour_start,row_count
- Also writes a summary "top10_tables_by_total_count.csv".

Setup:
    pip install python-oracledb
"""

import csv
import datetime as dt
import re
import sys
from pathlib import Path

import oracledb  # pip install python-oracledb

# ----------------------------- CONFIG ---------------------------------
DB_HOST       = "db.example.com"
DB_PORT       = 1521
DB_SERVICE    = "ORCLPDB1"
DB_USER       = "SCOTT"
DB_PASSWORD   = "tiger"

SCHEMA        = "SALES"         # schema/owner to scan (unquoted identifier)
TIME_COLUMN   = "CREATED_AT"    # time column in target tables (unquoted)

# Time window (inclusive start, exclusive end) — set to None to scan all.
# FORMAT MUST BE: "YYYYMMDD-HH:MM:SS.ffffff"
START_TS_STR  = "20251001-00:00:00.000000"   # or None
END_TS_STR    = "20251009-00:00:00.000000"   # or None

# Optional: force a specific session time zone (e.g., "UTC", "Europe/Bucharest")
SESSION_TIME_ZONE = None

OUTPUT_DIR    = "hourly_counts" # folder where CSVs will be saved
# ----------------------------------------------------------------------


IDENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")  # simple Oracle identifier check


def assert_safe_identifier(name: str, kind: str):
    if not IDENT_RE.match(name):
        raise ValueError(f"Unsafe {kind} identifier: {name!r}. Use simple, unquoted names.")


def dsn(host: str, port: int, service: str) -> str:
    # Thin mode DSN format
    return f"{host}:{port}/{service}"


def parse_ts(s: str | None) -> dt.datetime | None:
    """
    Parse 'YYYYMMDD-HH:MM:SS.ffffff' into a Python datetime, or None.
    Example: '20251009-00:00:00.000000'
    """
    if not s:
        return None
    return dt.datetime.strptime(s, "%Y%m%d-%H:%M:%S.%f")


def find_tables_with_column(conn, schema: str, column: str) -> list[str]:
    """
    Returns table names in `schema` that contain `column`.
    Limits to real tables (excludes views) by using ALL_TABLES.
    """
    sql = """
        SELECT t.table_name
        FROM all_tables t
        WHERE t.owner = :owner
          AND EXISTS (
                SELECT 1
                FROM all_tab_columns c
                WHERE c.owner = t.owner
                  AND c.table_name = t.table_name
                  AND c.column_name = :col
          )
        ORDER BY t.table_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=schema, col=column)
        return [r[0] for r in cur.fetchall()]


def hourly_counts(conn, schema: str, table: str, time_col: str,
                  start_ts: dt.datetime | None, end_ts: dt.datetime | None):
    """
    Returns rows of (hour_start, row_count) for the given table.
    Groups by the start of the hour using TRUNC(CAST(time_col AS D_




No file chosenNo file chosen
ChatGPT can make mistakes. Check important info. See Cookie Preferences.
