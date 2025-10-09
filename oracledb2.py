#!/usr/bin/env python3
"""
Oracle hourly counts per table → separate CSVs + Top 10 summary.
Handles DATE/TIMESTAMP, NUMBER epoch, and VARCHAR time strings.

- Connects to Oracle (python-oracledb, thin mode).
- Finds all tables in a schema that contain a given time column.
- Normalizes the time column to TIMESTAMP (based on its data type) and:
    • counts rows grouped by hour → "<SCHEMA>_<TABLE>.csv" (hour_start,row_count)
    • builds "top10_tables_by_total_count.csv" ranked by total rows in window

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

SCHEMA        = "SALES"          # schema/owner to scan (unquoted identifier)
TIME_COLUMN   = "CREATED_AT"     # time column present in the target tables (unquoted)

# Time window (inclusive start, exclusive end) — set to None to scan all.
# FORMAT MUST BE: "YYYYMMDD-HH:MM:SS.ffffff"  (example: "20251009-00:00:00.000000")
START_TS_STR  = "20251001-00:00:00.000000"   # or None
END_TS_STR    = "20251009-00:00:00.000000"   # or None

# If the time column is NUMBER, pick the unit for epoch:
#   "seconds", "milliseconds", or "microseconds"
NUMBER_EPOCH_UNIT = "seconds"

# If the time column is VARCHAR2/CHAR, specify its Oracle format model.
# Defaults to your requested wire format.
VARCHAR_TIME_FORMAT = "YYYYMMDD-HH24:MI:SS.FF6"

# Optional: force a specific session time zone (e.g., "UTC", "Europe/Bucharest")
SESSION_TIME_ZONE = None

OUTPUT_DIR    = "hourly_counts"  # folder where CSVs will be saved
# ----------------------------------------------------------------------


IDENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")  # simple Oracle identifier check


def assert_safe_identifier(name: str, kind: str):
    if not IDENT_RE.match(name):
        raise ValueError(f"Unsafe {kind} identifier: {name!r}. Use simple, unquoted names.")


def dsn(host: str, port: int, service: str) -> str:
    return f"{host}:{port}/{service}"


def parse_ts(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    return dt.datetime.strptime(s, "%Y%m%d-%H:%M:%S.%f")


def fetch_time_col_datatype(conn, schema: str, table: str, column: str) -> dict | None:
    sql = """
        SELECT data_type, data_precision, data_scale, char_length
        FROM all_tab_columns
        WHERE owner = :owner AND table_name = :table AND column_name = :col
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=schema, table=table, col=column)
        row = cur.fetchone()
        if not row:
            return None
        return {
            "data_type": row[0],          # e.g., 'DATE', 'TIMESTAMP(6)', 'NUMBER', 'VARCHAR2'
            "data_precision": row[1],
            "data_scale": row[2],
            "char_length": row[3],
        }


def build_time_expr(data_type: str, time_col: str) -> str:
    """
    Return an Oracle SQL expression that converts `time_col` to TIMESTAMP for grouping/filtering.
    """
    dt_upper = data_type.upper()

    # DATE or TIMESTAMP* → cast to TIMESTAMP
    if dt_upper.startswith("DATE") or dt_upper.startswith("TIMESTAMP"):
        return f"CAST({time_col} AS TIMESTAMP)"

    # NUMBER (epoch) → convert based on configured unit
    if dt_upper.startswith("NUMBER") or dt_upper.startswith("FLOAT"):
        if NUMBER_EPOCH_UNIT == "seconds":
            factor = "1"
        elif NUMBER_EPOCH_UNIT == "milliseconds":
            factor = "1000"
        elif NUMBER_EPOCH_UNIT == "microseconds":
            factor = "1000000"
        else:
            raise ValueError(f"Unsupported NUMBER_EPOCH_UNIT: {NUMBER_EPOCH_UNIT}")

        # Convert epoch to TIMESTAMP via DATE + INTERVAL
        # DATE '1970-01-01' + NUMTODSINTERVAL(col / factor, 'SECOND')
        return (
            f"CAST((DATE '1970-01-01' + NUMTODSINTERVAL({time_col}/({factor}), 'SECOND')) AS TIMESTAMP)"
        )

    # VARCHAR/CHAR → parse with TO_TIMESTAMP using the configured format
    if "CHAR" in dt_upper:
        fmt = VARCHAR_TIME_FORMAT.replace("'", "''")  # escape quotes
        return f"TO_TIMESTAMP({time_col}, '{fmt}')"

    # Fallback: try casting to TIMESTAMP (may fail if incompatible)
    return f"CAST({time_col} AS TIMESTAMP)"


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
                            start_ts: dt.datetime | None, end_ts: dt.datetime | None):
    """
    Returns rows of (hour_start, row_count) for the given table by normalizing the time column to TIMESTAMP.
    """
    assert_safe_identifier(schema, "schema")
    assert_safe_identifier(table, "table")
    assert_safe_identifier(time_col, "column")

    # Get datatype and build a normalized TIMESTAMP expression
    info = fetch_time_col_datatype(conn, schema, table, time_col)
    if not info:
        raise RuntimeError(f"Column metadata not found for {schema}.{table}.{time_col}")
    time_expr = build_time_expr(info["data_type"], time_col)

    # Use TRUNC on TIMESTAMP to get hour starts
    sql = f"""
        SELECT TRUNC({time_expr}, 'HH') AS hour_start,
               COUNT(*) AS row_count
        FROM {schema}.{table}
        WHERE {time_expr} IS NOT NULL
    """

    binds = {}
    if start_ts is not None:
        sql += f" AND {time_expr} >= :start_ts"
        binds["start_ts"] = start_ts
    if end_ts is not None:
        sql += f" AND {time_expr} < :end_ts"
        binds["end_ts"] = end_ts

    sql += """
        GROUP BY TRUNC({expr}, 'HH')
        ORDER BY hour_start
    """.format(expr=time_expr)

    with conn.cursor() as cur:
        cur.arraysize = 1000
        cur.execute(sql, binds)
        return cur.fetchall()  # list of (TIMESTAMP, int)


def fmt_hour(value) -> str:
    # value should be datetime from Oracle; fallback if it's a date
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:00")
    return dt.datetime.combine(value, dt.time()).strftime("%Y-%m-%d %H:00")


def main():
    schema = SCHEMA.upper()
    time_col = TIME_COLUMN.upper()

    start_ts = parse_ts(START_TS_STR)
    end_ts   = parse_ts(END_TS_STR)

    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn(DB_HOST, DB_PORT, DB_SERVICE))
    except Exception as e:
        print(f"❌ Failed to connect: {e}", file=sys.stderr)
        sys.exit(2)

    if SESSION_TIME_ZONE:
        try:
            with conn.cursor() as cur:
                cur.execute("ALTER SESSION SET time_zone = :tz", tz=SESSION_TIME_ZONE)
        except Exception as e:
            print(f"⚠️ Could not set session time zone ({SESSION_TIME_ZONE}): {e}", file=sys.stderr)

    per_table_stats = []

    try:
        tables = find_tables_with_column(conn, schema, time_col)
        if not tables:
            print(f"ℹ️ No tables in schema {schema} contain column {time_col}.")
            return

        print(f"Found {len(tables)} table(s) with column {time_col}. Writing CSVs to {out_dir.resolve()}")

        for tbl in tables:
            try:
                rows = hourly_counts_for_table(conn, schema, tbl, time_col, start_ts, end_ts)
                csv_path = out_dir / f"{schema}_{tbl}.csv"

                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(["hour_start", "row_count"])
                    for hour_start, cnt in rows:
                        w.writerow([fmt_hour(hour_start), cnt])

                total_rows = sum(cnt for _, cnt in rows) if rows else 0
                hours_covered = len(rows)
                first_hour = fmt_hour(rows[0][0]) if rows else ""
                last_hour  = fmt_hour(rows[-1][0]) if rows else ""
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
