#!/usr/bin/env python3
"""
Run a user-defined numeric query on every table in a schema and export results to CSV.

Requirements:
  pip install oracledb

Notes:
- Write your query in USER_QUERY_TEMPLATE and include the placeholder {table}
  which will be replaced with the fully-qualified identifier "SCHEMA"."TABLE".
- The query must return exactly ONE row with ONE numeric value (e.g., COUNT(*)).
- Example query templates:
    'SELECT COUNT(*) FROM {table}'
    'SELECT NVL(MAX(ID), 0) FROM {table}'
    'SELECT COUNT(*) FROM {table} WHERE TIME_COL >= DATE ''2025-10-01'''
"""

import csv
import sys
import getpass
import oracledb
from typing import List, Tuple

# ======================= USER SETTINGS =======================

HOST = "db-host.example.com"
PORT = 1521
SERVICE_NAME = "ORCLPDB1"   # or use SID if needed with a different DSN form
USERNAME = "YOUR_SCHEMA_USER"
PASSWORD = None             # set to None to be prompted securely

SCHEMA = "YOUR_SCHEMA"      # schema whose tables you want to scan (owner)
OUTPUT_CSV = "table_query_results.csv"

# The query to run for each table. MUST return a single numeric value.
# Use {table} exactly once to be replaced by the fully-qualified name.
USER_QUERY_TEMPLATE = "SELECT COUNT(*) FROM {table}"

# Optional: filter tables by a pattern. Leave as None to include all tables.
# Examples: 'EMP%', '%LOG', 'ORDERS' (exact if no wildcard)
TABLE_NAME_LIKE = None

# =============================================================

def quote_ident(name: str) -> str:
    """
    Safely double-quote an Oracle identifier. Assumes input is a single identifier
    without schema separators. Replaces any embedded double quotes by doubling them.
    """
    if name is None:
        raise ValueError("Identifier cannot be None")
    return '"' + name.replace('"', '""') + '"'

def make_qualified(owner: str, table: str) -> str:
    """
    Produce a fully qualified identifier: "OWNER"."TABLE"
    """
    return f"{quote_ident(owner)}.{quote_ident(table)}"

def get_connection() -> oracledb.Connection:
    global PASSWORD
    if PASSWORD is None:
        PASSWORD = getpass.getpass(f"Password for {USERNAME}@{HOST}:{PORT}/{SERVICE_NAME}: ")
    dsn = oracledb.makedsn(HOST, PORT, service_name=SERVICE_NAME)
    return oracledb.connect(user=USERNAME, password=PASSWORD, dsn=dsn)

def fetch_tables(conn: oracledb.Connection, owner: str, name_like: str = None) -> List[str]:
    """
    Return a list of table names (strings) in ALL_TABLES for the given owner (schema).
    Requires SELECT privilege on ALL_TABLES (usually granted).
    """
    sql = """
        SELECT table_name
          FROM all_tables
         WHERE owner = :owner
    """
    binds = {"owner": owner.upper()}
    if name_like:
        sql += " AND table_name LIKE :name_like ESCAPE '\\' "
        # Escape % and _ in literal patterns if the user didn’t intend wildcards.
        binds["name_like"] = name_like

    sql += " ORDER BY table_name"
    with conn.cursor() as cur:
        cur.execute(sql, binds)
        return [r[0] for r in cur.fetchall()]

def run_query_on_table(conn: oracledb.Connection, owner: str, table: str, query_template: str) -> Tuple[bool, float, str]:
    """
    Execute the user query for a single table.
    Returns (ok, value, error_message). On success ok=True and value is float (or int).
    On failure ok=False and error_message is populated.
    """
    qualified = make_qualified(owner, table)
    # Replace {table} with the properly quoted qualified name.
    sql = query_template.format(table=qualified)

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if row is None or len(row) == 0:
                return (False, 0.0, "Query returned no rows.")
            if len(row) > 1:
                return (False, 0.0, "Query returned multiple columns; expected exactly one.")
            val = row[0]
            try:
                # Convert to float just to be safe for CSV serialization
                num = float(val)
            except Exception:
                return (False, 0.0, f"Result is not numeric: {val!r}")
            return (True, num, "")
    except Exception as e:
        return (False, 0.0, f"{type(e).__name__}: {e}")

def main():
    print(f"Connecting to Oracle at {HOST}:{PORT}/{SERVICE_NAME} as {USERNAME} ...")
    try:
        conn = get_connection()
    except Exception as e:
        print(f"Failed to connect: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        tables = fetch_tables(conn, SCHEMA, TABLE_NAME_LIKE)
    except Exception as e:
        print(f"Failed to list tables from schema {SCHEMA}: {e}", file=sys.stderr)
        sys.exit(3)

    if not tables:
        print(f"No tables found in schema {SCHEMA}.")
        sys.exit(0)

    print(f"Found {len(tables)} table(s) in schema {SCHEMA}. Running query on each...")

    results = []  # list of (table_name, value or None, error or '')
    for t in tables:
        ok, value, err = run_query_on_table(conn, SCHEMA, t, USER_QUERY_TEMPLATE)
        if ok:
            print(f"  ✓ {t}: {value}")
            results.append((t, value, ""))
        else:
            print(f"  ✗ {t}: ERROR - {err}")
            results.append((t, None, err))

    # Write CSV
    # If an error occurred for a table, QUERY_RESULT will be blank and ERROR will contain text.
    out_path = OUTPUT_CSV
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["TABLE_NAME", "QUERY_RESULT", "ERROR"])
        for t, val, err in results:
            writer.writerow([t, "" if val is None else val, err])

    print(f"\nDone. Wrote results to {out_path}")
    print("CSV columns: TABLE_NAME, QUERY_RESULT, ERROR")

if __name__ == "__main__":
    main()
