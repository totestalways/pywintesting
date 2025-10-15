#!/usr/bin/env python3
"""
Run a user-defined query on every (or selected) tables in a schema and export results to CSV.

Requirements:
  pip install oracledb

Notes:
- Write your query in USER_QUERY_TEMPLATE and include the placeholder {table}
  which will be replaced with the fully-qualified identifier "SCHEMA"."TABLE".
- The query must return exactly ONE row with ONE value (number OR string).
- Examples:
    'SELECT COUNT(*) FROM {table}'
    'SELECT NVL(MAX(ID), 0) FROM {table}'
    'SELECT TO_CHAR(MAX(UPDATED_AT), ''YYYY-MM-DD HH24:MI:SS'') FROM {table}'
"""

import csv
import sys
import getpass
import oracledb
from typing import List, Tuple, Optional

# ======================= USER SETTINGS =======================

HOST = "db-host.example.com"
PORT = 1521
SERVICE_NAME = "ORCLPDB1"
USERNAME = "YOUR_SCHEMA_USER"
PASSWORD = None  # set to None to be prompted securely

SCHEMA = "YOUR_SCHEMA"      # owner whose tables you want to scan (case-insensitive)
OUTPUT_CSV = "table_query_results.csv"

# The query to run for each table. MUST return a single row with one column (scalar).
# Use {table} exactly once to be replaced by the fully-qualified name.
USER_QUERY_TEMPLATE = "SELECT COUNT(*) FROM {table}"

# Option A: filter discovered tables with LIKE (optional). Example: 'EMP%', '%LOG'
TABLE_NAME_LIKE: Optional[str] = None

# Option B: explicitly provide a list of table names to query (skips discovery).
# Leave as [] to query all discovered tables (optionally filtered by TABLE_NAME_LIKE).
# Names are compared case-insensitively; they will be quoted in SQL.
TABLES_INCLUDE: List[str] = []  # e.g. ['ORDERS', 'CUSTOMERS', 'AUDIT_LOG']

# =============================================================

def quote_ident(name: str) -> str:
    """Safely double-quote an Oracle identifier."""
    if name is None:
        raise ValueError("Identifier cannot be None")
    return '"' + name.replace('"', '""') + '"'

def make_qualified(owner: str, table: str) -> str:
    """Produce a fully qualified identifier: "OWNER"."TABLE"."""
    return f"{quote_ident(owner)}.{quote_ident(table)}"

def get_connection() -> oracledb.Connection:
    global PASSWORD
    if PASSWORD is None:
        PASSWORD = getpass.getpass(f"Password for {USERNAME}@{HOST}:{PORT}/{SERVICE_NAME}: ")
    dsn = oracledb.makedsn(HOST, PORT, service_name=SERVICE_NAME)
    return oracledb.connect(user=USERNAME, password=PASSWORD, dsn=dsn)

def fetch_tables(conn: oracledb.Connection, owner: str, name_like: Optional[str] = None) -> List[str]:
    """
    Return a list of table names in ALL_TABLES for the given owner.
    """
    sql = """
        SELECT table_name
          FROM all_tables
         WHERE owner = :owner
    """
    binds = {"owner": owner.upper()}
    if name_like:
        sql += " AND table_name LIKE :name_like ESCAPE '\\' "
        binds["name_like"] = name_like
    sql += " ORDER BY table_name"
    with conn.cursor() as cur:
        cur.execute(sql, binds)
        return [r[0] for r in cur.fetchall()]

def validate_tables_exist(conn: oracledb.Connection, owner: str, tables_wanted: List[str]) -> Tuple[List[str], List[str]]:
    """
    Return (existing_tables, missing_tables) for the desired list (case-insensitive).
    """
    if not tables_wanted:
        return ([], [])
    wanted_upper = [t.upper() for t in tables_wanted]
    existing = fetch_tables(conn, owner)
    existing_set = set(existing)  # already upper-case from data dictionary
    found, missing = [], []
    for t in wanted_upper:
        if t in existing_set:
            found.append(t)
        else:
            missing.append(t)
    return (found, missing)

def run_query_on_table(conn: oracledb.Connection, owner: str, table: str, query_template: str) -> Tuple[bool, Optional[str], str]:
    """
    Execute the user query for a single table.
    Returns (ok, value_as_text, error_message).
    - On success: ok=True, value_as_text is str (number/string converted to text).
    - On failure: ok=False, error_message populated.
    """
    qualified = make_qualified(owner, table)
    sql = query_template.format(table=qualified)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if row is None or len(row) == 0:
                return (False, None, "Query returned no rows.")
            if len(row) > 1:
                return (False, None, "Query returned multiple columns; expected exactly one.")
            val = row[0]
            # Convert any scalar to text for CSV (numbers, dates, strings)
            return (True, "" if val is None else str(val), "")
    except Exception as e:
        return (False, None, f"{type(e).__name__}: {e}")

def main():
    print(f"Connecting to Oracle at {HOST}:{PORT}/{SERVICE_NAME} as {USERNAME} ...")
    try:
        conn = get_connection()
    except Exception as e:
        print(f"Failed to connect: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        # Decide the table list
        if TABLES_INCLUDE:
            found, missing = validate_tables_exist(conn, SCHEMA, TABLES_INCLUDE)
            if missing:
                print("Warning: The following requested tables were not found in schema "
                      f"{SCHEMA}: {', '.join(missing)}", file=sys.stderr)
            tables = found
        else:
            tables = fetch_tables(conn, SCHEMA, TABLE_NAME_LIKE)

    except Exception as e:
        print(f"Failed to obtain tables for schema {SCHEMA}: {e}", file=sys.stderr)
        sys.exit(3)

    if not tables:
        print(f"No tables to process in schema {SCHEMA}.")
        sys.exit(0)

    print(f"Processing {len(tables)} table(s) in schema {SCHEMA}...")
    results = []  # (table_name, value_text or "", error or "")
    for t in tables:
        ok, value_text, err = run_query_on_table(conn, SCHEMA, t, USER_QUERY_TEMPLATE)
        if ok:
            print(f"  ✓ {t}: {value_text}")
            results.append((t, value_text or "", ""))
        else:
            print(f"  ✗ {t}: ERROR - {err}")
            results.append((t, "", err))

    # Write CSV
    out_path = OUTPUT_CSV
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["TABLE_NAME", "QUERY_RESULT", "ERROR"])
        for t, val, err in results:
            writer.writerow([t, val, err])

    print(f"\nDone. Wrote results to {out_path}")
    print("CSV columns: TABLE_NAME, QUERY_RESULT, ERROR")

if __name__ == "__main__":
    main()
