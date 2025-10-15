# file: max_rowscn_timestamp_to_csv.py
import re
import pandas as pd

# --- CONFIG: fill these in ---
ORACLE_DSN = "host:1521/service_name"   # e.g. "db.mycorp.com:1521/ORCLPDB1"
ORACLE_USER = "YOUR_USER"
ORACLE_PASS = "YOUR_PASSWORD"
SCHEMA      = "YOUR_SCHEMA"             # e.g. "MY_SCHEMA"
TABLES      = [
    "TABLE_A",
    "TABLE_B",
    # add more...
]
OUTPUT_CSV  = "max_rowscn_timestamp.csv"
# --- end config ---

# Prefer the modern 'oracledb' driver. Falls back to old 'cx_Oracle' if present.
try:
    import oracledb as oracle
except ImportError:
    import cx_Oracle as oracle  # type: ignore

_VALID_IDENT = re.compile(r"^[A-Z][A-Z0-9_]*$")

def q_ident(name: str) -> str:
    """
    Very simple identifier guard: only allow unquoted UPPERCASE [A-Z][A-Z0-9_]*.
    If you need mixed case or special chars, adjust as needed.
    """
    if not _VALID_IDENT.match(name):
        raise ValueError(f"Invalid identifier: {name!r}. Use UPPERCASE A-Z, 0-9, _ only.")
    return name

def main():
    rows = []
    with oracle.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN) as conn:
        with conn.cursor() as cur:
            q_schema = q_ident(SCHEMA)
            for t in TABLES:
                try:
                    q_table = q_ident(t)
                    sql = f"SELECT MAX(SCN_TO_TIMESTAMP(ORA_ROWSCN)) FROM {q_schema}.{q_table}"
                    cur.execute(sql)
                    (result,) = cur.fetchone()
                    # result is a datetime or None
                    rows.append({
                        "TABLE NAME": f"{q_schema}.{q_table}",
                        "QUERY RESULT": None if result is None else result,
                    })
                except Exception as e:
                    # Capture the error string as the result so it still appears in the CSV
                    rows.append({
                        "TABLE NAME": f"{SCHEMA}.{t}",
                        "QUERY RESULT": f"ERROR: {e}",
                    })

    df = pd.DataFrame(rows, columns=["TABLE NAME", "QUERY RESULT"])
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {len(df)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
