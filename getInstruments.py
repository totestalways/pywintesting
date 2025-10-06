# extract_by_filename.py
"""
For each Excel file in INPUT_DIR:
  - Read the sheet(s)
  - Find the 'InstrumentId' column (case-insensitive)
  - Keep values that do NOT contain '{'
  - Keep only rows whose InstrumentId matches the Excel file's base name
  - Write a per-file output with only those rows (default: CSV)

Examples:
  input/
    AAPL.xlsx   -> output/AAPL.csv contains rows where InstrumentId == 'AAPL'
    MSFT.xlsx   -> output/MSFT.csv contains rows where InstrumentId == 'MSFT'

Requires:
  pip install pandas openpyxl
  (For legacy .xls files: pip install "xlrd<2.0")
"""

from __future__ import annotations
import os
import re
import sys
from pathlib import Path
from typing import Optional, Iterable, List

import pandas as pd

# -------------------- CONFIG --------------------
INPUT_DIR: str = "input"                 # folder containing Excel files
OUTPUT_DIR: str = "output"               # folder for generated files

# Excel reading
SHEET_NAME: Optional[str] = None         # None=first sheet; or set a sheet name
READ_ALL_SHEETS: bool = False            # True = read & concatenate all sheets

# Column logic
COLUMN_NAME: str = "InstrumentId"        # column to filter on
DEDUPLICATE: bool = True                 # drop duplicate rows (by all cols if KEEP_ALL_COLUMNS, else by InstrumentId)
KEEP_ALL_COLUMNS: bool = False           # True=write entire row; False=write only the InstrumentId column

# Matching logic (Excel filename -> target instrument)
CASE_SENSITIVE: bool = True              # set False for case-insensitive match
NORMALIZE_NAME: bool = False             # if True, strip spaces/underscores/dashes before comparing

# Output format
OUTPUT_FORMAT: str = "csv"               # "csv" or "xlsx"
SKIP_EMPTY_FILES: bool = True            # if True, don't write a file when no rows matched

# File patterns to consider as Excel
FILE_PATTERNS: List[str] = ["*.xlsx", "*.xlsm", "*.xls"]  # (exclude xlsb unless you have an engine)
# ------------------------------------------------


def _iter_excel_files(folder: Path, patterns: Iterable[str]) -> Iterable[Path]:
    seen = set()
    for pat in patterns:
        for p in folder.glob(pat):
            # skip temp Excel files like "~$foo.xlsx"
            if p.name.startswith("~$"):
                continue
            if p.is_file() and p.resolve() not in seen:
                seen.add(p.resolve())
                yield p


def _find_column_case_insensitive(df: pd.DataFrame, target: str) -> str:
    lower_map = {c.lower(): c for c in df.columns}
    key = target.lower()
    if key in lower_map:
        return lower_map[key]
    # forgiving: strip spaces/underscores
    norm = lambda s: re.sub(r"[\s_]+", "", s.lower())
    normalized = {norm(c): c for c in df.columns}
    key2 = norm(target)
    if key2 in normalized:
        return normalized[key2]
    raise KeyError(
        f"Column '{target}' not found. Available columns: {', '.join(map(str, df.columns))}"
    )


def _normalize_token(s: str) -> str:
    """Optional normalization before comparing names."""
    if NORMALIZE_NAME:
        s = re.sub(r"[\s_\-]+", "", s)
    return s


def _equals(a: str, b: str) -> bool:
    if not CASE_SENSITIVE:
        a = a.casefold()
        b = b.casefold()
    return _normalize_token(a) == _normalize_token(b)


def _read_excel(path: Path, sheet_name: Optional[str], read_all: bool) -> pd.DataFrame:
    if read_all:
        # Concatenate all sheets (align columns by name)
        all_sheets = pd.read_excel(path, sheet_name=None, dtype=object)
        return pd.concat(all_sheets.values(), ignore_index=True, sort=False)
    return pd.read_excel(path, sheet_name=sheet_name, dtype=object)


def _write_output(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_FORMAT.lower() == "xlsx":
        df.to_excel(out_path.with_suffix(".xlsx"), index=False)
    else:
        df.to_csv(out_path.with_suffix(".csv"), index=False)


def main() -> None:
    in_dir = Path(INPUT_DIR)
    if not in_dir.exists() or not in_dir.is_dir():
        sys.exit(f"INPUT_DIR '{INPUT_DIR}' does not exist or is not a directory.")

    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = list(_iter_excel_files(in_dir, FILE_PATTERNS))
    if not files:
        sys.exit(f"No Excel files found in '{INPUT_DIR}' (patterns: {', '.join(FILE_PATTERNS)}).")

    processed = written = skipped = 0

    for xls_path in sorted(files):
        processed += 1
        base_name = xls_path.stem  # filename without extension
        try:
            df = _read_excel(xls_path, SHEET_NAME, READ_ALL_SHEETS)
            if df.empty:
                print(f"[SKIP empty] {xls_path.name}")
                skipped += 1
                continue

            col = _find_column_case_insensitive(df, COLUMN_NAME)
            ser = df[col].astype(str).str.strip()

            # 1) Not empty & no '{'
            mask_valid = ser.ne("") & ~ser.str.contains(r"\{", regex=True, na=False)
            # 2) Match filename
            mask_match = ser.apply(lambda v: _equals(v, base_name))

            mask = mask_valid & mask_match
            if not mask.any():
                print(f"[NO MATCH] {xls_path.name} -> 0 rows for InstrumentId == '{base_name}'")
                if SKIP_EMPTY_FILES:
                    skipped += 1
                    continue

            if KEEP_ALL_COLUMNS:
                out_df = df.loc[mask].copy()
                if DEDUPLICATE:
                    out_df = out_df.drop_duplicates(keep="first")
            else:
                filtered = ser[mask]
                if DEDUPLICATE:
                    filtered = filtered.drop_duplicates(keep="first")
                out_df = pd.DataFrame({col: filtered})

            out_file = out_dir / base_name
            _write_output(out_df, out_file)
            print(f"[WROTE] {xls_path.name} -> {out_file.with_suffix('.' + OUTPUT_FORMAT).name} ({len(out_df)} rows)")
            written += 1

        except Exception as e:
            print(f"[ERROR] {xls_path.name}: {e}")
            skipped += 1

    print(f"\nSummary: processed={processed}, written={written}, skipped={skipped}")
    print(
        f"Settings: SHEET_NAME={SHEET_NAME!r}, READ_ALL_SHEETS={READ_ALL_SHEETS}, "
        f"CASE_SENSITIVE={CASE_SENSITIVE}, NORMALIZE_NAME={NORMALIZE_NAME}, "
        f"KEEP_ALL_COLUMNS={KEEP_ALL_COLUMNS}, DEDUPLICATE={DEDUPLICATE}, "
        f"OUTPUT_FORMAT='{OUTPUT_FORMAT}'"
    )


if __name__ == "__main__":
    main()
