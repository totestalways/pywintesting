#!/usr/bin/env python3
"""
Scan subfolders for JSON files (default: definition.json) and list entries whose {type_key} equals {type_value}.

How to use:
1) Edit the CONFIG dict below, then run the script.
   - root: folder that contains multiple subfolders with definition.json inside
   - filename: name of the json file to look for (default: "definition.json")
   - key_path: dot/bracket path inside the JSON where to start the search (e.g. "components.schemas" or "items[0].fields").
               If None, the entire JSON is searched.
   - type_key: key to check (default: "type")
   - type_value: value to match (default: "STRING")
   - ignore_case: case-insensitive comparison if True
   - out_json: path to write JSON Lines results (or None to skip)
   - out_csv: path to write CSV results (or None to skip)

2) Or import this file and call run(...) directly.
"""

import json
import os
import sys
import csv
from typing import Any, List, Tuple, Dict, Optional

# ---- EDIT YOUR INPUTS HERE ----
CONFIG = {
    "root": r"/path/to/root",
    "filename": "definition.json",
    "key_path": None,            # e.g., "components.schemas" or "data.items[0].fields"; None = search whole JSON
    "type_key": "type",
    "type_value": "STRING",
    "ignore_case": True,
    "out_json": None,            # e.g., r"/path/to/results.jsonl"
    "out_csv": None,             # e.g., r"/path/to/results.csv"
}
# --------------------------------

def get_by_path(data: Any, path: Optional[str]) -> Any:
    """
    Navigate a dot/bracket path inside a JSON-like structure.
    Supports tokens like: a.b.c, items[0].fields, components.schemas

    Returns None if the path cannot be resolved.
    """
    if not path:
        return data
    cur = data
    for part in path.split("."):
        if part == "":
            continue
        # split "key[idx][idx]..." pattern
        key = ""
        rest = part
        i = rest.find("[")
        if i == -1:
            key = rest
            rest = ""
        else:
            key = rest[:i]
            rest = rest[i:]
        if key:
            if not isinstance(cur, dict) or key not in cur:
                return None
            cur = cur[key]
        # handle [index] chains
        while rest:
            if not rest.startswith("["):
                return None
            j = rest.find("]")
            if j == -1:
                return None
            idx_str = rest[1:j]
            rest = rest[j+1:]
            try:
                idx = int(idx_str)
            except ValueError:
                return None
            if not isinstance(cur, list) or idx < 0 or idx >= len(cur):
                return None
            cur = cur[idx]
    return cur

def values_equal(a: Any, b: Any, ignore_case: bool) -> bool:
    if isinstance(a, str) and isinstance(b, str):
        return a.lower() == b.lower() if ignore_case else a == b
    return a == b

def find_type_entries(obj: Any, type_key: str, type_value: Any, ignore_case: bool, path: str = "$") -> List[Tuple[str, Dict[str, Any]]]:
    """
    Recursively search for dicts that have {type_key} == {type_value}.
    Returns list of tuples: (json_path, dict_obj)
    """
    results: List[Tuple[str, Dict[str, Any]]] = []
    if isinstance(obj, dict):
        if type_key in obj and values_equal(obj[type_key], type_value, ignore_case):
            results.append((path, obj))
        for k, v in obj.items():
            child_path = f"{path}.{k}" if path != "$" else f"$.{k}"
            results.extend(find_type_entries(v, type_key, type_value, ignore_case, child_path))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            child_path = f"{path}[{i}]"
            results.extend(find_type_entries(v, type_key, type_value, ignore_case, child_path))
    return results

def compact_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(obj)

def scan(root: str, filename: str, key_path: Optional[str], type_key: str, type_value: str, ignore_case: bool):
    aggregated = []
    for dirpath, _dirnames, filenames in os.walk(root):
        if filename in filenames:
            fpath = os.path.join(dirpath, filename)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"[WARN] Failed to load {fpath}: {e}", file=sys.stderr)
                continue

            container = get_by_path(data, key_path)
            if key_path is not None and container is None:
                print(f"[WARN] Key path '{key_path}' not found in {fpath}", file=sys.stderr)
                continue

            target = container if key_path else data
            start_path = "$" if not key_path else f"$.{key_path}"
            hits = find_type_entries(target, type_key, type_value, ignore_case, start_path)
            for json_path, obj in hits:
                aggregated.append({
                    "file": fpath,
                    "json_path": json_path,
                    "type_key": type_key,
                    "type_value": obj.get(type_key, None),
                    "match_equal": values_equal(obj.get(type_key), type_value, ignore_case),
                    "object": obj
                })
    return aggregated

def run(
    root: str,
    filename: str = "definition.json",
    key_path: Optional[str] = None,
    type_key: str = "type",
    type_value: str = "STRING",
    ignore_case: bool = True,
    out_json: Optional[str] = None,
    out_csv: Optional[str] = None,
):
    results = scan(root, filename, key_path, type_key, type_value, ignore_case)

    # Console summary
    if not results:
        print("No matches found.")
    else:
        print(f"Found {len(results)} matches:\n")
        for r in results:
            obj_preview = r["object"]
            preview_text = compact_json(obj_preview)
            if len(preview_text) > 300:
                preview_text = preview_text[:300] + "…"

            print(f"- File: {r['file']}")
            print(f"  Path: {r['json_path']}")
            print(f"  {type_key}={r['type_value']}  match={r['match_equal']}")
            print(f"  Object: {preview_text}\n")

    # Optional outputs
    if out_json:
        try:
            with open(out_json, "w", encoding="utf-8") as fo:
                for r in results:
                    fo.write(json.dumps(r, ensure_ascii=False) + "\n")
            print(f"Wrote JSONL to {out_json}")
        except Exception as e:
            print(f"[ERROR] Failed to write JSONL: {e}", file=sys.stderr)

    if out_csv:
        try:
            with open(out_csv, "w", newline="", encoding="utf-8") as fo:
                w = csv.writer(fo)
                w.writerow(["file", "json_path", "type_key", "type_value", "match_equal", "object_json"])
                for r in results:
                    w.writerow([
                        r["file"],
                        r["json_path"],
                        r["type_key"],
                        r["type_value"],
                        r["match_equal"],
                        compact_json(r["object"])
                    ])
            print(f"Wrote CSV to {out_csv}")
        except Exception as e:
            print(f"[ERROR] Failed to write CSV: {e}", file=sys.stderr)

    return results

if __name__ == "__main__":
    run(**CONFIG)
