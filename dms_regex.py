import re
from datetime import datetime, timezone

LINE_RE = re.compile(
    r"""^
    (?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)   # timestamp
    \s+\[TARGET_APPLY\].*?                                                         # context
    Applied\s+record\s+\{?(?P<val>-?\d+)\}?\s+to\s+target                          # int, braces optional
    """,
    re.VERBOSE
)

def parse_timestamp(ts: str) -> datetime:
    if ts.endswith('Z'):
        ts = ts[:-1] + '+00:00'
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def parse_line(line: str):
    m = LINE_RE.search(line)
    if not m:
        raise ValueError(f"Could not parse line:\n{line}")
    ts = parse_timestamp(m.group('ts'))
    val = int(m.group('val'))
    return ts, val

def diff_two_messages(msg1: str, msg2: str):
    ts1, v1 = parse_line(msg1)
    ts2, v2 = parse_line(msg2)

    delta = abs(ts2 - ts1)
    total_seconds = int(delta.total_seconds())
    minutes, seconds = divmod(total_seconds, 60)

    int_diff = v2 - v1

    return {
        "msg1": {"timestamp": ts1.isoformat(), "value": v1},
        "msg2": {"timestamp": ts2.isoformat(), "value": v2},
        "time_diff": {"minutes": minutes, "seconds": seconds, "total_seconds": total_seconds},
        "int_diff": {"diff": int_diff, "abs_diff": abs(int_diff)},
    }

if __name__ == "__main__":
    # Your example without braces:
    m1 = "2023-02-22T11:26:07 [TARGET_APPLY] I: Applied record 123 to target"
    # Works with or without braces; this one shows braces are ignored:
    m2 = "2023-02-22T11:29:10 [TARGET_APPLY] I: Applied record {200} to target"

    from pprint import pprint
    pprint(diff_two_messages(m1, m2))
