LINE_RE = re.compile(
    r"""^
    (?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)   # timestamp
    \s+\[TARGET_APPLY\].*?                                                         # context
    Applied\s+record\s+\{?(?P<val>-?\d+)\}?\s+to\s+target\b.*$                     # int + trailing text
    """,
    re.VERBOSE
)
