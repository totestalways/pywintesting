# snapshot_dashboard_hardcoded.py
# pip install datadog-api-client==2.* requests

import re, time, pathlib, requests
from datetime import datetime, timedelta, timezone
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v1.api.dashboards_api import DashboardsApi
from datadog_api_client.v1.api.snapshots_api import SnapshotsApi

# ==== EDIT THESE ====
DASHBOARD_ID = "abc-def-123"        # your dashboard id
WINDOW_SECONDS = 6 * 3600           # last 6h
DD_SITE = "datadoghq.eu"            # e.g. datadoghq.com, datadoghq.eu, us3.datadoghq.com
DD_API_KEY = "PUT_YOUR_API_KEY_HERE"
DD_APP_KEY = "PUT_YOUR_APP_KEY_HERE"
OUT_DIR = "dd_snapshots_6h"
IMG_WIDTH = 1200
IMG_HEIGHT = 600
# =====================

def sanitize(name: str) -> str:
    return re.sub(r"[^\w\-]+", "_", (name or "untitled"))[:80]

def extract_queries(widget) -> list[str]:
    d = getattr(widget, "definition", None)
    reqs = getattr(d, "requests", None)
    if not reqs:
        return []
    out = []
    for r in reqs:
        q = getattr(r, "q", None)
        if isinstance(q, str) and q.strip():
            out.append(q.strip())
        elif isinstance(q, list):
            out.extend([qq for qq in q if isinstance(qq, str) and qq.strip()])
    # de-dup preserving order
    seen = set(); dedup = []
    for q in out:
        if q not in seen:
            seen.add(q); dedup.append(q)
    return dedup

def main():
    end = int(datetime.now(timezone.utc).timestamp())
    start = end - int(WINDOW_SECONDS)

    out_dir = pathlib.Path(OUT_DIR); out_dir.mkdir(parents=True, exist_ok=True)

    cfg = Configuration()
    # Inject site + creds directly (bypasses env vars)
    cfg.server_variables["site"] = DD_SITE
    cfg.api_key["apiKeyAuth"] = DD_API_KEY
    cfg.api_key["appKeyAuth"] = DD_APP_KEY

    with ApiClient(cfg) as api:
        dash_api = DashboardsApi(api)
        snap_api = SnapshotsApi(api)

        dash = dash_api.get_dashboard(DASHBOARD_ID)
        widgets = dash.widgets or []

        saved = 0
        for i, w in enumerate(widgets, 1):
            title = sanitize(getattr(getattr(w, "definition", None), "title", None))
            queries = extract_queries(w)
            if not queries:
                continue  # skip non-metric widgets

            for j, q in enumerate(queries, 1):
                resp = snap_api.get_graph_snapshot(
                    metric_query=q,
                    start=start,
                    end=end,
                    title=title,
                    width=IMG_WIDTH,
                    height=IMG_HEIGHT,
                )
                url = resp.snapshot_url
                # Poll for readiness
                for _ in range(15):
                    r = requests.get(url, timeout=30)
                    if r.status_code == 200 and r.headers.get("Content-Type","").startswith("image/"):
                        fn = out_dir / f"{i:02d}_{j:02d}_{title}.png"
                        fn.write_bytes(r.content)
                        print("saved", fn)
                        saved += 1
                        break
                    time.sleep(2)

        print(f"Done. Saved {saved} image(s) to {out_dir}")

if __name__ == "__main__":
    main()
