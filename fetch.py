#!/usr/bin/env python3
"""
fetch.py — the once-a-day job behind the dashboard.

Flow:
  1. Run ONE parameterized BigQuery scan (query.sql) with a hard byte cap.
  2. Compute severity, hub attribution, lane impact and z-scores locally (common.py).
  3. Maintain a rolling 60-day baseline (docs/history.json) for the outlier z-scores.
  4. Write docs/data.json — the file the dashboard reads.

Cost: maximum_bytes_billed guarantees the scan can never exceed the free tier; a
too-large query fails loudly instead of billing you.

Env / config:
  GCP_SA_KEY            service-account JSON (string). If unset, uses default creds (ADC).
  GOOGLE_CLOUD_PROJECT  billing/query project (else taken from the key).
  RUN_DATE             'YYYY-MM-DD' event day to score (default: yesterday, UTC).
  MENTION_WINDOW_DAYS   coverage-tail sweep (default 2).
  MAX_GB                byte cap in GB (default 30 -> ~0.03 TB, deep inside the 1 TB/mo free tier).
  ENRICH_TITLES        '1' to scrape real headlines for shown events (default off -> URL-slug titles).
  DRY_RUN              '1' to only print the query's estimated bytes and exit.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account

import common as C

ROOT = Path(__file__).resolve().parent.parent
HUBS_FILE = ROOT / "hubs.json"
LANES_FILE = ROOT / "lanes.json"
OUT_FILE = ROOT / "docs" / "data.json"
HISTORY_FILE = ROOT / "docs" / "history.json"
QUERY_FILE = Path(__file__).resolve().parent / "query.sql"

PARAMS = {
    "hub_radius_km": float(os.getenv("HUB_RADIUS_KM", 250)),
    "decay_km": float(os.getenv("DECAY_KM", 150)),
    "top_headlines_per_hub": int(os.getenv("TOP_HEADLINES_PER_HUB", 8)),
    "weights": dict(C.DEFAULT_WEIGHTS),
}
HISTORY_KEEP_DAYS = 60


def get_client() -> bigquery.Client:
    key = os.getenv("GCP_SA_KEY")
    if key:
        info = json.loads(key)
        creds = service_account.Credentials.from_service_account_info(info)
        project = os.getenv("GOOGLE_CLOUD_PROJECT") or info.get("project_id")
        return bigquery.Client(credentials=creds, project=project)
    return bigquery.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT"))


def run_query(client: bigquery.Client, run_date: str, window: int, max_gb: float, dry_run: bool):
    sql = QUERY_FILE.read_text()
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
            bigquery.ScalarQueryParameter("mention_window_days", "INT64", window),
        ],
        maximum_bytes_billed=int(max_gb * 1e9),
        labels={"app": "disruption-watch"},
        dry_run=dry_run,
        use_query_cache=True,
    )
    job = client.query(sql, job_config=cfg)
    if dry_run:
        gb = job.total_bytes_processed / 1e9
        print(f"[dry-run] this query would scan ~{gb:.2f} GB "
              f"({gb/1000*30:.3f} TB across 30 daily runs; free tier is 1 TB/mo).")
        return None
    return list(job.result())


def build_events(rows) -> list[dict]:
    events = []
    for i, r in enumerate(rows):
        lat, lon = r["event_latitude"], r["event_longitude"]
        if lat in (None, 0) and lon in (None, 0):
            continue
        gold = r["GoldsteinScale"]
        tone = r["avg_mention_tone"]
        ment = r["aviation_mention_count"]
        src = r["aviation_source_count"]
        fat = int(r["fatalities"] or 0)
        quad = r["QuadClass"]
        url = r["SOURCEURL"]
        events.append({
            "id": int(r["GlobalEventID"]),
            "severity": C.severity_score(gold, tone, ment, src, fat, quad, PARAMS["weights"]),
            "event_name": C.cameo_name(r["EventRootCode"]),
            "quad_class": int(quad) if quad is not None else None,
            "location": r["action_location_name"],
            "country": r["action_location_country"],
            "lat": float(lat), "lon": float(lon),
            "tone": round(float(tone), 2) if tone is not None else None,
            "fatalities": fat,
            "goldstein": float(gold) if gold is not None else None,
            "mentions": int(ment or 0),
            "sources": int(src or 0),
            "actor1": r["Actor1Name"],
            "actor2": r["Actor2Name"],
            "title": C.title_from_url(url),
            "url": url,
            "source": C.source_name(url),
        })
    return events


def shown_event_ids(events: list[dict], hubs: list[dict]) -> set:
    """Event ids that will actually appear in some hub's headline list — the only ones
    worth scraping real titles for (keeps optional enrichment fast and bounded)."""
    R = PARAMS["hub_radius_km"]
    N = PARAMS["top_headlines_per_hub"]
    ids = set()
    for h in hubs:
        near = sorted(
            (e for e in events if C.haversine_km(h["lat"], h["lon"], e["lat"], e["lon"]) <= R),
            key=lambda e: e["severity"], reverse=True)[:N]
        ids.update(e["id"] for e in near)
    return ids


def enrich_titles(events: list[dict], keep_ids: set) -> None:
    """Optional: fetch the real <title> for the shown events. Best-effort; falls back to
    the URL-slug title on any failure. Requires requests + beautifulsoup4 (+ optional
    deep-translator for non-English headlines)."""
    try:
        import requests
        from bs4 import BeautifulSoup
        from concurrent.futures import ThreadPoolExecutor, as_completed
    except ImportError:
        print("[enrich] requests/bs4 not installed — keeping URL-slug titles.")
        return
    try:
        from deep_translator import GoogleTranslator
        translate = GoogleTranslator(source="auto", target="en").translate
    except Exception:
        translate = None

    targets = [e for e in events if e["id"] in keep_ids and e["url"]]
    ua = {"User-Agent": "Mozilla/5.0 (compatible; DisruptionWatch/1.0)"}

    def fetch_one(e):
        try:
            resp = requests.get(e["url"], headers=ua, timeout=8)
            soup = BeautifulSoup(resp.content, "html.parser")
            t = (soup.title.string or "").strip() if soup.title else ""
            if C.is_garbage_title(t):
                return
            if translate and not t.isascii():
                try:
                    t = translate(t) or t
                except Exception:
                    pass
            e["title"] = t[:160]
        except Exception:
            pass  # keep slug title

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(fetch_one, e) for e in targets]
        for _ in as_completed(futs):
            pass
    print(f"[enrich] refreshed titles for up to {len(targets)} shown events.")


def load_history() -> dict:
    if HISTORY_FILE.exists():
        try:
            return json.loads(HISTORY_FILE.read_text())
        except Exception:
            return {}
    return {}


def apply_baseline(hub_records: list[dict], history: dict, run_date: str) -> None:
    idx_today = sorted(h["disruption_index"] for h in hub_records)
    n = len(idx_today)
    for h in hub_records:
        prior = [p["index"] for p in history.get(h["hub_code"], []) if p["date"] != run_date]
        z = C.zscore(h["disruption_index"], prior)
        q = (sum(1 for v in idx_today if v <= h["disruption_index"]) / n) if n else 0.0
        h["z_score"] = z
        h["alert_level"] = C.alert_level(h["events_within_radius"], z, q)
        trend = [{"date": p["date"], "index": p["index"]}
                 for p in history.get(h["hub_code"], []) if p["date"] != run_date][-13:]
        trend.append({"date": run_date, "index": h["disruption_index"]})
        h["trend"] = trend


def update_history(hub_records: list[dict], history: dict, run_date: str) -> dict:
    cutoff = (date.fromisoformat(run_date) - timedelta(days=HISTORY_KEEP_DAYS)).isoformat()
    for h in hub_records:
        series = [p for p in history.get(h["hub_code"], []) if p["date"] != run_date and p["date"] >= cutoff]
        series.append({"date": run_date, "index": h["disruption_index"]})
        history[h["hub_code"]] = series
    return history


def main():
    run_date = os.getenv("RUN_DATE") or (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    window = int(os.getenv("MENTION_WINDOW_DAYS", 2))
    max_gb = float(os.getenv("MAX_GB", 30))
    dry = os.getenv("DRY_RUN") == "1"

    hubs = C.load_json(HUBS_FILE)["hubs"]
    lanes_cfg = C.load_json(LANES_FILE)["lanes"]
    print(f"[run] date={run_date} window={window}d cap={max_gb}GB hubs={len(hubs)}")

    client = get_client()
    rows = run_query(client, run_date, window, max_gb, dry)
    if dry:
        return

    events = build_events(rows)
    print(f"[data] {len(events)} deduped aviation-covered disruption events")

    if os.getenv("ENRICH_TITLES") == "1":
        enrich_titles(events, shown_event_ids(events, hubs))

    hub_records = C.rollup_hubs(events, hubs, PARAMS["hub_radius_km"], PARAMS["decay_km"])
    history = load_history()
    apply_baseline(hub_records, history, run_date)
    update_history(hub_records, history, run_date)
    C.write_json(HISTORY_FILE, history)

    lanes = C.rollup_lanes(hub_records, lanes_cfg)
    payload = C.assemble_payload(run_date, hub_records, lanes, events, PARAMS, is_sample=False)
    C.write_json(OUT_FILE, payload)

    s = payload["stats"]
    print(f"[done] wrote {OUT_FILE.relative_to(ROOT)} | "
          f"{s['events_total']} events, {s['hubs_in_alert']} hubs in alert, peak={s['top_hub']}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[error] {type(e).__name__}: {e}", file=sys.stderr)
        raise
