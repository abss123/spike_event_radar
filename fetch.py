#!/usr/bin/env python3
"""
fetch.py — the once-a-day job behind the dashboard.

Flow:
  1. Run ONE parameterized BigQuery scan (query.sql) per day needed: the anchor
     (RUN_DATE) plus every day in the baseline window.
  2. Compute severity + hub attribution locally (common.py) for each of those days,
     zero-filling any (hub x day) with no nearby events to 0 — never a missing row.
  3. Measure the anchor against that freshly-scored, gap-free baseline window (mean,
     sample stddev, z, median, p25/p75) instead of the accumulated docs/history.json.
  4. Write docs/data.json — the file the dashboard reads. docs/history.json is still
     updated as a non-authoritative cache; nothing statistical reads it.

Cost: maximum_bytes_billed guarantees every one of those per-day scans can never exceed
the free tier; a too-large query fails loudly instead of billing you. Because the
baseline now issues one query per baseline day (BASELINE_DAYS of them, 21 by default)
instead of one, run DRY_RUN=1 to see the *combined* estimate before a real run.

Env / config:
  GOOGLE_CLOUD_PROJECT  query project. Optional — defaults to whatever project your
                        Application Default Credentials are scoped to.
  RUN_DATE              'YYYY-MM-DD' event day to score (default: yesterday, UTC).
  MENTION_WINDOW_DAYS   coverage-tail sweep, applied to EVERY day queried (default 2).
  MAX_GB                per-query byte cap in GB (default 30, deep inside the 1 TB/mo free tier).
  ENRICH_TITLES         '1' to scrape real headlines for shown events (default off -> URL-slug titles).
  DRY_RUN                '1' to print the estimated bytes for every query this run would
                        issue (anchor + baseline window) and exit — no query is billed.
  BASELINE_DAYS          size of the baseline window (default 21).
  EXCLUDE_ANCHOR          '1'/'true' to exclude RUN_DATE from its own baseline (default true).
  BASELINE_MODE          'rolling' (trailing window ending at/near the anchor, default) or
                        'fixed' (a fixed pre-shock window, see FIXED_BASELINE_END).
  FIXED_BASELINE_END     last day of the fixed baseline window when BASELINE_MODE=fixed
                        (default '2026-02-24').

Auth: uses Application Default Credentials — `gcloud auth application-default login`
or GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json locally; google-github-actions/auth
in CI (see .github/workflows/daily.yml). Works identically in both places.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from google.cloud import bigquery

import common as C

ROOT = Path(__file__).resolve().parent
HUBS_FILE = ROOT / "hubs.json"
LANES_FILE = ROOT / "lanes.json"
OUT_FILE = ROOT / "docs" / "data.json"
HISTORY_FILE = ROOT / "docs" / "history.json"
QUERY_FILE = ROOT / "query.sql"


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() not in ("0", "false", "no", "")


PARAMS = {
    "hub_radius_km": float(os.getenv("HUB_RADIUS_KM", 250)),
    "decay_km": float(os.getenv("DECAY_KM", 150)),
    "top_headlines_per_hub": int(os.getenv("TOP_HEADLINES_PER_HUB", 8)),
    "baseline_days": int(os.getenv("BASELINE_DAYS", 21)),
    "exclude_anchor": _env_bool("EXCLUDE_ANCHOR", True),
    "baseline_mode": os.getenv("BASELINE_MODE", "rolling"),
    "fixed_baseline_end": os.getenv("FIXED_BASELINE_END", "2026-02-24"),
    "weights": dict(C.DEFAULT_WEIGHTS),
}
HISTORY_KEEP_DAYS = 21


def get_client() -> bigquery.Client:
    """Application Default Credentials only — project=None falls back to whatever
    project the credentials are scoped to, so GOOGLE_CLOUD_PROJECT is optional."""
    return bigquery.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT"))


def run_query(client: bigquery.Client, run_date: str, window: int, max_gb: float, dry_run: bool):
    """dry_run=True costs nothing (BigQuery dry runs are metadata-only) and returns the
    estimated GB instead of rows — used both for the DRY_RUN=1 CLI flag and to size the
    per-day baseline queries before any of them actually bill bytes."""
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
        return job.total_bytes_processed / 1e9
    return list(job.result())


def score_day(client: bigquery.Client, day: str, window: int, max_gb: float,
              hubs: list[dict]) -> list[dict]:
    """Query + score one calendar day, then attribute to every hub (zero-filled — a hub
    with no nearby events still gets a 0.0 disruption_index row)."""
    rows = run_query(client, day, window, max_gb, dry_run=False)
    events = build_events(rows)
    return C.rollup_hubs(events, hubs, PARAMS["hub_radius_km"], PARAMS["decay_km"])


def baseline_window(anchor: str, baseline_days: int, exclude_anchor: bool,
                     mode: str, fixed_end: str) -> list[str]:
    """The list of calendar days (ISO strings, ascending) making up the baseline window.
    Mirrors the reference SQL's window math exactly:
      rolling + exclude_anchor : [anchor - baseline_days, anchor - 1]
      rolling + include_anchor : [anchor - baseline_days + 1, anchor]
      fixed                    : [fixed_end - baseline_days + 1, fixed_end]
    """
    a = date.fromisoformat(anchor)
    if mode == "fixed":
        end = date.fromisoformat(fixed_end)
        start = end - timedelta(days=baseline_days - 1)
    elif exclude_anchor:
        start = a - timedelta(days=baseline_days)
        end = a - timedelta(days=1)
    else:
        start = a - timedelta(days=baseline_days - 1)
        end = a
    n = (end - start).days + 1
    return [(start + timedelta(days=i)).isoformat() for i in range(n)]


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


def build_baseline_panel(client: bigquery.Client, run_date: str, anchor_records: list[dict],
                          window: int, max_gb: float, hubs: list[dict]) -> dict:
    """Score every day in the configured baseline window (reusing the already-scored
    anchor day when it falls inside that window) and return {hub_code: {date: index}} —
    every hub has exactly one zero-filled entry per day queried, including the anchor."""
    win_dates = baseline_window(run_date, PARAMS["baseline_days"], PARAMS["exclude_anchor"],
                                 PARAMS["baseline_mode"], PARAMS["fixed_baseline_end"])
    panel: dict[str, dict[str, float]] = {h["hub_code"]: {} for h in hubs}

    to_query = list(win_dates)
    if run_date in to_query:
        for h in anchor_records:
            panel[h["hub_code"]][run_date] = h["disruption_index"]
        to_query = [d for d in to_query if d != run_date]

    for d in to_query:
        for h in score_day(client, d, window, max_gb, hubs):
            panel[h["hub_code"]][d] = h["disruption_index"]

    for h in anchor_records:
        panel[h["hub_code"]].setdefault(run_date, h["disruption_index"])

    return {"window_dates": win_dates, "series": panel}


def apply_baseline(hub_records: list[dict], panel: dict, run_date: str) -> None:
    win_dates = panel["window_dates"]
    series = panel["series"]
    for h in hub_records:
        code = h["hub_code"]
        hist_vals = [series[code][d] for d in win_dates]  # exactly baseline_days entries
        stats = C.baseline_stats(h["disruption_index"], hist_vals)
        h.update(stats)
        h["alert_level"] = C.alert_level(h["events_within_radius"], stats["z_score"])
        trend_days = sorted(series[code].items())[-14:]  # last 14 days incl. anchor
        h["trend"] = [{"date": d, "index": v} for d, v in trend_days]


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
    win_dates = baseline_window(run_date, PARAMS["baseline_days"], PARAMS["exclude_anchor"],
                                 PARAMS["baseline_mode"], PARAMS["fixed_baseline_end"])
    query_dates = sorted(set(win_dates) | {run_date})
    print(f"[run] date={run_date} window={window}d cap={max_gb}GB/query hubs={len(hubs)} "
          f"baseline_mode={PARAMS['baseline_mode']} baseline_days={PARAMS['baseline_days']} "
          f"exclude_anchor={PARAMS['exclude_anchor']} baseline_window=[{win_dates[0]}..{win_dates[-1]}] "
          f"queries={len(query_dates)}")

    client = get_client()

    if dry:
        per_day_gb = {d: run_query(client, d, window, max_gb, dry_run=True) for d in query_dates}
        total_gb = sum(per_day_gb.values())
        print(f"[dry-run] anchor ({run_date}) ~{per_day_gb[run_date]:.2f} GB")
        print(f"[dry-run] {len(query_dates)} queries this run (anchor + {len(win_dates)} baseline "
              f"days) ~{total_gb:.2f} GB combined "
              f"(~{total_gb/1000*30:.3f} TB across 30 daily runs; free tier is 1 TB/mo).")
        return

    rows = run_query(client, run_date, window, max_gb, dry_run=False)
    events = build_events(rows)
    print(f"[data] {len(events)} deduped aviation-covered disruption events")

    if os.getenv("ENRICH_TITLES") == "1":
        enrich_titles(events, shown_event_ids(events, hubs))

    anchor_records = C.rollup_hubs(events, hubs, PARAMS["hub_radius_km"], PARAMS["decay_km"])
    panel = build_baseline_panel(client, run_date, anchor_records, window, max_gb, hubs)
    apply_baseline(anchor_records, panel, run_date)

    history = load_history()
    update_history(anchor_records, history, run_date)
    C.write_json(HISTORY_FILE, history)

    lanes = C.rollup_lanes(anchor_records, lanes_cfg)
    payload = C.assemble_payload(run_date, anchor_records, lanes, events, PARAMS, is_sample=False)
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
