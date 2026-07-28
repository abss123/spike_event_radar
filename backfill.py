#!/usr/bin/env python3
"""
backfill.py — one-off script to seed/rebuild docs/hub_daily.json.

fetch.py's daily run reads prior baseline days from docs/hub_daily.json instead of
re-querying BigQuery for each one (see build_baseline_panel / missing_baseline_days in
fetch.py). Run this once to seed that cache before the incremental pipeline can pay off,
and again any time DEFAULT_WEIGHTS changes — a stale weights fingerprint makes fetch.py
treat the whole cache as missing and fall back to querying every baseline day live.

Usage:
  python backfill.py --start 2026-01-01 --end 2026-02-27

Scores every day in [start, end] that isn't already cached under the current weights
fingerprint (skipped unless --force), writing one zero-filled disruption_index per hub
per day into docs/hub_daily.json exactly as fetch.py would score a live anchor day.
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta

import common as C
from fetch import HUB_DAILY_FILE, HUBS_FILE, PARAMS, get_client, load_hub_daily, score_day


def daterange(start: str, end: str) -> list[str]:
    a, b = date.fromisoformat(start), date.fromisoformat(end)
    if b < a:
        raise ValueError("--end must not be before --start")
    return [(a + timedelta(days=i)).isoformat() for i in range((b - a).days + 1)]


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", required=True, help="first day to backfill, YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="last day to backfill (inclusive), YYYY-MM-DD")
    ap.add_argument("--window", type=int, default=2, help="MENTION_WINDOW_DAYS (default 2)")
    ap.add_argument("--max-gb", type=float, default=30.0, help="per-query byte cap in GB (default 30)")
    ap.add_argument("--force", action="store_true",
                     help="re-query days already cached under the current weights fingerprint")
    args = ap.parse_args()

    days = daterange(args.start, args.end)
    hubs = C.load_json(HUBS_FILE)["hubs"]
    fp = C.weights_fingerprint(PARAMS["weights"])

    hub_daily = load_hub_daily()
    if hub_daily.get("weights_fingerprint") == fp and not args.force:
        series = hub_daily.get("series", {})
    else:
        if hub_daily.get("series"):
            print(f"[backfill] cached weights fingerprint ({hub_daily.get('weights_fingerprint')}) "
                  f"!= active ({fp}) — rebuilding the cache from scratch.")
        series = {}

    cached_dates: set[str] = set()
    for h_series in series.values():
        cached_dates.update(h_series.keys())

    todo = days if args.force else [d for d in days if d not in cached_dates]
    print(f"[backfill] {len(days)} day(s) requested, {len(days) - len(todo)} already cached, "
          f"{len(todo)} to query.")

    client = get_client()
    for i, d in enumerate(todo, 1):
        print(f"[backfill] ({i}/{len(todo)}) scoring {d}...")
        for h in score_day(client, d, args.window, args.max_gb, hubs):
            series.setdefault(h["hub_code"], {})[d] = h["disruption_index"]

    C.write_json(HUB_DAILY_FILE, {"schema_version": 1, "weights_fingerprint": fp, "series": series})
    per_hub_days = len(next(iter(series.values()), {}))
    print(f"[backfill] wrote {HUB_DAILY_FILE.name}: {len(series)} hub(s), "
          f"up to {per_hub_days} day(s) each.")


if __name__ == "__main__":
    main()
