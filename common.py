"""
common.py — shared logic for the GDELT air-freight disruption dashboard.

Both the live pipeline (fetch.py) and the sample-data generator import from here,
so the JSON contract is identical whether the data is real or synthetic.

The design principle: ONE BigQuery scan produces scored events; every hub / lane /
outlier rollup below happens locally in pandas. That keeps BigQuery bytes (and cost)
minimal and lets you add hubs by editing hubs.json alone.
"""
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

# --------------------------------------------------------------------------------------
# CAMEO root-code -> human label (matches the CASE statement in the original SQL)
# --------------------------------------------------------------------------------------
CAMEO_ROOT_NAMES = {
    "14": "Protest / Strike",
    "15": "Force Posture / Mobilization / Blockade",
    "16": "Reduce Relations / Embargo / Sanctions",
    "17": "Coerce / Geopolitical Threats",
    "18": "Assault / Physical Violence",
    "19": "Fight / Military & Tactical Fire",
    "20": "Unconventional Mass Violence / Terrorism",
}


def cameo_name(root_code: str) -> str:
    root_code = str(root_code)
    return CAMEO_ROOT_NAMES.get(root_code, f"CAMEO Root {root_code}")


# --------------------------------------------------------------------------------------
# Severity score  (one place to tune it — used by the live pipeline AND the sample data)
# --------------------------------------------------------------------------------------
# NOTE ON THE GOLDSTEIN WEIGHT:
#   The original SQL used w_goldstein = -5.0 against ABS(LEAST(Goldstein,0)), which makes
#   MORE conflictual events score LOWER. That inverts a score ranked DESC, so it's treated
#   here as a magnitude weight (default +5.0): worse event -> higher severity. To reproduce
#   the exact original number, set w_goldstein negative AND remove the abs() on the term
#   below. All weights are data, so retune without touching any other code.
DEFAULT_WEIGHTS = {
    "w_goldstein": 5.0,   # conflict intensity magnitude (|Goldstein| for conflict events)
    "w_tone": 2.0,        # coverage negativity (|negative mention tone|)
    "w_volume": 0.0,      # aviation coverage volume,  log10(mentions+1)
    "w_sources": 0.0,     # distinct aviation sources,  log10(sources+1)
    "w_fatalities": 0.0,  # reported fatalities,        log10(fatalities+1)
    "material_conflict_bonus": 0.0,  # flat bump when QuadClass == 4 (material conflict)
}


def severity_score(goldstein, tone, mentions, sources, fatalities, quad_class, weights=None) -> float:
    """Composite disruption severity. Higher = more disruptive. Mirrors the SQL terms."""
    w = {**DEFAULT_WEIGHTS, **(weights or {})}
    g = goldstein if goldstein is not None else 0.0
    t = tone if tone is not None else 0.0
    conflict = abs(min(g, 0.0)) * w["w_goldstein"]
    neg_tone = abs(min(t, 0.0)) * w["w_tone"]
    vol = math.log10((mentions or 0) + 1) * w["w_volume"]
    src = math.log10((sources or 0) + 1) * w["w_sources"]
    fat = math.log10((fatalities or 0) + 1) * w["w_fatalities"]
    bonus = w["material_conflict_bonus"] if quad_class == 4 else 0.0
    return round(conflict + neg_tone + vol + src + fat + bonus, 2)


# --------------------------------------------------------------------------------------
# Geo
# --------------------------------------------------------------------------------------
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


# --------------------------------------------------------------------------------------
# Headline derivation from a source URL (free, no network)
# --------------------------------------------------------------------------------------
_STOP_SLUG = re.compile(r"\.(html?|php|aspx?|jsp|amp)$", re.I)
_ID_CHUNK = re.compile(r"^[0-9a-f]{6,}$|^\d{5,}$", re.I)  # article-id-looking chunks

# Titles that mean "the scrape failed", not real news. Extend freely.
GARBAGE_TITLES = {
    "", "403 forbidden", "404", "403", "404 - page not found", "page not found",
    "just a moment...", "access denied", "attention required! | cloudflare",
    "error 403 forbidden", "file not found", "405 not allowed.", "404 not found",
    "error: the request could not be satisfied", "are you a robot?", "bot verification",
    "[translation error]", "one moment, please...", "please wait...",
}


def is_garbage_title(title: str | None) -> bool:
    if not title:
        return True
    return title.strip().lower() in GARBAGE_TITLES


def title_from_url(url: str | None) -> str:
    """Best-effort readable headline from the URL slug. Instant and robust — good enough
    for ranking, and used as the fallback when live title scraping is off or fails."""
    if not url:
        return "Untitled report"
    try:
        path = urlparse(url).path
    except Exception:
        return "Untitled report"
    seg = [s for s in path.split("/") if s]
    if not seg:
        host = urlparse(url).netloc.replace("www.", "")
        return host or "Untitled report"
    slug = _STOP_SLUG.sub("", seg[-1])
    words = [w for w in re.split(r"[-_+]", slug) if w and not _ID_CHUNK.match(w)]
    if len(words) < 2 and len(seg) > 1:  # last segment was an id; try the one before
        slug2 = _STOP_SLUG.sub("", seg[-2])
        words = [w for w in re.split(r"[-_+]", slug2) if w and not _ID_CHUNK.match(w)]
    if not words:
        host = urlparse(url).netloc.replace("www.", "")
        return host or "Untitled report"
    text = " ".join(words)
    # Sentence-case-ish: capitalise first letter, keep obvious acronyms upper
    text = text[0].upper() + text[1:]
    return text[:160]


def source_name(url: str | None) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""


# --------------------------------------------------------------------------------------
# Hub attribution + rollups (all in pandas — no extra BigQuery bytes)
# --------------------------------------------------------------------------------------
def rollup_hubs(events: list[dict], hubs: list[dict], radius_km: float, decay_km: float) -> list[dict]:
    """For each hub, distance-decayed sum of nearby event severity + summary stats.
    Mirrors OUTPUT 2 of the original SQL, but computed locally."""
    out = []
    for h in hubs:
        idx = 0.0
        n = 0
        closest = None
        peak = None
        for e in events:
            d = haversine_km(h["lat"], h["lon"], e["lat"], e["lon"])
            if d <= radius_km:
                n += 1
                idx += e["severity"] * math.exp(-d / decay_km)
                closest = d if closest is None else min(closest, d)
                peak = e["severity"] if peak is None else max(peak, e["severity"])
        out.append({
            "hub_code": h["hub_code"], "hub_name": h["hub_name"], "hub_region": h["hub_region"],
            "lat": h["lat"], "lon": h["lon"],
            "disruption_index": round(idx, 2),
            "events_within_radius": n,
            "closest_event_km": round(closest, 1) if closest is not None else None,
            "peak_event_severity": round(peak, 2) if peak is not None else None,
        })
    return out


def rollup_lanes(hub_records: list[dict], lanes: list[dict]) -> list[dict]:
    """Lane impact = pressure at origin + pressure at destination. Mirrors OUTPUT 3."""
    by_code = {h["hub_code"]: h for h in hub_records}
    out = []
    for ln in lanes:
        o = by_code.get(ln["origin"])
        d = by_code.get(ln["dest"])
        if not o or not d:
            continue  # skip lanes that reference a hub not in hubs.json
        op = o["disruption_index"]
        dp = d["disruption_index"]
        out.append({
            "origin": ln["origin"], "dest": ln["dest"], "corridor": ln.get("corridor", ""),
            "origin_pressure": round(op, 2), "dest_pressure": round(dp, 2),
            "lane_impact_index": round(op + dp, 2),
            "o_lat": o["lat"], "o_lon": o["lon"], "d_lat": d["lat"], "d_lon": d["lon"],
        })
    out.sort(key=lambda x: x["lane_impact_index"], reverse=True)
    return out


# --------------------------------------------------------------------------------------
# Rolling baseline -> z-score -> alert level  (mirrors the notebook's outlier detection)
# --------------------------------------------------------------------------------------
def zscore(today: float, history_vals: list[float]) -> float | None:
    """z = (today - mean) / stddev over the trailing window. None if < 7 prior days."""
    vals = [v for v in history_vals if v is not None]
    if len(vals) < 7:
        return None
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    sd = math.sqrt(var)
    if sd < 1e-9:
        return 3.0 if today > mean else 0.0  # any jump off a flat baseline is notable
    return round((today - mean) / sd, 2)


def alert_level(events_in_radius: int, z: float | None, index_quantile: float) -> str:
    """Map to a 5-tier alert level. Uses z when enough history exists, else falls back
    to where today's index ranks among today's hubs (index_quantile in [0,1])."""
    if events_in_radius == 0:
        return "none"
    if z is not None:
        if z >= 3:
            return "extreme"
        if z >= 2:
            return "high"
        if z >= 1:
            return "elevated"
        return "normal"
    # cold-start fallback (fewer than 7 days of history yet)
    if index_quantile >= 0.90:
        return "high"
    if index_quantile >= 0.70:
        return "elevated"
    return "normal"


# --------------------------------------------------------------------------------------
# Assemble the final data.json payload
# --------------------------------------------------------------------------------------
def assemble_payload(run_date: str, hub_records: list[dict], lanes: list[dict],
                     events: list[dict], params: dict, is_sample: bool) -> dict:
    hubs_in_alert = sum(1 for h in hub_records if h["alert_level"] in ("elevated", "high", "extreme"))
    top_hub = max(hub_records, key=lambda h: h["disruption_index"], default=None)
    top_lane = lanes[0] if lanes else None
    return {
        "schema_version": 1,
        "run_date": run_date,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "is_sample": is_sample,
        "params": params,
        "stats": {
            "events_total": len(events),
            "hubs_in_alert": hubs_in_alert,
            "top_hub": (top_hub or {}).get("hub_code"),
            "top_hub_index": (top_hub or {}).get("disruption_index"),
            "top_corridor": (top_lane or {}).get("corridor"),
        },
        "hubs": sorted(hub_records, key=lambda h: h["disruption_index"], reverse=True),
        "lanes": lanes,
        "events": sorted(events, key=lambda e: e["severity"], reverse=True),
    }


def load_json(path: str | Path) -> dict:
    with open(path) as f:
        return json.load(f)


def write_json(path: str | Path, data: dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
