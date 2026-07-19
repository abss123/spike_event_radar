"""
make_sample_data.py — produce a realistic docs/data.json WITHOUT touching BigQuery.

Used for (a) the live dashboard preview and (b) documenting the JSON contract. The
scenario is synthetic but plausible for late-Feb/early-Mar 2026: Gulf airspace tension,
a European ground-staff strike, and an APAC storm. Everything downstream (hub index,
z-scores, alert levels, lane impact) is computed by the SAME functions the real
pipeline uses, so the demo faithfully mirrors production output.
"""
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
import common as C

random.seed(20260302)
RUN_DATE = "2026-03-02"
PARAMS = {
    "hub_radius_km": 250, "decay_km": 150, "top_headlines_per_hub": 8,
    "weights": dict(C.DEFAULT_WEIGHTS),
}

# (title, near_hub, dlat, dlon, root, quad, tone, fatalities, goldstein, mentions, sources, a1, a2, url)
EVENTS_RAW = [
    # ---- Gulf / Middle East cluster: geopolitical escalation, airspace risk -------------
    ("Gulf states warn airlines over airspace risk as tensions climb", "DXB", 0.15, 0.20, "17", 3, -7.8, 0, -6.5, 71, 34, "United Arab Emirates", "Iran", "https://www.reuters.com/world/middle-east/gulf-airspace-warning-2026-03-02"),
    ("Carriers reroute around Strait of Hormuz after military build-up", "DXB", -0.4, 0.6, "15", 4, -8.6, 0, -8.0, 88, 41, "Military", "Iran", "https://www.aljazeera.com/news/2026/3/2/hormuz-reroute-flights"),
    ("Missile test near shipping lane rattles cargo operators", "DOH", 0.3, -0.35, "19", 4, -9.1, 0, -9.0, 64, 29, "Iran", "Gulf Cooperation Council", "https://www.bloomberg.com/news/articles/2026-03-02/missile-test-cargo"),
    ("Doha hub adds security screening as regional threat level rises", "DOH", -0.1, 0.15, "17", 3, -6.2, 0, -5.5, 42, 22, "Qatar", "Unknown", "https://www.thenationalnews.com/gulf/2026/03/02/doha-screening"),
    ("Sanctions threat clouds Tehran overflight permissions", "IKA", 0.2, 0.25, "16", 3, -6.9, 0, -6.0, 55, 26, "United States", "Iran", "https://apnews.com/article/iran-overflight-sanctions-2026"),
    ("Air-defence activity reported north of Tehran airport", "IKA", 0.35, -0.2, "19", 4, -8.9, 2, -8.5, 47, 21, "Iran", "Unknown", "https://www.reuters.com/world/middle-east/tehran-air-defence-2026"),
    ("Insurers hike war-risk premiums on Gulf air cargo", "DXB", 0.9, 1.1, "16", 3, -5.4, 0, -4.5, 33, 19, "Lloyds", "Gulf", "https://www.ft.com/content/gulf-war-risk-premium-2026"),

    # ---- Europe: ground-staff / ATC strike, protests --------------------------------------
    ("Ground-staff strike grounds cargo flights at Paris CDG", "CDG", 0.05, 0.1, "14", 3, -6.8, 0, -5.0, 96, 44, "Labor Union", "Aeroports de Paris", "https://www.lemonde.fr/economie/article/2026/03/02/cdg-greve-fret"),
    ("Union pickets spread to Frankfurt as pay talks collapse", "FRA", 0.1, -0.2, "14", 3, -6.1, 0, -4.5, 78, 37, "Verdi", "Fraport", "https://www.dw.com/en/frankfurt-strike-cargo-2026"),
    ("Handlers walk out at Leipzig sorting centre overnight", "LEJ", -0.15, 0.2, "14", 3, -5.7, 0, -4.0, 51, 24, "Labor Union", "DHL Hub", "https://www.reuters.com/business/leipzig-handlers-strike-2026"),
    ("Farmers block roads near Paris freight terminals", "CDG", -0.6, -0.5, "14", 3, -4.9, 0, -3.5, 44, 21, "Farmers", "Government of France", "https://www.france24.com/en/europe/20260302-paris-farmer-blockade"),
    ("Frankfurt slot cancellations ripple across European network", "FRA", 0.5, 0.4, "14", 3, -5.2, 0, -3.8, 39, 20, "Fraport", "Airlines", "https://www.aviation-news.eu/frankfurt-slots-2026"),

    # ---- APAC: storm + a protest -----------------------------------------------------------
    ("Severe storm shuts runways across the Pearl River Delta", "HKG", 0.2, 0.15, "14", 1, -4.2, 0, -1.5, 82, 38, None, None, "https://www.scmp.com/news/hong-kong/weather/2026/03/02/prd-storm"),
    ("Guangzhou cargo backlog builds after weather closures", "CAN", 0.1, -0.1, "14", 1, -3.8, 0, -1.0, 47, 22, None, None, "https://www.chinadaily.com.cn/guangzhou-cargo-2026"),
    ("Port protest snarls trucking to Shanghai Pudong", "PVG", -0.2, 0.25, "14", 3, -4.6, 0, -3.2, 40, 19, "Workers", "Port Authority", "https://www.reuters.com/world/china/shanghai-port-protest-2026"),
    ("Typhoon warning lifted but delays persist at Hong Kong", "HKG", -0.35, -0.2, "14", 1, -2.9, 0, -0.5, 28, 15, None, None, "https://www.hongkongfp.com/2026/03/02/typhoon-delays"),

    # ---- US: minor disruptions -------------------------------------------------------------
    ("Protest outside Memphis facility briefly halts trucks", "MEM", 0.15, 0.2, "14", 3, -3.4, 0, -2.5, 31, 16, "Protesters", "Company", "https://www.commercialappeal.com/memphis-protest-2026"),
    ("Newark ramp workers rally over scheduling dispute", "EWR", 0.1, -0.15, "14", 3, -3.1, 0, -2.0, 24, 13, "Labor Union", "Airport", "https://www.nj.com/newark-ramp-rally-2026"),

    # ---- Scattered lower-severity items near quieter hubs ----------------------------------
    ("Sanctions review touches select cargo routes via Singapore", "SIN", 0.6, 0.5, "16", 3, -3.6, 0, -2.8, 26, 14, "United States", "Unknown", "https://www.straitstimes.com/business/singapore-sanctions-review-2026"),
    ("Minor demonstration near Incheon cargo gate", "ICN", 0.2, 0.2, "14", 3, -2.4, 0, -1.5, 18, 11, "Protesters", "Airport", "https://en.yna.co.kr/incheon-demo-2026"),
]

hubs = C.load_json(ROOT / "hubs.json")["hubs"]
lanes_cfg = C.load_json(ROOT / "lanes.json")["lanes"]
hub_by_code = {h["hub_code"]: h for h in hubs}

# Build event dicts with jittered coords near their anchor hub
events = []
for i, (title, near, dlat, dlon, root, quad, tone, fat, gold, ment, src, a1, a2, url) in enumerate(EVENTS_RAW):
    base = hub_by_code[near]
    lat = round(base["lat"] + dlat, 4)
    lon = round(base["lon"] + dlon, 4)
    sev = C.severity_score(gold, tone, ment, src, fat, quad, PARAMS["weights"])
    events.append({
        "id": 100000 + i,
        "severity": sev,
        "event_name": C.cameo_name(root),
        "quad_class": quad,
        "location": f"{base['hub_name'].split('(')[0].strip()} area",
        "country": base["hub_region"],
        "lat": lat, "lon": lon,
        "tone": tone, "fatalities": fat, "goldstein": gold,
        "mentions": ment, "sources": src,
        "actor1": a1, "actor2": a2,
        "title": title,
        "url": url,
        "source": C.source_name(url),
    })

# Hub rollups (pandas-free here; pure python from common)
hub_records = C.rollup_hubs(events, hubs, PARAMS["hub_radius_km"], PARAMS["decay_km"])

# Synthetic 14-day history so z-scores/alert levels are meaningful in the demo.
# Baseline center is set as a fraction of today's index so the resulting z lands in a
# chosen tier — enough variety to show all five alert levels on the map.
SPIKE = {"DXB", "DOH", "IKA"}   # z >> 3  -> extreme
HIGH = {"CDG", "FRA"}            # z ~ 2-3 -> high
ELEVATED = {"LEJ", "HKG", "CAN"}  # z ~ 1-2 -> elevated
dates = [(date.fromisoformat(RUN_DATE) - timedelta(days=d)).isoformat() for d in range(13, -1, -1)]
today_indices = [h["disruption_index"] for h in hub_records]
sorted_idx = sorted(today_indices)

for h in hub_records:
    today = h["disruption_index"]
    if h["hub_code"] in SPIKE:
        center = today * 0.12
    elif h["hub_code"] in HIGH:
        center = today * 0.75
    elif h["hub_code"] in ELEVATED:
        center = today * 0.82
    else:
        center = today  # no anomaly
    hist = [max(0.0, round(random.gauss(center, center * 0.18 + 0.5), 2)) for _ in dates[:-1]]
    z = C.zscore(today, hist)
    # index quantile fallback (unused here since history exists, but keep contract complete)
    q = (sorted_idx.index(today) + 1) / len(sorted_idx) if sorted_idx else 0.0
    h["z_score"] = z
    h["alert_level"] = C.alert_level(h["events_within_radius"], z, q)
    h["trend"] = [{"date": dt, "index": v} for dt, v in zip(dates, hist + [round(today, 2)])]

lanes = C.rollup_lanes(hub_records, lanes_cfg)
payload = C.assemble_payload(RUN_DATE, hub_records, lanes, events, PARAMS, is_sample=True)
C.write_json(ROOT / "docs" / "data.json", payload)

print(f"Wrote docs/data.json  |  {len(events)} events, {len(hub_records)} hubs, {len(lanes)} lanes")
print("Alert levels:", {h["hub_code"]: h["alert_level"] for h in hub_records if h["alert_level"] != "normal"})
