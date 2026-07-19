# Air Freight Disruption Watch

An automated, **zero-cost** daily dashboard that scans GDELT for events likely to
disrupt air-cargo lanes, scores them, attributes them to your hubs, and plots the
result on an interactive map. Click a hub to read the headlines driving its alert.

It turns the manual notebook workflow (run SQL → export Excel → run cells → static
matplotlib map) into a hands-off pipeline that refreshes itself once a day.

```
GitHub Actions (daily cron)                     GitHub Pages (free static host)
        │                                                  ▲
        ├─ 1 capped BigQuery scan of GDELT ── query.sql    │  serves docs/index.html
        ├─ score + attribute to hubs (pandas-free, common.py)
        ├─ rolling 14-day baseline → z-scores → alert levels
        └─ writes docs/data.json ──── git commit ──────────┘  dashboard fetches data.json
```

Everything runs inside three permanently-free tiers: **BigQuery** (1 TB/month of
queries free — this uses a tiny fraction, with a hard cap so it can *never* be
billed), **GitHub Actions** (the scheduler), and **GitHub Pages** (the host).

---

## Repo layout

```
hubs.json                  ← YOUR hub list. Add a hub here and it shows up everywhere.
lanes.json                 ← optional origin→destination lanes for the arc layer
docs/
  index.html               ← the dashboard (single file). GitHub Pages serves this.
  data.json                ← regenerated daily by the pipeline (starts as sample data)
  history.json             ← rolling per-hub baseline (created on first run)
pipeline/
  query.sql                ← the BigQuery scan (your scored_events logic, parameterized)
  common.py                ← shared scoring / attribution / z-score logic
  fetch.py                 ← the daily job: query → score → write data.json
  requirements.txt
tools/
  make_sample_data.py      ← regenerates the offline sample data.json
.github/workflows/daily.yml← the schedule
```

The dashboard reads `docs/data.json` when deployed, and falls back to an embedded
sample when opened as a bare file — so you can double-click `docs/index.html` right
now and click around before any setup.

---

## Setup (about 15 minutes, no credit card required)

### 1. Put this project in a GitHub repo
Create a new repo (public repos get free Pages + unlimited Actions minutes) and push
these files.

### 2. Create a Google Cloud project + service account
1. Go to the [Google Cloud console](https://console.cloud.google.com/) → create a project.
2. Enable the **BigQuery API** for it.
3. **Billing:** you have two options, both free for this workload:
   - *Simplest & reliable:* enable billing, then set a **budget alert at $1** so you're
     notified of anything unexpected. You still pay **$0** — the pipeline caps every
     query at 30 GB (`MAX_GB`), and the first 1 TB/month is free, so it's impossible to
     be charged.
   - *No card at all:* use the [BigQuery sandbox](https://cloud.google.com/bigquery/docs/sandbox)
     (same 1 TB/month free tier, no billing). If a service-account query ever gets
     rejected in sandbox mode, switch to option 1 — you still won't be charged.
4. Create a **service account** → grant it the **BigQuery Job User** role → create a
   **JSON key** and download it. (Job User is enough; GDELT's public dataset is world-readable.)

### 3. Add the key as a repo secret
Repo → **Settings → Secrets and variables → Actions → New repository secret**:
- `GCP_SA_KEY` → paste the entire contents of the JSON key file.
- `GOOGLE_CLOUD_PROJECT` → your project id (optional; read from the key if omitted).

### 4. Turn on GitHub Pages
Repo → **Settings → Pages** → Source: **Deploy from a branch** → branch `main`,
folder **`/docs`** → Save. Your dashboard will be at
`https://<you>.github.io/<repo>/`.

### 5. Run it once
Repo → **Actions** → *Daily disruption scan* → **Run workflow**. It runs the scan,
commits a fresh `docs/data.json`, and Pages redeploys automatically. After that it
runs itself every day at 06:00 UTC. (Daily commits also keep the schedule alive —
GitHub only pauses cron on repos with 60 days of zero activity.)

---

## Add a hub

Open `hubs.json`, copy any line, edit the five fields:

```json
{ "hub_code": "BOM", "hub_name": "Mumbai", "hub_region": "APAC", "lat": 19.0887, "lon": 72.8679 }
```

Commit. On the next run the new hub appears on the map, gets nearby news attributed to
it, builds its own baseline, and becomes available for lanes. No code or SQL changes.

You can also **drop a hub live** in the dashboard (＋ Add hub → click the map) to
explore a location on the spot — that pin is session-only until you add it to `hubs.json`.

Add a lane the same way in `lanes.json` (origin/dest must be hub codes that exist).

---

## Tuning

All knobs are data or environment variables — no logic to edit.

| What | Where | Default |
|---|---|---|
| Hub catchment radius | `HUB_RADIUS_KM` env / `common` | 250 km |
| Distance decay for hub pressure | `DECAY_KM` env | 150 km |
| Headlines shown per hub | `TOP_HEADLINES_PER_HUB` env | 8 |
| Coverage-tail sweep | `MENTION_WINDOW_DAYS` env | 2 days |
| Event day to score | `RUN_DATE` env | yesterday (UTC) |
| Query byte cap | `MAX_GB` env | 30 GB |
| Severity weights | `DEFAULT_WEIGHTS` in `pipeline/common.py` | Goldstein 5, tone 2 |

**Severity / the Goldstein weight.** Your original SQL used `w_goldstein = -5.0`
against `ABS(LEAST(Goldstein,0))`, which makes *more* conflictual events score
*lower* — the opposite of a score ranked `DESC`. This project treats severity as a
magnitude (worse → higher), with a positive Goldstein weight, so headlines rank
correctly. To reproduce your exact original number, flip the weight negative and drop
the `abs()` in `severity_score()` (see the comment there). You can also switch on the
`w_volume` / `w_sources` weights to reward heavily-covered stories.

---

## Cost & limits

- **BigQuery:** partition + column pruning means each daily scan reads only what it
  needs — a few GB. `MAX_GB=30` caps it far below the **1 TB/month free tier**; a
  query that would exceed the cap fails instead of billing. Check the exact bytes with
  a dry run (below).
- **GitHub Actions:** a run takes a couple of minutes. Public repos get unlimited
  minutes; the Free plan's 2,000 private minutes/month is far more than ~60/month here.
- **GitHub Pages:** free static hosting for public repos.
- **Map tiles / fonts:** CARTO basemap + Google Fonts, both free at this scale.

---

## Run it locally

```bash
pip install -r pipeline/requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json   # or set GCP_SA_KEY

# See how many bytes the scan would read (no cost, nothing runs):
DRY_RUN=1 python pipeline/fetch.py

# Real run for a specific day:
RUN_DATE=2026-03-02 python pipeline/fetch.py

# Preview the dashboard:
cd docs && python -m http.server 8000    # then open http://localhost:8000

# Regenerate the offline sample data (no BigQuery needed):
cd tools && python make_sample_data.py
```

Turn on real headline scraping with `ENRICH_TITLES=1` (best-effort; falls back to
readable titles derived from the article URL when a page blocks scraping). It only
scrapes the handful of events actually shown, so it stays fast.

---

## Notes

- The hub and lane lists are **illustrative** — swap in your real network master data.
- GDELT ingests coverage for days after an event, so the pipeline scores yesterday and
  sweeps a 2-day tail. For a fully-settled feed, set `RUN_DATE` back another day.
- This surfaces *signals*, not verified operational impact — treat it as a lead for
  where to look, not ground truth.
