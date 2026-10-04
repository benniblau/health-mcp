# health-mcp

Apple Health data — Apple Watch workouts, heart rate, sleep and the rest — in
your own SQLite database, exposed to Claude via the Model Context Protocol.

The iOS app [Health Auto Export](https://www.healthyapps.dev/) pushes the data
from the phone; this server receives it, stores it and serves it. No Apple
account, cloud service or third party sits in between.

```
iPhone (Health Auto Export) ──POST JSON──▶ /api/v1/ingest ──▶ SQLite
                                                                ▲
Claude ──────────────── /mcp (streamable HTTP) ─────────────────┘
```

## Status

**Phase 1 of 5 — capture.** The server receives exports and archives every one
unmodified, but does not parse them yet. Strava is mirrored and queryable
(see below), so running history can be analysed today.

| Phase | | |
|---|---|---|
| 1 | Receive and archive exports, two-token auth, deployment | done |
| 2 | Parse into tables: workouts, metrics, sleep, series, other data types | next |
| 3 | Views and MCP tools for running progress and health trends | |
| 4 | Full-history backfill, then the recurring automation | |
| 5 | Documentation of the payload quirks found along the way | |

Archiving first is deliberate. The parser is written against real payloads
rather than documentation, and because the raw bodies are kept, it can be
re-run over everything received so far whenever it changes.

## What it does

- **`mcp_server.py`** — one process, one port: the ingest route the phone posts
  to, a stateless streamable-HTTP MCP endpoint, and a small read REST API.
- **`ingest.py`** — owns the database and everything that writes to it. Also a
  CLI for looking at what has arrived.

There is no downloader and no cron job: the phone pushes.

## Setup

### 1. Install dependencies

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 2. Configure tokens

```bash
cp .env.example .env
```

Generate **two different** tokens:

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

| Variable | Goes where | May do |
|---|---|---|
| `HEALTH_INGEST_TOKEN` | on the phone | `POST /api/v1/ingest`, nothing else |
| `HEALTH_MCP_AUTH_TOKEN` | in your MCP client | read via MCP and REST |

They are separate so that the token sitting on a phone cannot read health data.
The server refuses to start if they are equal.

### 3. Start the server

```bash
.venv/bin/python mcp_server.py                     # HTTP, port 8081 by default
.venv/bin/python mcp_server.py --transport stdio   # local Claude Desktop
```

The database is created on first start.

| Variable | Default | |
|---|---|---|
| `HEALTH_DB_PATH` | `./health.db` | SQLite path |
| `HEALTH_MAX_INGEST_MB` | `100` | largest single export accepted |
| `HEALTH_MCP_TRANSPORT` | `http` | or `stdio` |
| `HEALTH_MCP_HTTP_HOST` | `0.0.0.0` | bind address |
| `HEALTH_MCP_HTTP_PORT` | `8081` | port |

### 4. Make it reachable from the phone

The phone has to reach `/api/v1/ingest` over HTTPS from wherever it is, so put
the server behind a reverse proxy or tunnel on a fixed domain.

If the proxy adds its own login (Pangolin, Authelia, Cloudflare Access …), turn
that off for this service or bypass it for `/api/v1/ingest` and `/mcp`. Neither
Health Auto Export nor an MCP client can complete an interactive login; the
bearer tokens are the guard.

### 5. Configure Health Auto Export

Automations need the app's Premium tier. Create an automation of type
**REST API**:

| Setting | Value |
|---|---|
| URL | `https://<your-domain>/api/v1/ingest` |
| Headers | `Authorization` : `Bearer <HEALTH_INGEST_TOKEN>` |
| Export Format | JSON |
| Export Version | Version 2 |
| Data Type | whatever you want stored — all types are accepted |
| Summarize Data | off |
| Batch Requests | on |

Send a manual export of a few days first and check that it arrived (below)
before switching on a schedule.

Two things to expect from iOS:

- **Exports only run while the iPhone is unlocked.** Apps cannot read Health
  data on a locked phone, so a run shows up when the phone is next used, not
  when the workout ends.
- Background refresh is throttled, and paused in Low Power Mode.

### 6. Connect Claude

```bash
claude mcp add --transport http health https://<your-domain>/mcp \
  --header "Authorization: Bearer <HEALTH_MCP_AUTH_TOKEN>"
```

Both `/mcp` and `/mcp/` work.

## Checking what has arrived

```bash
.venv/bin/python ingest.py --list                 # recent exports and their contents
.venv/bin/python ingest.py --dump 12              # raw body of export 12, to stdout
.venv/bin/python ingest.py --dump 12 -o run.json  # ... or to a file
```

```bash
curl -H "Authorization: Bearer $HEALTH_MCP_AUTH_TOKEN" \
  https://<your-domain>/api/v1/sync-state
```

```json
{
  "requests": 1,
  "last_received_at": "2026-10-03T06:25:41+00:00",
  "recent": [
    {
      "id": 1,
      "received_at": "2026-10-03T06:25:41+00:00",
      "bytes": 253,
      "parsed_at": null,
      "contents": {
        "types": {"metrics": 1, "workouts": 1},
        "metrics": {"resting_heart_rate": 1},
        "workouts": {"Outdoor Run": 1}
      }
    }
  ]
}
```

## What Claude can query

| Tool | |
|---|---|
| `get_sync_status` | when the phone last sent data, what each export held, and when Strava last synced |
| `query_strava_activities` | list activities by sport, date and distance |
| `get_strava_activity` | one activity in full: laps, 1 km splits, best efforts, zones, optionally the recorded samples |
| `get_running_progress` | runs, distance, pace and heart rate per week or month, plus Strava's totals |
| `get_best_efforts` | fastest time per standard distance, or the progression over one distance |
| `get_strava_athlete` | profile, run totals, shoes |
| `execute_sql` | read-only `SELECT` over everything |

The Apple Health side has no tools yet: exports are archived but not parsed
(phase 2).

## Strava

Optional. `strava_downloader.py` mirrors one athlete's Strava account into the
same database — useful because Strava usually holds the running history from
before the watch.

```bash
.venv/bin/python strava_downloader.py --auth-url         # link to authorize with
.venv/bin/python strava_downloader.py --authorize CODE   # exchange the code
.venv/bin/python strava_downloader.py --with-streams     # sync; everything on a first run
```

`.env.example` walks through creating the API application. It asks for
read-only scopes and never writes to Strava. Tokens are kept in
`.strava_token.json` (mode 600). To keep it current, run it from cron:

```
30 * * * * /path/to/.venv/bin/python /path/to/strava_downloader.py --with-streams >> /path/to/download.log 2>&1
```

## REST API

| Route | Token | |
|---|---|---|
| `POST /api/v1/ingest` | ingest | receive one export |
| `GET /api/v1/sync-state?limit=` | read | recent exports, without bodies |
| `GET /api/v1/health` | none | liveness probe; reveals nothing about the data |

`POST /api/v1/ingest` answers:

| Status | |
|---|---|
| `200` | stored; the body lists what the export contained. `"duplicate": true` means an identical body was already archived and this one was dropped |
| `400` | not JSON, or not the `{"data": {...}}` envelope — usually the automation is set to CSV |
| `401` | missing or wrong token |
| `413` | larger than `HEALTH_MAX_INGEST_MB` — enable Batch Requests |

## Database schema

```
ingest_log — one row per export: received_at, sha256, size, request headers
             (credentials removed), per-type counts, gzipped raw body
```

`schema/schema_health.sql` is the single source of truth and is executed on
every start. The database runs in WAL mode so reads are not blocked while a
large export is written.

## The export format

```json
{
  "data": {
    "metrics": [], "workouts": [], "stateOfMind": [], "medications": [],
    "symptoms": [], "cycleTracking": [], "ecg": [], "heartRateNotifications": []
  }
}
```

- Dates are `yyyy-MM-dd HH:mm:ss Z`.
- Quantities are `{"qty": …, "units": …}`, and **units follow the phone's
  settings** (`km` or `mi`, `degC` or `degF`).
- **Resends are normal.** The default date range sends the previous day again
  on every sync. Identical bodies are dropped; overlapping ones will be handled
  by idempotent upserts once the parser exists.
- Export Version 1 has no workout `id` and different route keys — use Version 2.

Reference: the Health Auto Export help centre on the
[export format](https://help.healthyapps.dev/en/health-auto-export/export-format)
and the
[REST API automation](https://help.healthyapps.dev/en/health-auto-export/automations/rest-api).

## Privacy

This stores health data, and with every data type enabled that includes
medications, symptoms and cycle tracking.

- The database, `.env` and captured payloads (`tests/fixtures/`) are gitignored.
  Keep them out of commits.
- `Authorization` and `Cookie` headers are never written to the archive.
- The raw bodies are kept indefinitely, by design. Back the database up like
  anything else you cannot re-create, and restrict its file permissions.

## Production deployment (Linux)

```bash
# copy the project, create the venv, install requirements, write .env
sudo cp deploy/health-mcp.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now health-mcp
journalctl -u health-mcp -f
```

The unit assumes `/home/benni/health-mcp` and user `benni`; adjust the paths
and user for your machine. It deliberately does not set `ProtectHome`, because
the service lives under `/home`.

When updating, never overwrite `.env` or the database:

```bash
rsync -a --exclude .venv --exclude __pycache__ --exclude '*.db*' \
      --exclude .env --exclude tests/fixtures ./ <host>:health-mcp/
ssh <host> sudo systemctl restart health-mcp
```

## Modeled after

[coros-mcp](https://github.com/benniblau/coros-mcp) — same HTTP stack, auth
and conventions, with the downloader replaced by a push endpoint.
