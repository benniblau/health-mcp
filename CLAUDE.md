# health-mcp — Claude Code Notes

## Project overview

Apple Health data for one person (Apple Watch + iPhone), pushed by the
**Health Auto Export** (HAE) iOS app, stored in SQLite and exposed over MCP.

Unlike `../coros-mcp` there is no downloader: the phone pushes, so ingest is a
POST route on the server itself. One process, one port.

- **`ingest.py`** — owns the database and everything that writes to it; also a
  CLI for inspecting what has arrived (`--list`, `--dump ID`)
- **`mcp_server.py`** — FastMCP tools, REST routes (including the ingest
  route), HTTP transport
- **`schema/schema_health.sql`** — single source of truth, executed on start

## Status

Phase 1 (capture) is built: every export is archived unmodified in
`ingest_log`. Nothing is parsed yet. Next:

2. Parser: SI normalisation, upserts into `workouts`, `metrics`, `sleep`, the
   series tables and one table per remaining HAE data type; `--replay` over
   `ingest_log`; tests against captured payloads in `tests/fixtures/`
3. Views and MCP tools for running progress
4. Full-history backfill from the phone, then the automation
5. README with the phone setup

**Write the parser against real payloads, not the docs.** The HAE help centre
does not document running dynamics, VO2max or splits, and it is unclear which
of those arrive as workout fields and which as metrics.

## Running locally

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python mcp_server.py            # HTTP on HEALTH_MCP_HTTP_PORT (8081)
.venv/bin/python ingest.py --list         # what has arrived
.venv/bin/python ingest.py --dump 3 -o tests/fixtures/run.json
```

## Environment variables

| Variable | Description |
|----------|-------------|
| `HEALTH_INGEST_TOKEN` | Bearer token on the phone — may only POST `/api/v1/ingest` |
| `HEALTH_MCP_AUTH_TOKEN` | Bearer token for MCP and the read REST API |
| `HEALTH_DB_PATH` | SQLite path (default `./health.db`) |
| `HEALTH_MAX_INGEST_MB` | Largest single export accepted (default 100) |
| `HEALTH_MCP_TRANSPORT` | `http` (default) or `stdio` |
| `HEALTH_MCP_HTTP_HOST` / `_PORT` | Bind address (default `0.0.0.0:8081`) |

## Health Auto Export notes

- Envelope is `{"data": {"metrics": [], "workouts": [], "stateOfMind": [],
  "medications": [], "symptoms": [], "cycleTracking": [], "ecg": [],
  "heartRateNotifications": []}}`. Dates are `yyyy-MM-dd HH:mm:ss Z`.
- **Units follow the phone's settings** (`km`/`mi`, `degC`/`degF`), and every
  quantity is `{qty, units}`. Normalise to SI on ingest; convert in views.
- **Resends are normal.** The default date range re-sends the previous day on
  every sync. Identical bodies are dropped by `sha256`; overlapping ones must
  be handled by idempotent upserts (workout `id`; metric `name + date + source`).
- **HAE cannot run while the iPhone is locked**, and iOS throttles background
  refresh. Data arrives when the phone is next used, not when a run ends.
- Use Export Version 2 — v1 has no workout `id` and different route keys.

## Safety invariants

- **Two tokens, two scopes.** The ingest token must never gain `mcp:access`:
  it sits on a phone. `guard(scope)` and `RequireAuthMiddleware` enforce it.
- **`/api/v1/health` is the only unauthenticated route** and must not reveal
  anything about the data.
- **`Authorization` and `Cookie` headers are never archived** (`SECRET_HEADERS`).
- **Both `/mcp` and `/mcp/` must work** — see `_accept_bare_mcp_path`.
- When `execute_sql` is added it must be read-only, as in coros-mcp
  (SELECT-only, no stacked statements, `PRAGMA query_only = ON`).
- Captured payloads are personal health data: `tests/fixtures/` and `*.db` are
  gitignored. Keep them out of commits.

## Deployment

Production follows the same pattern as the other MCPs on that host:
`/home/benni/health-mcp`, `.venv`, runs as `benni`, unit from `deploy/`
installed to `/etc/systemd/system/health-mcp.service`, **port 8090** (8080–8089
are taken by the other MCP services). `sudo` needs a password.

The server copy was placed by rsync, not `git clone`. Deploy by rsync, never
with `--delete` and always excluding `.env` and `*.db*` (the production
database is the only copy of the archive):

```bash
rsync -a --exclude .venv --exclude __pycache__ --exclude '*.db*' \
      --exclude .env --exclude tests/fixtures ./ <host>:health-mcp/
ssh <host> sudo systemctl restart health-mcp
```

Served through Pangolin on a fixed domain. The
Pangolin resource must have its own authentication disabled (or a bypass rule
for `/api/v1/ingest` and `/mcp`): neither HAE nor an MCP client can complete a
Pangolin login, so the bearer tokens are the guard.
