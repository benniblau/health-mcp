#!/usr/bin/env python3
"""
MCP Server for Apple Health data pushed by Health Auto Export

One process does both halves: it receives the exports the phone POSTs and
exposes what has been stored to MCP clients and over a small REST API.

Transports:
    stdio                — for local Claude Desktop (read side only)
    streamable HTTP      — stateless, bearer-authenticated, for remote clients

Usage:
    python mcp_server.py                       # HTTP (default) on HEALTH_MCP_HTTP_PORT
    python mcp_server.py --transport stdio     # stdio

In HTTP mode the server serves:
    /mcp  and  /mcp/       — the MCP streamable HTTP endpoint (both spellings)
    POST /api/v1/ingest    — where Health Auto Export sends data (ingest token)
    /api/v1/...            — the read REST API (read token)
    /api/v1/health         — liveness probe, unauthenticated

Two tokens, deliberately: the one on the phone can only write, so losing the
phone does not hand anyone the health data.
"""

import argparse
import hmac
import json
import logging
import os
import sqlite3
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

from mcp.server.fastmcp import FastMCP

import ingest
from ingest import DB_PATH, get_db, init_database

# ── Logging to stderr only (keep stdout clean for STDIO MCP transport) ──────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# ── Server ───────────────────────────────────────────────────────────────────
mcp = FastMCP("apple-health")

# Mount point for the streamable HTTP transport (no trailing slash).
MCP_PATH = "/mcp"
API_PREFIX = "/api/v1"

SCOPE_READ = "mcp:access"
SCOPE_INGEST = "ingest:write"

# A full-scope export with GPS routes is large; batching on the phone keeps
# single requests well under this.
MAX_INGEST_BYTES = int(os.getenv("HEALTH_MAX_INGEST_MB", "100")) * 1024 * 1024


MAX_LIMIT = 500

# Columns that may be interpolated into an ORDER BY clause. Anything reaching
# an ORDER BY must come from this set — it cannot be parameterised.
STRAVA_ORDER_COLUMNS = {
    "date", "start_date_local", "distance_km", "moving_time", "elapsed_time_min",
    "pace_min_per_km", "total_elevation_gain", "average_heartrate",
    "max_heartrate", "cadence_spm", "suffer_score", "calories", "name",
}

RUN_TYPES = ("Run", "TrailRun", "VirtualRun")


def _json(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)


def _rows(rows) -> List[Dict[str, Any]]:
    # Blobs (the archived request bodies) are reported by size, not dumped.
    return [
        {k: (f"<{len(v)} bytes>" if isinstance(v, bytes) else v) for k, v in dict(r).items()}
        for r in rows
    ]


def _clock(seconds: Optional[float]) -> Optional[str]:
    """Seconds as h:mm:ss or m:ss — how a runner reads a time."""
    if seconds is None:
        return None
    seconds = int(round(seconds))
    h, rest = divmod(seconds, 3600)
    m, sec = divmod(rest, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def _pace(seconds: Optional[float], metres: Optional[float]) -> Optional[str]:
    """Pace as m:ss per km."""
    if not seconds or not metres:
        return None
    return _clock(seconds / (metres / 1000.0))


# ─────────────────────────────────────────────────────────────────────────────
# Tools
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def get_sync_status(limit: int = 20) -> str:
    """
    Show when the phone last sent data and what each recent export contained.

    Health Auto Export can only run while the iPhone is unlocked, so a gap
    here usually means the phone, not the server.
    """
    status = ingest.recent_ingests(max(1, min(limit, 200)))
    with get_db() as conn:
        count, synced, newest = conn.execute(
            "SELECT COUNT(*), MAX(synced_at), MAX(start_date_local) FROM strava_activities"
        ).fetchone()
    status["strava"] = {
        "activities": count,
        "last_synced_at": synced,
        "newest_activity": newest,
    }
    return _json(status)


# ── Strava ───────────────────────────────────────────────────────────────────

@mcp.tool()
def query_strava_activities(
    sport_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_distance_km: Optional[float] = None,
    max_distance_km: Optional[float] = None,
    limit: int = 50,
    offset: int = 0,
    order_by: str = "date",
    order_desc: bool = True,
) -> str:
    """
    List Strava activities with filters, newest first by default.

    Each row carries distance_km, moving_time (seconds), pace_min_per_km
    (decimal minutes) and pace (m:ss per km), average_heartrate, cadence_spm,
    elevation gain, calories and the recording device. Heart rate is absent
    for runs recorded with the phone alone.

    Args:
        sport_type: Strava sport type, e.g. 'Run', 'TrailRun', 'Walk', 'Ride'.
        start_date: Earliest activity date (YYYY-MM-DD).
        end_date: Latest activity date (YYYY-MM-DD).
        min_distance_km: Minimum distance in km.
        max_distance_km: Maximum distance in km.
        limit: Maximum results (default 50, max 500).
        offset: Number of results to skip, for paging.
        order_by: Sort column (default 'date').
        order_desc: Sort descending if True.
    """
    if order_by not in STRAVA_ORDER_COLUMNS:
        return _json({"error": f"order_by must be one of {sorted(STRAVA_ORDER_COLUMNS)}"})
    clauses, params = [], []
    for clause, value in (
        ("sport_type = ?", sport_type),
        ("date >= ?", start_date),
        ("date <= ?", end_date),
        ("distance_km >= ?", min_distance_km),
        ("distance_km <= ?", max_distance_km),
    ):
        if value is not None:
            clauses.append(clause)
            params.append(value)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    direction = "DESC" if order_desc else "ASC"
    with get_db() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM strava_activity_summary {where}", params
        ).fetchone()[0]
        rows = _rows(conn.execute(
            f"SELECT * FROM strava_activity_summary {where} "
            f"ORDER BY {order_by} {direction} LIMIT ? OFFSET ?",
            params + [max(1, min(limit, MAX_LIMIT)), max(0, offset)],
        ).fetchall())
    for r in rows:
        r["pace"] = _pace(r["moving_time"], (r["distance_km"] or 0) * 1000)
    return _json({"total": total, "count": len(rows), "activities": rows})


@mcp.tool()
def get_strava_activity(activity_id: int, include_streams: bool = False,
                        max_stream_points: int = 300) -> str:
    """
    One Strava activity in full: summary, description, laps, 1 km splits,
    best efforts within the run, and time in heart-rate / pace zones.

    Args:
        activity_id: Strava activity id, from query_strava_activities.
        include_streams: Also return the recorded samples (time, distance,
            position, altitude, speed, heart rate, cadence, grade), thinned
            evenly to at most max_stream_points.
        max_stream_points: Upper bound on samples returned (default 300,
            max 2000). The stored stream is roughly one sample per second.
    """
    with get_db() as conn:
        summary = conn.execute(
            "SELECT * FROM strava_activity_summary WHERE id = ?", (activity_id,)
        ).fetchone()
        if summary is None:
            return _json({"error": f"No Strava activity with id {activity_id}"})
        activity = dict(summary)
        activity["pace"] = _pace(activity["moving_time"], (activity["distance_km"] or 0) * 1000)
        activity.update(dict(conn.execute(
            """SELECT description, timezone, start_date AS start_date_utc, elev_high,
                      elev_low, max_speed, start_lat, start_lng, workout_type,
                      visibility, pr_count, external_id
               FROM strava_activities WHERE id = ?""", (activity_id,)).fetchone()))

        laps = _rows(conn.execute(
            """SELECT lap_index, name, distance, elapsed_time, moving_time,
                      total_elevation_gain, average_heartrate, max_heartrate,
                      average_cadence * 2 AS cadence_spm
               FROM strava_activity_laps WHERE activity_id = ? ORDER BY lap_index""",
            (activity_id,)).fetchall())
        splits = _rows(conn.execute(
            """SELECT split, distance, moving_time, elapsed_time, elevation_difference,
                      average_heartrate, average_grade_adjusted_speed
               FROM strava_activity_splits WHERE activity_id = ? ORDER BY split""",
            (activity_id,)).fetchall())
        efforts = _rows(conn.execute(
            """SELECT name, distance, elapsed_time, pr_rank
               FROM strava_best_efforts WHERE activity_id = ? ORDER BY distance""",
            (activity_id,)).fetchall())
        zones = _rows(conn.execute(
            """SELECT zone_type, zone_index, zone_min, zone_max, time
               FROM strava_activity_zones WHERE activity_id = ?
               ORDER BY zone_type, zone_index""", (activity_id,)).fetchall())

        for row in laps + splits:
            row["pace"] = _pace(row["moving_time"], row["distance"])
        for row in efforts:
            row["time"] = _clock(row["elapsed_time"])
            row["pace"] = _pace(row["elapsed_time"], row["distance"])

        result: Dict[str, Any] = {
            "activity": activity, "laps": laps, "splits_km": splits,
            "best_efforts": efforts, "zones": zones,
        }

        if include_streams:
            total = conn.execute(
                "SELECT COUNT(*) FROM strava_activity_streams WHERE activity_id = ?",
                (activity_id,)).fetchone()[0]
            cap = max(1, min(max_stream_points, 2000))
            step = max(1, -(-total // cap))
            result["streams"] = {
                "samples_stored": total,
                "every_nth": step,
                "samples": _rows(conn.execute(
                    """SELECT time, distance, lat, lng, altitude, velocity,
                              heartrate, cadence, grade
                       FROM strava_activity_streams
                       WHERE activity_id = ? AND idx % ? = 0 ORDER BY idx""",
                    (activity_id, step)).fetchall()),
            }
    return _json(result)


@mcp.tool()
def get_running_progress(period: str = "week", limit: int = 26) -> str:
    """
    Running volume and pace over time, from Strava: runs, total and longest
    distance, time, average pace, elevation and average heart rate per week
    or month, newest first, plus Strava's own recent / year-to-date / all-time
    run totals.

    Periods without a run are simply absent — a gap in the list is a gap in
    training. Weeks start on Monday.

    Args:
        period: 'week' (default) or 'month'.
        limit: Number of periods to return (default 26, max 500).
    """
    if period not in ("week", "month"):
        return _json({"error": "period must be 'week' or 'month'"})
    limit = max(1, min(limit, MAX_LIMIT))
    with get_db() as conn:
        if period == "week":
            rows = _rows(conn.execute(
                "SELECT * FROM strava_weekly_running LIMIT ?", (limit,)).fetchall())
        else:
            marks = ", ".join("?" for _ in RUN_TYPES)
            rows = _rows(conn.execute(
                f"""SELECT strftime('%Y-%m', start_date_local) AS month,
                           COUNT(*) AS runs,
                           ROUND(SUM(distance) / 1000.0, 1) AS total_km,
                           ROUND(MAX(distance) / 1000.0, 1) AS longest_km,
                           ROUND(SUM(moving_time) / 3600.0, 2) AS total_hours,
                           ROUND((SUM(moving_time) / 60.0) / (SUM(distance) / 1000.0), 2)
                               AS pace_min_per_km,
                           ROUND(SUM(total_elevation_gain), 0) AS elevation_m,
                           ROUND(AVG(average_heartrate), 0) AS avg_heartrate
                    FROM strava_activities
                    WHERE sport_type IN ({marks}) AND distance > 0
                    GROUP BY month ORDER BY month DESC LIMIT ?""",
                list(RUN_TYPES) + [limit]).fetchall())
        athlete = conn.execute(
            """SELECT recent_run_count, recent_run_distance, recent_run_moving_time,
                      ytd_run_count, ytd_run_distance, ytd_run_moving_time,
                      all_run_count, all_run_distance, all_run_moving_time, synced_at
               FROM strava_athletes LIMIT 1""").fetchone()

    for r in rows:
        r["pace"] = _clock(r["pace_min_per_km"] * 60) if r["pace_min_per_km"] else None
    totals = {}
    if athlete:
        for prefix, label in (("recent", "last_4_weeks"), ("ytd", "year_to_date"), ("all", "all_time")):
            distance, seconds = athlete[f"{prefix}_run_distance"], athlete[f"{prefix}_run_moving_time"]
            totals[label] = {
                "runs": athlete[f"{prefix}_run_count"],
                "km": round((distance or 0) / 1000.0, 1),
                "hours": round((seconds or 0) / 3600.0, 1),
                "pace": _pace(seconds, distance),
            }
    return _json({"period": period, "periods": rows, "strava_totals": totals})


@mcp.tool()
def get_best_efforts(distance: Optional[str] = None) -> str:
    """
    Fastest times over standard distances, as Strava finds them inside runs
    (a 5K best can come from the middle of a 10 km run). Only GPS runs have
    them.

    Without `distance`: the best time per distance, with the run it came from.
    With `distance`: every effort over that distance in date order — the
    progression.

    Args:
        distance: One of Strava's names, e.g. '400m', '1/2 mile', '1K',
            '1 mile', '2 mile', '5K', '10K', '15K', '10 mile', '20K',
            'Half-Marathon', 'Marathon'.
    """
    with get_db() as conn:
        if distance is None:
            rows = _rows(conn.execute(
                """SELECT e.name, e.distance, MIN(e.elapsed_time) AS elapsed_time,
                          date(e.start_date_local) AS date, e.activity_id,
                          a.name AS activity_name,
                          (SELECT COUNT(*) FROM strava_best_efforts x WHERE x.name = e.name)
                              AS efforts
                   FROM strava_best_efforts e
                   JOIN strava_activities a ON a.id = e.activity_id
                   GROUP BY e.name ORDER BY e.distance""").fetchall())
        else:
            rows = _rows(conn.execute(
                """SELECT e.name, e.distance, e.elapsed_time,
                          date(e.start_date_local) AS date, e.activity_id,
                          a.name AS activity_name, e.pr_rank
                   FROM strava_best_efforts e
                   JOIN strava_activities a ON a.id = e.activity_id
                   WHERE e.name = ? COLLATE NOCASE ORDER BY e.start_date_local""",
                (distance,)).fetchall())
            if not rows:
                names = [r[0] for r in conn.execute(
                    "SELECT DISTINCT name FROM strava_best_efforts ORDER BY distance")]
                return _json({"error": f"No efforts named '{distance}'", "available": names})
    for r in rows:
        r["time"] = _clock(r["elapsed_time"])
        r["pace"] = _pace(r["elapsed_time"], r["distance"])
    return _json({"distance": distance, "efforts": rows})


@mcp.tool()
def get_strava_athlete() -> str:
    """Strava profile, run totals (last 4 weeks, year to date, all time) and shoes."""
    with get_db() as conn:
        athlete = conn.execute(
            """SELECT id, firstname, lastname, city, country, sex, weight,
                      created_at, recent_run_count, recent_run_distance,
                      ytd_run_count, ytd_run_distance, all_run_count,
                      all_run_distance, synced_at
               FROM strava_athletes LIMIT 1""").fetchone()
        gear = _rows(conn.execute(
            """SELECT name, brand_name, model_name, gear_type,
                      ROUND(distance / 1000.0, 1) AS distance_km, retired
               FROM strava_gear ORDER BY distance DESC""").fetchall())
    if athlete is None:
        return _json({"error": "No Strava data yet — has strava_downloader.py run?"})
    return _json({"athlete": dict(athlete), "gear": gear})


@mcp.tool()
def execute_sql(query: str, limit: int = 100) -> str:
    """
    Run a custom read-only SELECT query against the database.

    Strava tables: strava_activities, strava_activity_laps,
    strava_activity_splits (1 km), strava_best_efforts, strava_activity_zones,
    strava_activity_streams (per sample), strava_athletes, strava_gear.
    Strava views: strava_activity_summary, strava_weekly_running,
    strava_monthly_stats.
    Apple Health: ingest_log (one row per export received; bodies not parsed
    into tables yet).

    Strava units: distance in metres, time in seconds, speed in m/s;
    average_cadence counts one foot (double it for steps per minute);
    start_date_local is local wall time. The views convert to km and min/km.
    Only SELECT statements are permitted.

    Args:
        query: SQL SELECT query (WITH … SELECT is allowed).
        limit: Maximum rows (default 100, max 1000).
    """
    stripped = query.strip().upper()
    if not stripped.startswith(("SELECT", "WITH")):
        return _json({"error": "Only SELECT queries are permitted"})
    # Block multi-statement payloads smuggled in behind a semicolon.
    if ";" in query.strip().rstrip(";"):
        return _json({"error": "Multiple SQL statements are not permitted"})

    sql = query.strip().rstrip(";")
    if "LIMIT" not in stripped:
        sql += f" LIMIT {max(1, min(limit, 1000))}"

    try:
        with get_db() as conn:
            # Defence in depth: reject writes even if they slip past the checks.
            conn.execute("PRAGMA query_only = ON")
            rows = conn.execute(sql).fetchall()
        return _json({"count": len(rows), "rows": _rows(rows)})
    except sqlite3.Error as e:
        return _json({"error": str(e)})


# ─────────────────────────────────────────────────────────────────────────────
# REST API
# ─────────────────────────────────────────────────────────────────────────────

def build_rest_routes():
    from starlette.concurrency import run_in_threadpool
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    def ok(payload, status: int = 200):
        return JSONResponse(json.loads(_json(payload)), status_code=status)

    def err(message: str, status: int = 400):
        return JSONResponse({"error": message}, status_code=status)

    def guard(scope: str):
        """Reject calls without the given scope before touching the database."""
        def decorator(handler):
            async def wrapped(request):
                if scope not in request.auth.scopes:
                    return err("Unauthorized", 401)
                try:
                    return await handler(request)
                except ValueError as e:
                    return err(str(e), 400)
                except Exception as e:                       # noqa: BLE001
                    logger.exception("REST handler failed")
                    return err(str(e), 500)
            return wrapped
        return decorator

    async def health(request):
        """Liveness probe — intentionally unauthenticated, and says nothing else."""
        try:
            with get_db() as conn:
                conn.execute("SELECT 1 FROM ingest_log LIMIT 1")
            return ok({"status": "ok"})
        except Exception as e:                               # noqa: BLE001
            return err(f"database unavailable: {e}", 503)

    @guard(SCOPE_INGEST)
    async def ingest_export(request):
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > MAX_INGEST_BYTES:
            return err("payload too large — enable Batch Requests", 413)
        chunks, size = [], 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > MAX_INGEST_BYTES:
                return err("payload too large — enable Batch Requests", 413)
            chunks.append(chunk)
        # Parsing and compressing tens of MB would stall MCP requests if done
        # on the event loop.
        result = await run_in_threadpool(
            ingest.store_payload, b"".join(chunks), dict(request.headers)
        )
        logger.info(f"ingest #{result['id']}: {size} bytes, {result['types']}"
                    + (" (duplicate)" if result["duplicate"] else ""))
        return ok(result)

    @guard(SCOPE_READ)
    async def sync_state(request):
        limit = int(request.query_params.get("limit", "20"))
        return ok(ingest.recent_ingests(max(1, min(limit, 200))))

    p = API_PREFIX
    return [
        Route(f"{p}/health", health, methods=["GET"]),
        Route(f"{p}/ingest", ingest_export, methods=["POST"]),
        Route(f"{p}/sync-state", sync_state, methods=["GET"]),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Transport
# ─────────────────────────────────────────────────────────────────────────────

def run_stdio() -> None:
    init_database()
    mcp.run()


def main_http() -> None:
    """Run the MCP streamable HTTP transport and the REST API together."""
    import uvicorn
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.middleware.authentication import AuthenticationMiddleware
    from starlette.routing import Mount

    from mcp.server.auth.middleware.bearer_auth import BearerAuthBackend, RequireAuthMiddleware
    from mcp.server.auth.provider import AccessToken
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

    read_token = os.getenv("HEALTH_MCP_AUTH_TOKEN")
    ingest_token = os.getenv("HEALTH_INGEST_TOKEN")
    if not read_token or not ingest_token:
        logger.error("HEALTH_MCP_AUTH_TOKEN and HEALTH_INGEST_TOKEN are required for HTTP transport")
        sys.exit(1)
    if read_token == ingest_token:
        logger.error("HEALTH_MCP_AUTH_TOKEN and HEALTH_INGEST_TOKEN must differ")
        sys.exit(1)

    host = os.getenv("HEALTH_MCP_HTTP_HOST", "0.0.0.0")
    port = int(os.getenv("HEALTH_MCP_HTTP_PORT", "8081"))

    init_database()

    class StaticTokenVerifier:
        """Maps each configured token to the one scope it grants."""

        def __init__(self, scopes_by_token):
            self.scopes_by_token = scopes_by_token

        async def verify_token(self, token: str) -> Optional[AccessToken]:
            for expected, scope in self.scopes_by_token.items():
                if hmac.compare_digest(token.encode(), expected.encode()):
                    return AccessToken(
                        token=token,
                        client_id=scope,
                        scopes=[scope],
                        expires_at=None,
                    )
            return None

    verifier = StaticTokenVerifier({read_token: SCOPE_READ, ingest_token: SCOPE_INGEST})
    # Stateless: no session is retained between requests, so any instance can
    # serve any request and the server can be restarted without breaking clients.
    session_manager = StreamableHTTPSessionManager(app=mcp._mcp_server, stateless=True)

    @asynccontextmanager
    async def lifespan(app):
        async with session_manager.run():
            yield

    def _normalize_path(inner):
        """Give the session manager a non-empty path when mounted at /mcp."""
        async def wrapped(scope, receive, send):
            if scope["type"] == "http" and not scope.get("path"):
                scope = {**scope, "path": "/", "raw_path": b"/"}
            await inner(scope, receive, send)
        return wrapped

    mcp_app = RequireAuthMiddleware(
        _normalize_path(session_manager.handle_request),
        required_scopes=[SCOPE_READ],
    )

    app = Starlette(
        routes=[Mount(MCP_PATH, app=mcp_app)] + build_rest_routes(),
        middleware=[
            Middleware(AuthenticationMiddleware, backend=BearerAuthBackend(verifier)),
        ],
        lifespan=lifespan,
    )

    def _accept_bare_mcp_path(inner):
        """
        Make `/mcp` and `/mcp/` behave identically.

        Starlette compiles Mount("/mcp") to the regex `^/mcp/(?P<path>.*)$`, so
        a request to bare `/mcp` does not match and the router answers with a
        307 redirect to `/mcp/`. Many MCP clients do not follow redirects, and
        some drop the Authorization header when they do. Rewriting the path
        here — outside the router — means both spellings are served directly.
        """
        async def wrapped(scope, receive, send):
            if scope["type"] in ("http", "websocket") and scope.get("path") == MCP_PATH:
                scope = {
                    **scope,
                    "path": MCP_PATH + "/",
                    "raw_path": (MCP_PATH + "/").encode("ascii"),
                }
            await inner(scope, receive, send)
        return wrapped

    logger.info(f"Starting health MCP server on {host}:{port} (database {DB_PATH})")
    logger.info(f"  MCP    : http://{host}:{port}{MCP_PATH}  (and {MCP_PATH}/)")
    logger.info(f"  Ingest : http://{host}:{port}{API_PREFIX}/ingest")
    uvicorn.run(_accept_bare_mcp_path(app), host=host, port=port, log_level="info")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apple Health MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default=os.getenv("HEALTH_MCP_TRANSPORT", "http"),
    )
    args = parser.parse_args()
    if args.transport == "stdio":
        run_stdio()
    else:
        main_http()
