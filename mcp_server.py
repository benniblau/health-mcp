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
import sys
from contextlib import asynccontextmanager
from typing import Any, Optional

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


def _json(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)


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
    return _json(ingest.recent_ingests(max(1, min(limit, 200))))


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
