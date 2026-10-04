#!/usr/bin/env python3
"""
Ingest side of health-mcp.

Health Auto Export pushes Apple Health data as JSON; this module owns the
database and everything that writes to it. `mcp_server.py` imports it for the
POST route, and it doubles as a small CLI for looking at what has arrived:

    python ingest.py --list                 # recent requests and what they held
    python ingest.py --dump 12              # raw body of request 12, to stdout
    python ingest.py --dump 12 -o run.json  # ... or to a file (test fixtures)

Phase 1 only archives. Each body is stored unmodified so the parser, when it
exists, can be run — and re-run — over everything received so far.
"""

import argparse
import gzip
import hashlib
import json
import os
import sqlite3
import sys
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = BASE_DIR / "schema" / "schema_health.sql"
# Resolved against this file, not the caller's cwd: cron runs from the home
# directory, and a relative path would quietly start a second database there.
DB_PATH = str(BASE_DIR / os.getenv("HEALTH_DB_PATH", "health.db"))

# Never written to the archive, whatever else the phone sends.
SECRET_HEADERS = {"authorization", "cookie", "proxy-authorization"}


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_database() -> None:
    """Create the database if needed. Safe to call on every start."""
    with get_db() as conn:
        # WAL so MCP reads are not blocked while a large export is written.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(SCHEMA_PATH.read_text())
        conn.commit()


def summarise(data: Dict[str, Any]) -> Dict[str, Any]:
    """Count what a payload holds, per data type, without interpreting it."""
    counts: Dict[str, Any] = {
        key: len(value) if isinstance(value, list) else 1
        for key, value in data.items()
    }
    metrics = {
        str(m.get("name")): len(m.get("data") or [])
        for m in data.get("metrics") or [] if isinstance(m, dict)
    }
    workouts = Counter(
        str(w.get("name")) for w in data.get("workouts") or [] if isinstance(w, dict)
    )
    return {"types": counts, "metrics": metrics, "workouts": dict(workouts)}


def store_payload(raw: bytes, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Archive one Health Auto Export request.

    Raises ValueError for anything that is not the documented envelope
    (`{"data": {...}}`), so a misconfigured automation fails visibly on the
    phone rather than filling the archive with bodies nothing can parse.
    """
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"body is not valid JSON: {e}") from e
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        raise ValueError('expected a JSON object with a "data" object — '
                         "is the automation's export format set to JSON?")

    counts = summarise(data)
    digest = hashlib.sha256(raw).hexdigest()
    safe_headers = {
        k: v for k, v in (headers or {}).items() if k.lower() not in SECRET_HEADERS
    }

    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO ingest_log
                (received_at, sha256, bytes, headers_json, counts_json, body_gz)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                digest,
                len(raw),
                json.dumps(safe_headers),
                json.dumps(counts),
                gzip.compress(raw),
            ),
        )
        conn.commit()
        duplicate = cur.rowcount == 0
        row = conn.execute(
            "SELECT id FROM ingest_log WHERE sha256 = ?", (digest,)
        ).fetchone()

    return {"id": row["id"], "duplicate": duplicate, "bytes": len(raw), **counts}


def recent_ingests(limit: int = 20) -> Dict[str, Any]:
    """The newest requests, without their bodies."""
    with get_db() as conn:
        total, last = conn.execute(
            "SELECT COUNT(*), MAX(received_at) FROM ingest_log"
        ).fetchone()
        rows = conn.execute(
            """
            SELECT id, received_at, bytes, counts_json, parsed_at
            FROM ingest_log ORDER BY id DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return {
        "requests": total,
        "last_received_at": last,
        "recent": [
            {
                "id": r["id"],
                "received_at": r["received_at"],
                "bytes": r["bytes"],
                "parsed_at": r["parsed_at"],
                "contents": json.loads(r["counts_json"] or "{}"),
            }
            for r in rows
        ],
    }


def load_body(ingest_id: int) -> bytes:
    with get_db() as conn:
        row = conn.execute(
            "SELECT body_gz FROM ingest_log WHERE id = ?", (ingest_id,)
        ).fetchone()
    if row is None:
        raise ValueError(f"no ingest with id {ingest_id}")
    return gzip.decompress(row["body_gz"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect archived Health Auto Export requests")
    parser.add_argument("--list", action="store_true", help="show recent requests")
    parser.add_argument("--dump", type=int, metavar="ID", help="write one raw body")
    parser.add_argument("-o", "--output", help="file for --dump (default: stdout)")
    args = parser.parse_args()

    init_database()
    if args.dump is not None:
        try:
            body = load_body(args.dump)
        except ValueError as e:
            sys.exit(str(e))
        if args.output:
            Path(args.output).write_bytes(body)
        else:
            sys.stdout.buffer.write(body)
    else:
        print(json.dumps(recent_ingests(), indent=2))
