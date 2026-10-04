#!/usr/bin/env python3
"""
Strava downloader for health-mcp

Fetches the athlete profile, activities (with laps, 1 km splits, best efforts,
zones and optionally per-sample streams) and gear from the Strava API v3 into
the same SQLite database the Health Auto Export ingest writes to. Designed to
run from cron.

Strava is here because it holds the running history: Apple Health only knows
what the watch recorded, and only from the day the watch was first worn.

Usage:
    python strava_downloader.py --auth-url         # print the link to authorize with
    python strava_downloader.py --authorize CODE   # exchange the code it yields
    python strava_downloader.py                    # incremental; everything on a first run
    python strava_downloader.py --days 30          # re-sync the last 30 days
    python strava_downloader.py --backfill-detail  # detail only where missing
    python strava_downloader.py --full             # re-fetch detail for ALL activities
    python strava_downloader.py --with-streams     # + per-sample streams where missing

Read-only by design: it asks for activity:read_all and profile:read_all and has
no code path that writes to Strava.
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

import ingest

BASE_URL = "https://www.strava.com/api/v3"
TOKEN_URL = "https://www.strava.com/oauth/token"
AUTHORIZE_URL = "https://www.strava.com/oauth/authorize"
SCOPES = "activity:read_all,profile:read_all"

# Anchored to this file so cron and a manual run share one token, whatever
# their working directory.
TOKEN_PATH = Path(__file__).resolve().parent / ".strava_token.json"

STREAM_KEYS = "time,distance,latlng,altitude,velocity_smooth,heartrate,cadence,grade_smooth"

RUN_TYPES = ("Run", "TrailRun", "VirtualRun")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _upsert(conn: sqlite3.Connection, table: str, data: Dict[str, Any],
            key: Iterable[str] = ("id",)) -> None:
    """
    Insert, or update only the columns given.

    Not INSERT OR REPLACE: that rewrites the whole row, so a list sync — which
    carries no description, device or calories — would blank what the detail
    sync stored. Columns left out of `data` are left alone.
    """
    key = tuple(key)
    columns = list(data)
    updates = ", ".join(f"{c} = excluded.{c}" for c in columns if c not in key)
    conn.execute(
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)}) "
        f"ON CONFLICT({', '.join(key)}) DO UPDATE SET {updates}",
        tuple(data.values()),
    )


class StravaDownloader:
    def __init__(self, skip_auth: bool = False):
        """
        `skip_auth` is for the authorization flow only: exchanging a fresh
        OAuth code must not first refresh with an old token.
        """
        self.db_path = ingest.DB_PATH
        self.client_id = os.getenv("STRAVA_CLIENT_ID", "")
        self.client_secret = os.getenv("STRAVA_CLIENT_SECRET", "")
        self.access_token = ""
        self.refresh_token = ""
        self.expires_at = 0
        self.zones_available = True

        if not self.client_id or not self.client_secret:
            print("❌  STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET must be set in .env "
                  "(https://www.strava.com/settings/api)")
            sys.exit(1)

        self._load_tokens()
        if not self.refresh_token and not skip_auth:
            print("❌  Not authorized yet. Run with --auth-url, open the link, "
                  "then --authorize <code>.")
            sys.exit(1)

        ingest.init_database()
        if not skip_auth:
            self.authenticate()

    # ------------------------------------------------------------------
    # Authentication / token management
    # ------------------------------------------------------------------

    def auth_url(self) -> str:
        # approval_prompt=force so Strava re-asks even if the app was already
        # authorized with narrower scopes.
        return (f"{AUTHORIZE_URL}?client_id={self.client_id}"
                "&redirect_uri=http://localhost&response_type=code"
                f"&approval_prompt=force&scope={SCOPES}")

    def _load_tokens(self) -> None:
        if not TOKEN_PATH.exists():
            return
        data = json.loads(TOKEN_PATH.read_text())
        self.access_token = data.get("access_token", "")
        self.refresh_token = data.get("refresh_token", "")
        self.expires_at = int(data.get("expires_at", 0))

    def _save_tokens(self) -> None:
        # Created 0600 from the start rather than chmod-ed after writing.
        fd = os.open(TOKEN_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump({
                "access_token": self.access_token,
                "refresh_token": self.refresh_token,
                "expires_at": self.expires_at,
            }, fh)

    def _token_request(self, payload: Dict[str, str]) -> Dict[str, Any]:
        resp = None
        for attempt in range(3):
            try:
                resp = requests.post(TOKEN_URL, data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    **payload,
                }, timeout=30)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                wait = 10 * (attempt + 1)
                print(f"    Connection error on attempt {attempt + 1}: {e}. Retrying in {wait}s…")
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                wait = 10 * (attempt + 1)
                print(f"    Strava token endpoint returned {resp.status_code}, retrying in {wait}s…")
                time.sleep(wait)
                continue
            break
        if resp is None or resp.status_code >= 500:
            raise RuntimeError("Strava token endpoint unavailable after 3 attempts — try again shortly.")
        if resp.status_code != 200:
            raise RuntimeError(f"Strava refused the token request ({resp.status_code}): {resp.text[:300]}")
        data = resp.json()
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        self.expires_at = data["expires_at"]
        self._save_tokens()
        return data

    def authenticate(self, force: bool = False) -> None:
        """Refresh the access token if it has expired or is within 5 minutes of it."""
        if not force and self.access_token and time.time() < self.expires_at - 300:
            print("✅  Access token still valid")
            return
        print("🔑  Refreshing Strava access token…")
        self._token_request({"grant_type": "refresh_token", "refresh_token": self.refresh_token})
        print(f"✅  Token refreshed, expires {datetime.fromtimestamp(self.expires_at)}")

    def exchange_code(self, code: str) -> None:
        """Trade a one-time authorization code for tokens and persist them."""
        try:
            data = self._token_request({"grant_type": "authorization_code", "code": code})
        except RuntimeError as e:
            raise RuntimeError(
                f"{e}\nAuthorization codes are single-use and expire quickly — "
                "open the --auth-url link again for a fresh one."
            )
        athlete = data.get("athlete") or {}
        print(f"✅  Authorized as {athlete.get('firstname', '?')} {athlete.get('lastname', '')} "
              f"(athlete {athlete.get('id', '?')}); token saved to {TOKEN_PATH.name}")

    # ------------------------------------------------------------------
    # HTTP client
    # ------------------------------------------------------------------

    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """GET with rate-limit handling, retries and one re-auth on 401."""
        url = f"{BASE_URL}{endpoint}"
        last = "no response"

        for attempt in range(3):
            try:
                resp = requests.get(
                    url, params=params, timeout=30,
                    headers={"Authorization": f"Bearer {self.access_token}"},
                )
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 10 * (attempt + 1)
                last = str(e)[:80]
                print(f"    Network error on {endpoint}: {last}. Retrying in {wait}s…")
                time.sleep(wait)
                continue
            last = str(resp.status_code)

            if resp.status_code == 401:
                if attempt == 0:
                    print("⚠️   401 Unauthorized — forcing token refresh and retrying…")
                    self.authenticate(force=True)
                    continue
                raise RuntimeError(
                    f"401 Unauthorized after token refresh on {endpoint}: {resp.text[:200]}\n"
                    f"Most likely a scope issue — re-authorize (--auth-url) with scope={SCOPES}."
                )

            if resp.status_code == 429:
                usage = resp.headers.get("X-RateLimit-Usage", "?")
                limit = resp.headers.get("X-RateLimit-Limit", "?")
                sleep_secs = 900 - (time.time() % 900) + 5
                print(f"⏳  Rate limited (usage {usage} / limit {limit}) — "
                      f"sleeping {sleep_secs:.0f}s until the window resets…")
                time.sleep(sleep_secs)
                continue

            if resp.status_code in (500, 502, 503, 504):
                wait = 15 * (attempt + 1)
                print(f"    Strava API {resp.status_code} on {endpoint}, retrying in {wait}s…")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        raise RuntimeError(f"Failed to GET {endpoint} after 3 attempts (last: {last})")

    # ------------------------------------------------------------------
    # Athlete
    # ------------------------------------------------------------------

    def download_athlete(self) -> int:
        print("\n📊  Downloading athlete profile…")
        athlete = self._get("/athlete")
        athlete_id = athlete["id"]
        stats = self._get(f"/athletes/{athlete_id}/stats")

        row: Dict[str, Any] = {
            "id": athlete_id,
            "username": athlete.get("username"),
            "firstname": athlete.get("firstname"),
            "lastname": athlete.get("lastname"),
            "city": athlete.get("city"),
            "country": athlete.get("country"),
            "sex": athlete.get("sex"),
            "weight": athlete.get("weight"),
            "measurement_preference": athlete.get("measurement_preference"),
            "created_at": athlete.get("created_at"),
            "stats_json": json.dumps(stats),
            "synced_at": _now(),
        }
        for prefix in ("recent", "ytd", "all"):
            totals = stats.get(f"{prefix}_run_totals") or {}
            row[f"{prefix}_run_count"] = totals.get("count")
            row[f"{prefix}_run_distance"] = totals.get("distance")
            row[f"{prefix}_run_moving_time"] = totals.get("moving_time")

        with sqlite3.connect(self.db_path) as conn:
            _upsert(conn, "strava_athletes", row)
            conn.commit()

        print(f"✅  Athlete: {athlete.get('firstname')} {athlete.get('lastname')}")
        return athlete_id

    # ------------------------------------------------------------------
    # Activities
    # ------------------------------------------------------------------

    @staticmethod
    def _activity_row(a: Dict) -> Dict[str, Any]:
        """Columns both SummaryActivity and DetailedActivity carry."""
        start = a.get("start_latlng") or []
        return {
            "id": a["id"],
            "athlete_id": (a.get("athlete") or {}).get("id"),
            "name": a.get("name"),
            "type": a.get("type"),
            "sport_type": a.get("sport_type"),
            "workout_type": a.get("workout_type"),
            "start_date": a.get("start_date"),
            "start_date_local": a.get("start_date_local"),
            "timezone": a.get("timezone"),
            "utc_offset": a.get("utc_offset"),
            "distance": a.get("distance"),
            "moving_time": a.get("moving_time"),
            "elapsed_time": a.get("elapsed_time"),
            "total_elevation_gain": a.get("total_elevation_gain"),
            "elev_high": a.get("elev_high"),
            "elev_low": a.get("elev_low"),
            "average_speed": a.get("average_speed"),
            "max_speed": a.get("max_speed"),
            "has_heartrate": int(bool(a.get("has_heartrate"))),
            "average_heartrate": a.get("average_heartrate"),
            "max_heartrate": a.get("max_heartrate"),
            "average_cadence": a.get("average_cadence"),
            "average_watts": a.get("average_watts"),
            "average_temp": a.get("average_temp"),
            "suffer_score": a.get("suffer_score"),
            "start_lat": start[0] if len(start) >= 2 else None,
            "start_lng": start[1] if len(start) >= 2 else None,
            "map_summary_polyline": (a.get("map") or {}).get("summary_polyline"),
            "trainer": int(bool(a.get("trainer"))),
            "manual": int(bool(a.get("manual"))),
            "commute": int(bool(a.get("commute"))),
            "private": int(bool(a.get("private"))),
            "visibility": a.get("visibility"),
            "pr_count": a.get("pr_count", 0),
            "achievement_count": a.get("achievement_count", 0),
            "gear_id": a.get("gear_id"),
            "external_id": a.get("external_id"),
            "upload_id": a.get("upload_id"),
            "synced_at": _now(),
        }

    def download_activities(self, days_back: Optional[int] = None,
                            since: Optional[str] = None) -> List[int]:
        """
        Fetch the activity list. Returns the ids that were not in the database.

        Cutoff, in order of precedence: --since, --days, the newest activity
        already stored minus a day, STRAVA_START_DATE, and otherwise none at
        all — a first run takes the whole history, which is the point of
        having Strava here.
        """
        print("\n🏃  Downloading activities…")
        after: Optional[int] = None
        if since is not None:
            try:
                cutoff = datetime.fromisoformat(since).replace(tzinfo=timezone.utc)
            except ValueError:
                raise ValueError(f"--since date '{since}' is not a valid YYYY-MM-DD date")
            after = int(cutoff.timestamp())
            print(f"    Since {cutoff.date()} (--since)")
        elif days_back is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
            after = int(cutoff.timestamp())
            print(f"    Since {cutoff.date()} ({days_back} days back)")
        else:
            with sqlite3.connect(self.db_path) as conn:
                latest = conn.execute("SELECT MAX(start_date) FROM strava_activities").fetchone()[0]
            if latest:
                cutoff = datetime.fromisoformat(latest.replace("Z", "+00:00")) - timedelta(days=1)
                after = int(cutoff.timestamp())
                print(f"    Incremental sync from {cutoff.date()}")
            elif os.getenv("STRAVA_START_DATE"):
                cutoff = datetime.fromisoformat(os.environ["STRAVA_START_DATE"]).replace(tzinfo=timezone.utc)
                after = int(cutoff.timestamp())
                print(f"    First sync from {cutoff.date()} (STRAVA_START_DATE)")
            else:
                print("    First sync: the whole history")

        new_ids: List[int] = []
        page, total = 1, 0
        while True:
            params: Dict[str, Any] = {"per_page": 200, "page": page}
            if after:
                params["after"] = after
            batch = self._get("/athlete/activities", params=params)
            if not batch:
                break
            with sqlite3.connect(self.db_path) as conn:
                for a in batch:
                    if not conn.execute("SELECT 1 FROM strava_activities WHERE id = ?",
                                        (a["id"],)).fetchone():
                        new_ids.append(a["id"])
                    _upsert(conn, "strava_activities", self._activity_row(a))
                conn.commit()
            total += len(batch)
            print(f"    Page {page}: {len(batch)} activities ({total} total, {len(new_ids)} new)")
            page += 1
            time.sleep(0.3)

        print(f"✅  Activities: {total} fetched, {len(new_ids)} new")
        return new_ids

    # ------------------------------------------------------------------
    # Activity detail (laps, splits, best efforts, zones)
    # ------------------------------------------------------------------

    def download_activity_details(self, activity_id: int) -> None:
        detail = self._get(f"/activities/{activity_id}")

        with sqlite3.connect(self.db_path) as conn:
            row = self._activity_row(detail)
            row.update({
                "description": detail.get("description"),
                "device_name": detail.get("device_name"),
                "calories": detail.get("calories"),
                "perceived_exertion": detail.get("perceived_exertion"),
                "detail_synced_at": _now(),
            })
            _upsert(conn, "strava_activities", row)

            conn.execute("DELETE FROM strava_activity_laps WHERE activity_id = ?", (activity_id,))
            for lap in detail.get("laps") or []:
                _upsert(conn, "strava_activity_laps", {
                    "id": lap.get("id"),
                    "activity_id": activity_id,
                    "lap_index": lap.get("lap_index"),
                    "name": lap.get("name"),
                    "start_date": lap.get("start_date"),
                    "start_date_local": lap.get("start_date_local"),
                    "elapsed_time": lap.get("elapsed_time"),
                    "moving_time": lap.get("moving_time"),
                    "distance": lap.get("distance"),
                    "total_elevation_gain": lap.get("total_elevation_gain"),
                    "average_speed": lap.get("average_speed"),
                    "max_speed": lap.get("max_speed"),
                    "average_cadence": lap.get("average_cadence"),
                    "average_heartrate": lap.get("average_heartrate"),
                    "max_heartrate": lap.get("max_heartrate"),
                    "pace_zone": lap.get("pace_zone"),
                    "start_index": lap.get("start_index"),
                    "end_index": lap.get("end_index"),
                })

            conn.execute("DELETE FROM strava_activity_splits WHERE activity_id = ?", (activity_id,))
            for sp in detail.get("splits_metric") or []:
                _upsert(conn, "strava_activity_splits", {
                    "activity_id": activity_id,
                    "split": sp.get("split"),
                    "distance": sp.get("distance"),
                    "elapsed_time": sp.get("elapsed_time"),
                    "moving_time": sp.get("moving_time"),
                    "elevation_difference": sp.get("elevation_difference"),
                    "average_speed": sp.get("average_speed"),
                    "average_grade_adjusted_speed": sp.get("average_grade_adjusted_speed"),
                    "average_heartrate": sp.get("average_heartrate"),
                    "pace_zone": sp.get("pace_zone"),
                }, key=("activity_id", "split"))

            conn.execute("DELETE FROM strava_best_efforts WHERE activity_id = ?", (activity_id,))
            for be in detail.get("best_efforts") or []:
                _upsert(conn, "strava_best_efforts", {
                    "id": be.get("id"),
                    "activity_id": activity_id,
                    "name": be.get("name"),
                    "distance": be.get("distance"),
                    "elapsed_time": be.get("elapsed_time"),
                    "moving_time": be.get("moving_time"),
                    "start_date": be.get("start_date"),
                    "start_date_local": be.get("start_date_local"),
                    "pr_rank": be.get("pr_rank"),
                })
            conn.commit()

        self._download_zones(activity_id)

    def _download_zones(self, activity_id: int) -> None:
        """Time in zone — a subscriber feature, so stop asking after one refusal."""
        if not self.zones_available:
            return
        try:
            zones = self._get(f"/activities/{activity_id}/zones")
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in (402, 403):
                self.zones_available = False
                print(f"    ℹ️  Zones need a Strava subscription ({status}) — skipping them for this run")
            else:
                print(f"    ⚠️  Zones not available for activity {activity_id}: {e}")
            return
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM strava_activity_zones WHERE activity_id = ?", (activity_id,))
            for group in zones or []:
                for idx, bucket in enumerate(group.get("distribution_buckets") or []):
                    _upsert(conn, "strava_activity_zones", {
                        "activity_id": activity_id,
                        "zone_type": group.get("type", "unknown"),
                        "zone_index": idx,
                        "zone_min": bucket.get("min"),
                        "zone_max": bucket.get("max"),
                        "time": bucket.get("time"),
                    }, key=("activity_id", "zone_type", "zone_index"))
            conn.commit()

    def _ids(self, where: str = "") -> List[int]:
        with sqlite3.connect(self.db_path) as conn:
            return [r[0] for r in conn.execute(
                f"SELECT id FROM strava_activities {where} ORDER BY start_date DESC")]

    # ------------------------------------------------------------------
    # Streams
    # ------------------------------------------------------------------

    def download_streams(self, activity_id: int) -> int:
        """Per-sample streams for one activity. Returns the number of samples."""
        try:
            streams = self._get(f"/activities/{activity_id}/streams",
                                params={"keys": STREAM_KEYS, "key_by_type": "true"})
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                streams = {}        # manual entries have none
            else:
                raise

        def series(key: str) -> List[Any]:
            return (streams.get(key) or {}).get("data") or []

        times = series("time")
        columns = {
            "distance": series("distance"), "altitude": series("altitude"),
            "velocity": series("velocity_smooth"), "heartrate": series("heartrate"),
            "cadence": series("cadence"), "grade": series("grade_smooth"),
        }
        latlng = series("latlng")

        def at(values: List[Any], i: int) -> Any:
            return values[i] if i < len(values) else None

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM strava_activity_streams WHERE activity_id = ?", (activity_id,))
            conn.executemany(
                """INSERT INTO strava_activity_streams
                   (activity_id, idx, time, distance, lat, lng, altitude,
                    velocity, heartrate, cadence, grade)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                [(
                    activity_id, i, t, at(columns["distance"], i),
                    (at(latlng, i) or [None, None])[0], (at(latlng, i) or [None, None])[1],
                    at(columns["altitude"], i), at(columns["velocity"], i),
                    at(columns["heartrate"], i), at(columns["cadence"], i),
                    at(columns["grade"], i),
                ) for i, t in enumerate(times)],
            )
            conn.execute("UPDATE strava_activities SET streams_synced_at = ? WHERE id = ?",
                         (_now(), activity_id))
            conn.commit()
        return len(times)

    # ------------------------------------------------------------------
    # Gear
    # ------------------------------------------------------------------

    def download_gear(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            gear_ids = [r[0] for r in conn.execute(
                "SELECT DISTINCT gear_id FROM strava_activities WHERE gear_id IS NOT NULL")]
        if not gear_ids:
            return
        print(f"\n👟  Downloading {len(gear_ids)} gear item(s)…")
        with sqlite3.connect(self.db_path) as conn:
            for gear_id in gear_ids:
                try:
                    g = self._get(f"/gear/{gear_id}")
                except Exception as e:                       # noqa: BLE001
                    print(f"    ⚠️  Could not fetch gear {gear_id}: {e}")
                    continue
                _upsert(conn, "strava_gear", {
                    "id": g["id"],
                    "name": g.get("name"),
                    "brand_name": g.get("brand_name"),
                    "model_name": g.get("model_name"),
                    "description": g.get("description"),
                    "distance": g.get("distance"),
                    "gear_type": "bike" if gear_id.startswith("b") else "shoe",
                    "primary_gear": int(bool(g.get("primary"))),
                    "retired": int(bool(g.get("retired"))),
                    "synced_at": _now(),
                })
                time.sleep(0.2)
            conn.commit()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def print_summary(self) -> None:
        print("\n" + "=" * 60)
        print("STRAVA SYNC SUMMARY")
        print("=" * 60)
        with sqlite3.connect(self.db_path) as conn:
            total, earliest, latest, with_detail, with_streams = conn.execute(
                """SELECT COUNT(*), MIN(start_date_local), MAX(start_date_local),
                          SUM(detail_synced_at IS NOT NULL), SUM(streams_synced_at IS NOT NULL)
                   FROM strava_activities""").fetchone()
            print(f"Activities : {total} total ({with_detail or 0} with detail, "
                  f"{with_streams or 0} with streams)")
            print(f"Date range : {(earliest or '?')[:10]} → {(latest or '?')[:10]}")
            print("\nBy sport type:")
            for sport, count, km in conn.execute(
                    """SELECT sport_type, COUNT(*), ROUND(SUM(distance) / 1000.0, 1)
                       FROM strava_activities GROUP BY sport_type ORDER BY 2 DESC"""):
                print(f"  {sport or 'Unknown':20s} {count:5d} activities  {km or 0:8.1f} km")
            efforts = conn.execute("SELECT COUNT(*) FROM strava_best_efforts").fetchone()[0]
            print(f"\nBest efforts: {efforts}")
        print(f"Database    : {self.db_path}")
        print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Download Strava data into the health-mcp database")
    cutoff = parser.add_mutually_exclusive_group()
    cutoff.add_argument("--since", metavar="DATE",
                        help="Sync activities on or after this date (YYYY-MM-DD)")
    cutoff.add_argument("--days", type=int, default=None,
                        help="Sync activities from this many days back")
    parser.add_argument("--full", action="store_true",
                        help="Re-fetch detail for ALL activities, not just new ones")
    parser.add_argument("--backfill-detail", action="store_true",
                        help="Fetch detail only for activities that have none yet")
    parser.add_argument("--with-streams", action="store_true",
                        help="Also fetch per-sample streams for activities that lack them")
    parser.add_argument("--auth-url", action="store_true",
                        help="Print the link to open in a browser to authorize, and exit")
    parser.add_argument("--authorize", metavar="CODE",
                        help="Exchange the one-time code from that link for tokens, and exit")
    args = parser.parse_args()

    if args.auth_url:
        print(StravaDownloader(skip_auth=True).auth_url())
        return
    if args.authorize:
        StravaDownloader(skip_auth=True).exchange_code(args.authorize)
        return

    downloader = StravaDownloader()
    downloader.download_athlete()
    new_ids = downloader.download_activities(days_back=args.days, since=args.since)

    if args.full:
        ids_for_detail = downloader._ids()
        print(f"\n📋  --full: re-fetching detail for all {len(ids_for_detail)} activities")
    elif args.backfill_detail:
        ids_for_detail = downloader._ids("WHERE detail_synced_at IS NULL")
        print(f"\n📋  --backfill-detail: {len(ids_for_detail)} activities missing detail")
    else:
        ids_for_detail = new_ids
        if ids_for_detail:
            print(f"\n📋  Fetching detail for {len(ids_for_detail)} new activities")

    for i, activity_id in enumerate(ids_for_detail, 1):
        if i % 10 == 0 or i == len(ids_for_detail):
            print(f"    Detail {i}/{len(ids_for_detail)} (activity {activity_id})")
        try:
            downloader.download_activity_details(activity_id)
        except Exception as e:                               # noqa: BLE001
            print(f"    ⚠️  Failed detail for {activity_id}: {e}")
        time.sleep(0.6)

    if args.with_streams:
        ids = downloader._ids("WHERE streams_synced_at IS NULL AND manual = 0")
        print(f"\n📈  Fetching streams for {len(ids)} activities")
        for i, activity_id in enumerate(ids, 1):
            try:
                n = downloader.download_streams(activity_id)
            except Exception as e:                           # noqa: BLE001
                print(f"    ⚠️  Failed streams for {activity_id}: {e}")
                continue
            if i % 10 == 0 or i == len(ids):
                print(f"    Streams {i}/{len(ids)} (activity {activity_id}, {n} samples)")
            time.sleep(0.6)

    downloader.download_gear()
    downloader.print_summary()


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        print(f"\n❌  {e}", file=sys.stderr)
        sys.exit(1)
