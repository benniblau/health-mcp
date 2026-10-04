"""
Database side of strava_downloader.py, against a stubbed API.

    .venv/bin/python -m unittest discover tests

Nothing here talks to Strava: `_get` is replaced, and the database is a
temporary file.
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

_tmp = tempfile.mkdtemp()
os.environ["HEALTH_DB_PATH"] = str(Path(_tmp) / "test.db")
os.environ["STRAVA_CLIENT_ID"] = "1"
os.environ["STRAVA_CLIENT_SECRET"] = "x"
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests

import ingest
import strava_downloader as sd

SUMMARY = {
    "id": 100, "athlete": {"id": 7}, "name": "Morning Run", "type": "Run",
    "sport_type": "Run", "start_date": "2026-09-27T07:00:00Z",
    "start_date_local": "2026-09-27T09:00:00Z", "distance": 10000.0,
    "moving_time": 3000, "elapsed_time": 3100, "total_elevation_gain": 50.0,
    "average_speed": 3.33, "has_heartrate": True, "average_heartrate": 150.0,
    "average_cadence": 85.0, "start_latlng": [49.3, 8.4],
    "map": {"summary_polyline": "abc"}, "gear_id": "g1",
}
DETAIL = {
    **SUMMARY, "description": "felt good", "device_name": "Strava iPhone App",
    "calories": 650.0,
    "laps": [{"id": 1, "lap_index": 1, "distance": 10000.0, "elapsed_time": 3100}],
    "splits_metric": [{"split": i, "distance": 1000.0, "moving_time": 300} for i in range(1, 11)],
    "best_efforts": [
        {"id": 11, "name": "5K", "distance": 5000, "elapsed_time": 1450, "pr_rank": 1},
        {"id": 12, "name": "10K", "distance": 10000, "elapsed_time": 3000, "pr_rank": None},
    ],
}
STREAMS = {
    "time": {"data": [0, 1, 2]}, "distance": {"data": [0.0, 3.1, 6.3]},
    "latlng": {"data": [[49.3, 8.4], [49.3001, 8.4001], [49.3002, 8.4002]]},
    "heartrate": {"data": [120, 121]},          # shorter than time, as Strava sometimes sends
}


class FakeApi:
    def __init__(self):
        self.calls = []
        self.zones_status = 402

    def __call__(self, endpoint, params=None):
        self.calls.append(endpoint)
        if endpoint == "/athlete":
            return {"id": 7, "firstname": "K", "lastname": "B"}
        if endpoint == "/athletes/7/stats":
            return {"all_run_totals": {"count": 3, "distance": 30000.0, "moving_time": 9000}}
        if endpoint == "/athlete/activities":
            return [SUMMARY] if params["page"] == 1 else []
        if endpoint == "/activities/100":
            return DETAIL
        if endpoint == "/activities/100/zones":
            response = requests.Response()
            response.status_code = self.zones_status
            raise requests.exceptions.HTTPError(response=response)
        if endpoint == "/activities/100/streams":
            return STREAMS
        if endpoint == "/gear/g1":
            return {"id": "g1", "name": "Pegasus", "distance": 420000.0}
        raise AssertionError(f"unexpected call {endpoint}")


def make_downloader():
    d = sd.StravaDownloader(skip_auth=True)
    d._get = FakeApi()
    return d


def one(sql, *args):
    with sqlite3.connect(ingest.DB_PATH) as conn:
        return conn.execute(sql, args).fetchone()


class StravaDownloaderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sd.time.sleep = lambda *_: None
        cls.d = make_downloader()
        cls.d.download_athlete()
        cls.new_ids = cls.d.download_activities()
        cls.d.download_activity_details(100)

    def test_first_sync_reports_new_activity(self):
        self.assertEqual(self.new_ids, [100])
        self.assertEqual(one("SELECT all_run_count FROM strava_athletes")[0], 3)

    def test_detail_tables(self):
        self.assertEqual(one("SELECT COUNT(*) FROM strava_activity_splits")[0], 10)
        self.assertEqual(one("SELECT COUNT(*) FROM strava_activity_laps")[0], 1)
        self.assertEqual(one("SELECT elapsed_time FROM strava_best_efforts WHERE name='5K'")[0], 1450)

    def test_list_sync_does_not_blank_detail_columns(self):
        """The regression strava-mcp works around with DETAIL_ONLY_COLUMNS."""
        again = self.d.download_activities()
        self.assertEqual(again, [])
        device, description, calories, synced = one(
            "SELECT device_name, description, calories, detail_synced_at "
            "FROM strava_activities WHERE id = 100")
        self.assertEqual((device, description, calories), ("Strava iPhone App", "felt good", 650.0))
        self.assertIsNotNone(synced)

    def test_detail_resync_does_not_duplicate(self):
        self.d.zones_available = True
        self.d.download_activity_details(100)
        self.assertEqual(one("SELECT COUNT(*) FROM strava_best_efforts")[0], 2)
        self.assertEqual(one("SELECT COUNT(*) FROM strava_activities")[0], 1)

    def test_zones_refusal_stops_further_requests(self):
        self.d.zones_available = True
        self.d._download_zones(100)
        self.assertFalse(self.d.zones_available)
        before = len(self.d._get.calls)
        self.d._download_zones(100)
        self.assertEqual(len(self.d._get.calls), before)

    def test_streams_with_ragged_series(self):
        self.assertEqual(self.d.download_streams(100), 3)
        self.assertEqual(one("SELECT lat, heartrate FROM strava_activity_streams WHERE idx = 1"),
                         (49.3001, 121))
        self.assertEqual(one("SELECT heartrate FROM strava_activity_streams WHERE idx = 2")[0], None)
        self.assertIsNotNone(one("SELECT streams_synced_at FROM strava_activities")[0])

    def test_views(self):
        self.d.download_gear()
        km, pace, spm, gear = one(
            "SELECT distance_km, pace_min_per_km, cadence_spm, gear_name FROM strava_activity_summary")
        self.assertEqual((km, pace, spm, gear), (10.0, 5.0, 170.0, "Pegasus"))
        # 2026-09-27 is a Sunday: its week starts Monday the 21st.
        self.assertEqual(one("SELECT week_start, runs, total_km FROM strava_weekly_running"),
                         ("2026-09-21", 1, 10.0))
        self.assertEqual(one("SELECT month, total_km FROM strava_monthly_stats"), ("2026-09", 10.0))

    def test_monday_belongs_to_its_own_week(self):
        with sqlite3.connect(ingest.DB_PATH) as conn:
            self.assertEqual(conn.execute(
                "SELECT date('2026-09-21T06:00:00Z', 'weekday 0', '-6 days')").fetchone()[0],
                "2026-09-21")


if __name__ == "__main__":
    unittest.main()
