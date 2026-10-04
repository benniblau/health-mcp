-- =============================================================================
-- health-mcp — SQLite schema
-- =============================================================================
-- Single source of truth: init_database() executes this file directly.
-- Every table is IF NOT EXISTS, so re-running is safe. Views (once there are
-- any) must be dropped and recreated, as in coros-mcp, or an existing database
-- keeps a stale definition.
-- =============================================================================

-- One row per request Health Auto Export sends. The body is kept unmodified
-- (gzipped) so the parser can be re-run over history: anything it gets wrong
-- or does not yet understand is fixable without re-exporting from the phone.
CREATE TABLE IF NOT EXISTS ingest_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at   TEXT    NOT NULL,           -- UTC, ISO 8601
    sha256        TEXT    NOT NULL UNIQUE,    -- of the raw body; resends are dropped
    bytes         INTEGER NOT NULL,           -- raw body size
    headers_json  TEXT,                       -- request headers, credentials removed
    counts_json   TEXT,                       -- what the payload contained, per data type
    parsed_at     TEXT,                       -- NULL until the parser has processed it
    body_gz       BLOB    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ingest_log_received ON ingest_log(received_at);
CREATE INDEX IF NOT EXISTS idx_ingest_log_parsed   ON ingest_log(parsed_at);

-- =============================================================================
-- Strava
-- =============================================================================
-- Filled by strava_downloader.py from the Strava API v3. Everything is
-- prefixed strava_ because this database also holds Apple Health data, and the
-- two describe the same runs: a workout recorded on the watch and synced to
-- Strava exists once in each. Columns keep Strava's own names and units
-- (metres, seconds, m/s); conversions live in the views.

CREATE TABLE IF NOT EXISTS strava_athletes (
    id                          INTEGER PRIMARY KEY,
    username                    TEXT,
    firstname                   TEXT,
    lastname                    TEXT,
    city                        TEXT,
    country                     TEXT,
    sex                         TEXT,
    weight                      REAL,       -- kg
    measurement_preference      TEXT,
    created_at                  TEXT,
    -- Run totals from /athletes/{id}/stats; the rest of that payload is in stats_json
    recent_run_count            INTEGER,    -- last 4 weeks
    recent_run_distance         REAL,
    recent_run_moving_time      INTEGER,
    ytd_run_count               INTEGER,
    ytd_run_distance            REAL,
    ytd_run_moving_time         INTEGER,
    all_run_count               INTEGER,
    all_run_distance            REAL,
    all_run_moving_time         INTEGER,
    stats_json                  TEXT,
    synced_at                   TEXT
);

CREATE TABLE IF NOT EXISTS strava_gear (
    id              TEXT PRIMARY KEY,
    name            TEXT,
    brand_name      TEXT,
    model_name      TEXT,
    description     TEXT,
    distance        REAL,       -- metres
    gear_type       TEXT,       -- 'bike' or 'shoe'
    primary_gear    INTEGER DEFAULT 0,
    retired         INTEGER DEFAULT 0,
    synced_at       TEXT
);

CREATE TABLE IF NOT EXISTS strava_activities (
    id                      INTEGER PRIMARY KEY,
    athlete_id              INTEGER,
    name                    TEXT,
    type                    TEXT,
    sport_type              TEXT,
    workout_type            INTEGER,    -- runs: 0 default, 1 race, 2 long run, 3 workout

    start_date              TEXT,       -- UTC ISO 8601
    start_date_local        TEXT,       -- local wall time, though Strava labels it Z
    timezone                TEXT,
    utc_offset              REAL,

    distance                REAL,       -- metres
    moving_time             INTEGER,    -- seconds
    elapsed_time            INTEGER,    -- seconds
    total_elevation_gain    REAL,       -- metres
    elev_high               REAL,
    elev_low                REAL,
    average_speed           REAL,       -- m/s
    max_speed               REAL,       -- m/s

    has_heartrate           INTEGER DEFAULT 0,
    average_heartrate       REAL,
    max_heartrate           REAL,
    average_cadence         REAL,       -- runs: strides per minute for ONE foot; double for spm
    average_watts           REAL,
    average_temp            INTEGER,
    suffer_score            INTEGER,    -- "Relative Effort"

    start_lat               REAL,
    start_lng               REAL,
    map_summary_polyline    TEXT,

    trainer                 INTEGER DEFAULT 0,
    manual                  INTEGER DEFAULT 0,
    commute                 INTEGER DEFAULT 0,
    private                 INTEGER DEFAULT 0,
    visibility              TEXT,
    pr_count                INTEGER DEFAULT 0,
    achievement_count       INTEGER DEFAULT 0,
    gear_id                 TEXT,
    external_id             TEXT,       -- what the uploading app called the file
    upload_id               INTEGER,

    -- Only GET /activities/{id} carries these. The list sync never writes
    -- them, so it cannot blank them either.
    description             TEXT,
    device_name             TEXT,       -- "Apple Watch …", "Strava iPhone App", …
    calories                REAL,
    perceived_exertion      REAL,

    synced_at               TEXT,
    detail_synced_at        TEXT,       -- NULL until detail has been fetched
    streams_synced_at       TEXT        -- NULL until streams have been fetched
);

CREATE INDEX IF NOT EXISTS idx_strava_activities_start ON strava_activities(start_date);
CREATE INDEX IF NOT EXISTS idx_strava_activities_local ON strava_activities(start_date_local);
CREATE INDEX IF NOT EXISTS idx_strava_activities_sport ON strava_activities(sport_type);

CREATE TABLE IF NOT EXISTS strava_activity_laps (
    id                      INTEGER PRIMARY KEY,
    activity_id             INTEGER NOT NULL,
    lap_index               INTEGER,
    name                    TEXT,
    start_date              TEXT,
    start_date_local        TEXT,
    elapsed_time            INTEGER,
    moving_time             INTEGER,
    distance                REAL,
    total_elevation_gain    REAL,
    average_speed           REAL,
    max_speed               REAL,
    average_cadence         REAL,
    average_heartrate       REAL,
    max_heartrate           REAL,
    pace_zone               INTEGER,
    start_index             INTEGER,
    end_index               INTEGER
);

CREATE INDEX IF NOT EXISTS idx_strava_laps_activity ON strava_activity_laps(activity_id);

-- 1 km splits
CREATE TABLE IF NOT EXISTS strava_activity_splits (
    activity_id                     INTEGER NOT NULL,
    split                           INTEGER NOT NULL,
    distance                        REAL,
    elapsed_time                    INTEGER,
    moving_time                     INTEGER,
    elevation_difference            REAL,
    average_speed                   REAL,
    average_grade_adjusted_speed    REAL,
    average_heartrate               REAL,
    pace_zone                       INTEGER,
    PRIMARY KEY (activity_id, split)
);

-- Strava's fastest stretch over each standard distance within one run
-- (400m, 1K, 1 mile, 5K, 10K, half marathon, …). pr_rank 1 = all-time best
-- at the moment the activity was processed.
CREATE TABLE IF NOT EXISTS strava_best_efforts (
    id                  INTEGER PRIMARY KEY,
    activity_id         INTEGER NOT NULL,
    name                TEXT,
    distance            REAL,
    elapsed_time        INTEGER,
    moving_time         INTEGER,
    start_date          TEXT,
    start_date_local    TEXT,
    pr_rank             INTEGER
);

CREATE INDEX IF NOT EXISTS idx_strava_best_activity ON strava_best_efforts(activity_id);
CREATE INDEX IF NOT EXISTS idx_strava_best_name     ON strava_best_efforts(name, elapsed_time);

-- Time in zone. Strava only answers this for subscribers.
CREATE TABLE IF NOT EXISTS strava_activity_zones (
    activity_id     INTEGER NOT NULL,
    zone_type       TEXT NOT NULL,      -- 'heartrate' or 'pace'
    zone_index      INTEGER NOT NULL,
    zone_min        REAL,
    zone_max        REAL,
    time            INTEGER,            -- seconds
    PRIMARY KEY (activity_id, zone_type, zone_index)
);

-- Per-sample streams, only with --with-streams.
CREATE TABLE IF NOT EXISTS strava_activity_streams (
    activity_id     INTEGER NOT NULL,
    idx             INTEGER NOT NULL,
    time            INTEGER,            -- seconds since start
    distance        REAL,               -- metres since start
    lat             REAL,
    lng             REAL,
    altitude        REAL,
    velocity        REAL,               -- m/s, smoothed
    heartrate       INTEGER,
    cadence         INTEGER,
    grade           REAL,               -- percent, smoothed
    PRIMARY KEY (activity_id, idx)
);

-- -----------------------------------------------------------------------------
-- Views — dropped and recreated so a changed definition reaches an existing
-- database.
-- -----------------------------------------------------------------------------

DROP VIEW IF EXISTS strava_activity_summary;
CREATE VIEW strava_activity_summary AS
SELECT
    a.id,
    a.name,
    a.sport_type,
    a.workout_type,
    a.start_date_local,
    date(a.start_date_local)                        AS date,
    ROUND(a.distance / 1000.0, 2)                   AS distance_km,
    a.moving_time,
    ROUND(a.moving_time / 60.0, 1)                  AS moving_time_min,
    ROUND(a.elapsed_time / 60.0, 1)                 AS elapsed_time_min,
    CASE WHEN a.distance > 0
         THEN ROUND((a.moving_time / 60.0) / (a.distance / 1000.0), 2)
    END                                             AS pace_min_per_km,
    a.total_elevation_gain,
    a.average_heartrate,
    a.max_heartrate,
    ROUND(a.average_cadence * 2, 0)                 AS cadence_spm,
    a.suffer_score,
    a.calories,
    a.perceived_exertion,
    a.trainer,
    a.manual,
    a.device_name,
    g.name                                          AS gear_name,
    a.detail_synced_at IS NOT NULL                  AS has_detail
FROM strava_activities a
LEFT JOIN strava_gear g ON a.gear_id = g.id;

-- Weeks start on Monday.
DROP VIEW IF EXISTS strava_weekly_running;
CREATE VIEW strava_weekly_running AS
SELECT
    date(start_date_local, 'weekday 0', '-6 days')  AS week_start,
    COUNT(*)                                        AS runs,
    ROUND(SUM(distance) / 1000.0, 1)                AS total_km,
    ROUND(MAX(distance) / 1000.0, 1)                AS longest_km,
    ROUND(SUM(moving_time) / 3600.0, 2)             AS total_hours,
    ROUND((SUM(moving_time) / 60.0) / (SUM(distance) / 1000.0), 2) AS pace_min_per_km,
    ROUND(SUM(total_elevation_gain), 0)             AS elevation_m,
    ROUND(AVG(average_heartrate), 0)                AS avg_heartrate
FROM strava_activities
WHERE sport_type IN ('Run', 'TrailRun', 'VirtualRun') AND distance > 0
GROUP BY week_start
ORDER BY week_start DESC;

DROP VIEW IF EXISTS strava_monthly_stats;
CREATE VIEW strava_monthly_stats AS
SELECT
    strftime('%Y-%m', start_date_local)             AS month,
    sport_type,
    COUNT(*)                                        AS activities,
    ROUND(SUM(distance) / 1000.0, 1)                AS total_km,
    ROUND(SUM(moving_time) / 3600.0, 1)             AS total_hours,
    CASE WHEN SUM(distance) > 0
         THEN ROUND((SUM(moving_time) / 60.0) / (SUM(distance) / 1000.0), 2)
    END                                             AS pace_min_per_km,
    ROUND(SUM(total_elevation_gain), 0)             AS elevation_m,
    ROUND(AVG(average_heartrate), 0)                AS avg_heartrate
FROM strava_activities
WHERE start_date_local IS NOT NULL
GROUP BY month, sport_type
ORDER BY month DESC, activities DESC;
