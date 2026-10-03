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
