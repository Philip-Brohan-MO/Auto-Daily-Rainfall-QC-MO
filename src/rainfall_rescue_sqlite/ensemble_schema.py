"""Database schema helpers for ensemble transcription JSON ingestion."""

from __future__ import annotations

import sqlite3

TABLES_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

DROP TABLE IF EXISTS ensemble_ingestion_file_errors;
DROP TABLE IF EXISTS ensemble_ingestion_runs;
DROP TABLE IF EXISTS daily_qc_status;
DROP TABLE IF EXISTS daily_qc_results;
DROP TABLE IF EXISTS daily_qc_features;
DROP TABLE IF EXISTS qc_sessions;
DROP TABLE IF EXISTS ensemble_monthly_totals;
DROP TABLE IF EXISTS ensemble_daily_values;
DROP TABLE IF EXISTS ensemble_files;

CREATE TABLE ensemble_files (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name TEXT NOT NULL,
    source_path TEXT NOT NULL UNIQUE,
    year_start INTEGER,
    year_end INTEGER,
    descriptor TEXT,
    section_id TEXT,
    num_days INTEGER NOT NULL DEFAULT 0,
    matched_location_name TEXT,
    matched_year INTEGER,
    matched_latitude REAL,
    matched_longitude REAL,
    matched_elevation_ft REAL,
    match_type TEXT,
    match_source_session_id INTEGER
);

CREATE TABLE ensemble_daily_values (
    file_id INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    month INTEGER NOT NULL,
    ensemble_member INTEGER NOT NULL,
    rainfall REAL,
    is_missing INTEGER NOT NULL DEFAULT 0 CHECK (is_missing IN (0, 1)),
    PRIMARY KEY (file_id, day_of_month, month, ensemble_member),
    FOREIGN KEY (file_id) REFERENCES ensemble_files(file_id)
);

CREATE TABLE ensemble_monthly_totals (
    file_id INTEGER NOT NULL,
    month INTEGER NOT NULL,
    ensemble_member INTEGER NOT NULL,
    total REAL,
    is_missing INTEGER NOT NULL DEFAULT 0 CHECK (is_missing IN (0, 1)),
    PRIMARY KEY (file_id, month, ensemble_member),
    FOREIGN KEY (file_id) REFERENCES ensemble_files(file_id)
);

CREATE TABLE ensemble_ingestion_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    source_root TEXT NOT NULL,
    db_path TEXT NOT NULL,
    files_discovered INTEGER NOT NULL DEFAULT 0,
    files_ingested INTEGER NOT NULL DEFAULT 0,
    daily_rows INTEGER NOT NULL DEFAULT 0,
    total_rows INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running',
    message TEXT
);

CREATE TABLE ensemble_ingestion_file_errors (
    run_id INTEGER NOT NULL,
    source_path TEXT NOT NULL,
    error_message TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES ensemble_ingestion_runs(run_id)
);

CREATE TABLE qc_sessions (
    qc_session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    parent_qc_session_id INTEGER,
    config_json TEXT NOT NULL,
    promotion_threshold REAL NOT NULL DEFAULT 0.95,
    message TEXT
);

CREATE TABLE daily_qc_features (
    file_id INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    month INTEGER NOT NULL,
    climatology_median REAL,
    climatology_mad REAL,
    nearby_median REAL,
    nearby_mad REAL,
    temporal_delta REAL,
    feature_version TEXT NOT NULL DEFAULT 'v1',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (file_id, day_of_month, month),
    FOREIGN KEY (file_id) REFERENCES ensemble_files(file_id)
);

CREATE TABLE daily_qc_results (
    qc_session_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    month INTEGER NOT NULL,
    check_name TEXT NOT NULL,
    check_version TEXT NOT NULL,
    qc_score REAL,
    qc_flag TEXT NOT NULL CHECK (qc_flag IN ('pass', 'review', 'fail')),
    details_json TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (qc_session_id, file_id, day_of_month, month, check_name),
    FOREIGN KEY (qc_session_id) REFERENCES qc_sessions(qc_session_id),
    FOREIGN KEY (file_id) REFERENCES ensemble_files(file_id)
);

CREATE TABLE daily_qc_status (
    qc_session_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    month INTEGER NOT NULL,
    final_score REAL,
    final_flag TEXT NOT NULL CHECK (final_flag IN ('pass', 'review', 'fail')),
    promoted_good INTEGER NOT NULL DEFAULT 0 CHECK (promoted_good IN (0, 1)),
    promoted_at TEXT,
    PRIMARY KEY (qc_session_id, file_id, day_of_month, month),
    FOREIGN KEY (qc_session_id) REFERENCES qc_sessions(qc_session_id),
    FOREIGN KEY (file_id) REFERENCES ensemble_files(file_id)
);
"""

INDEXES_SQL = """
CREATE INDEX idx_ensemble_files_years ON ensemble_files(year_start, year_end);
CREATE INDEX idx_ensemble_files_descriptor ON ensemble_files(descriptor);
CREATE INDEX idx_ensemble_files_match_type ON ensemble_files(match_type);
CREATE INDEX idx_ensemble_files_session ON ensemble_files(match_source_session_id);
CREATE INDEX idx_ensemble_files_matched_year ON ensemble_files(matched_year);
CREATE INDEX idx_ensemble_daily_day_month ON ensemble_daily_values(day_of_month, month);
CREATE INDEX idx_ensemble_daily_member ON ensemble_daily_values(ensemble_member);
CREATE INDEX idx_ensemble_totals_month ON ensemble_monthly_totals(month);
CREATE INDEX idx_qc_sessions_status ON qc_sessions(status);
CREATE INDEX idx_qc_results_session_check ON daily_qc_results(qc_session_id, check_name);
CREATE INDEX idx_qc_results_flag ON daily_qc_results(qc_flag);
CREATE INDEX idx_qc_status_session_flag ON daily_qc_status(qc_session_id, final_flag);
CREATE INDEX idx_qc_status_session_date ON daily_qc_status(qc_session_id, month, day_of_month);
CREATE INDEX idx_qc_features_version ON daily_qc_features(feature_version);
"""

QC_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS qc_sessions (
    qc_session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    parent_qc_session_id INTEGER,
    config_json TEXT NOT NULL,
    promotion_threshold REAL NOT NULL DEFAULT 0.95,
    message TEXT
);

CREATE TABLE IF NOT EXISTS daily_qc_features (
    file_id INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    month INTEGER NOT NULL,
    climatology_median REAL,
    climatology_mad REAL,
    nearby_median REAL,
    nearby_mad REAL,
    temporal_delta REAL,
    feature_version TEXT NOT NULL DEFAULT 'v1',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (file_id, day_of_month, month),
    FOREIGN KEY (file_id) REFERENCES ensemble_files(file_id)
);

CREATE TABLE IF NOT EXISTS daily_qc_results (
    qc_session_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    month INTEGER NOT NULL,
    check_name TEXT NOT NULL,
    check_version TEXT NOT NULL,
    qc_score REAL,
    qc_flag TEXT NOT NULL CHECK (qc_flag IN ('pass', 'review', 'fail')),
    details_json TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (qc_session_id, file_id, day_of_month, month, check_name),
    FOREIGN KEY (qc_session_id) REFERENCES qc_sessions(qc_session_id),
    FOREIGN KEY (file_id) REFERENCES ensemble_files(file_id)
);

CREATE TABLE IF NOT EXISTS daily_qc_status (
    qc_session_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    month INTEGER NOT NULL,
    final_score REAL,
    final_flag TEXT NOT NULL CHECK (final_flag IN ('pass', 'review', 'fail')),
    promoted_good INTEGER NOT NULL DEFAULT 0 CHECK (promoted_good IN (0, 1)),
    promoted_at TEXT,
    PRIMARY KEY (qc_session_id, file_id, day_of_month, month),
    FOREIGN KEY (qc_session_id) REFERENCES qc_sessions(qc_session_id),
    FOREIGN KEY (file_id) REFERENCES ensemble_files(file_id)
);
"""

QC_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_qc_sessions_status ON qc_sessions(status);
CREATE INDEX IF NOT EXISTS idx_qc_results_session_check ON daily_qc_results(qc_session_id, check_name);
CREATE INDEX IF NOT EXISTS idx_qc_results_flag ON daily_qc_results(qc_flag);
CREATE INDEX IF NOT EXISTS idx_qc_status_session_flag ON daily_qc_status(qc_session_id, final_flag);
CREATE INDEX IF NOT EXISTS idx_qc_status_session_date ON daily_qc_status(qc_session_id, month, day_of_month);
CREATE INDEX IF NOT EXISTS idx_qc_features_version ON daily_qc_features(feature_version);
"""

# Full schema (tables + indexes) for the normal single-pass ingest.
SCHEMA_SQL = TABLES_SQL + INDEXES_SQL


def rebuild_schema(connection: sqlite3.Connection) -> None:
    """Drop and recreate the full SQLite schema for a clean rebuild."""
    with connection:
        connection.executescript(SCHEMA_SQL)


def rebuild_schema_tables_only(connection: sqlite3.Connection) -> None:
    """Recreate the tables without secondary indexes.

    Used by the shard merge, which bulk-loads millions of rows and then builds
    the indexes once at the end (far faster than maintaining them per insert).
    """
    with connection:
        connection.executescript(TABLES_SQL)


def create_indexes(connection: sqlite3.Connection) -> None:
    """Create the secondary indexes (call after a bulk load)."""
    with connection:
        connection.executescript(INDEXES_SQL)


def ensure_qc_schema(connection: sqlite3.Connection) -> None:
    """Create QC tables/indexes if missing (non-destructive migration path)."""
    with connection:
        connection.executescript(QC_TABLES_SQL)
        connection.executescript(QC_INDEXES_SQL)
