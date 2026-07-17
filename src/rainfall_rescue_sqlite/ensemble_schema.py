"""Database schema helpers for ensemble transcription JSON ingestion."""

from __future__ import annotations

import sqlite3

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

DROP TABLE IF EXISTS ensemble_ingestion_file_errors;
DROP TABLE IF EXISTS ensemble_ingestion_runs;
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

CREATE INDEX idx_ensemble_files_years ON ensemble_files(year_start, year_end);
CREATE INDEX idx_ensemble_files_descriptor ON ensemble_files(descriptor);
CREATE INDEX idx_ensemble_files_match_type ON ensemble_files(match_type);
CREATE INDEX idx_ensemble_files_session ON ensemble_files(match_source_session_id);
CREATE INDEX idx_ensemble_files_matched_year ON ensemble_files(matched_year);
CREATE INDEX idx_ensemble_daily_day_month ON ensemble_daily_values(day_of_month, month);
CREATE INDEX idx_ensemble_daily_member ON ensemble_daily_values(ensemble_member);
CREATE INDEX idx_ensemble_totals_month ON ensemble_monthly_totals(month);
"""


def rebuild_schema(connection: sqlite3.Connection) -> None:
    """Drop and recreate the full SQLite schema for a clean rebuild."""
    with connection:
        connection.executescript(SCHEMA_SQL)
