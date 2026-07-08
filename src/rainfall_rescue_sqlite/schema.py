"""Database schema helpers for Rainfall Rescue SQLite ingestion."""

from __future__ import annotations

import sqlite3

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

DROP TABLE IF EXISTS ingestion_file_errors;
DROP TABLE IF EXISTS ingestion_runs;
DROP TABLE IF EXISTS annual_totals;
DROP TABLE IF EXISTS monthly_rainfall;
DROP TABLE IF EXISTS stations;

CREATE TABLE stations (
    station_file_id TEXT PRIMARY KEY,
    station_folder TEXT NOT NULL,
    station_file_name TEXT NOT NULL,
    location_name TEXT,
    grid_reference TEXT,
    longitude REAL,
    latitude REAL,
    elevation_ft INTEGER,
    station_number TEXT,
    source_path TEXT NOT NULL UNIQUE
);

CREATE TABLE monthly_rainfall (
    station_file_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    rainfall_in REAL,
    PRIMARY KEY (station_file_id, year, month),
    FOREIGN KEY (station_file_id) REFERENCES stations(station_file_id)
);

CREATE TABLE annual_totals (
    station_file_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    total_in REAL,
    PRIMARY KEY (station_file_id, year),
    FOREIGN KEY (station_file_id) REFERENCES stations(station_file_id)
);

CREATE TABLE ingestion_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    source_root TEXT NOT NULL,
    db_path TEXT NOT NULL,
    files_discovered INTEGER NOT NULL DEFAULT 0,
    files_ingested INTEGER NOT NULL DEFAULT 0,
    station_rows INTEGER NOT NULL DEFAULT 0,
    monthly_rows INTEGER NOT NULL DEFAULT 0,
    annual_rows INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running',
    message TEXT
);

CREATE TABLE ingestion_file_errors (
    run_id INTEGER NOT NULL,
    source_path TEXT NOT NULL,
    error_message TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES ingestion_runs(run_id)
);

CREATE INDEX idx_stations_location_name ON stations(location_name);
CREATE INDEX idx_stations_station_number ON stations(station_number);
CREATE INDEX idx_stations_source_path ON stations(source_path);
CREATE INDEX idx_monthly_year_month ON monthly_rainfall(year, month);
CREATE INDEX idx_monthly_station_year ON monthly_rainfall(station_file_id, year);
CREATE INDEX idx_annual_year ON annual_totals(year);
"""


def rebuild_schema(connection: sqlite3.Connection) -> None:
    """Drop and recreate the full SQLite schema for a clean rebuild."""
    with connection:
        connection.executescript(SCHEMA_SQL)
