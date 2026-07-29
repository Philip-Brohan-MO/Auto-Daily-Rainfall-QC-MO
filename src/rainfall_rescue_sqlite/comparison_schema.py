"""Schema for cross-source monthly similarity matching artifacts."""

from __future__ import annotations

import sqlite3

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

DROP TABLE IF EXISTS similarity_matches;
DROP TABLE IF EXISTS similarity_sessions;
DROP TABLE IF EXISTS ensemble_member_monthly_values;
DROP TABLE IF EXISTS ensemble_consensus_vectors;
DROP TABLE IF EXISTS rr_monthly_vectors;

CREATE TABLE rr_monthly_vectors (
    rr_vector_id TEXT PRIMARY KEY,
    station_file_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    location_name TEXT,
    station_number TEXT,
    latitude REAL,
    longitude REAL,
    completeness REAL NOT NULL,
    raw_vector_json TEXT NOT NULL,
    norm_vector_json TEXT NOT NULL
);

CREATE TABLE ensemble_consensus_vectors (
    ensemble_vector_id TEXT PRIMARY KEY,
    file_id INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    descriptor TEXT,
    section_id TEXT,
    year_start INTEGER,
    year_end INTEGER,
    completeness REAL NOT NULL,
    uncertainty_score REAL,
    monthly_iqr_json TEXT NOT NULL,
    raw_vector_json TEXT NOT NULL,
    norm_vector_json TEXT NOT NULL
);

CREATE TABLE ensemble_member_monthly_values (
    ensemble_vector_id TEXT NOT NULL,
    month INTEGER NOT NULL,
    ensemble_member INTEGER NOT NULL,
    total REAL,
    is_missing INTEGER NOT NULL DEFAULT 0 CHECK (is_missing IN (0, 1)),
    PRIMARY KEY (ensemble_vector_id, month, ensemble_member),
    FOREIGN KEY (ensemble_vector_id) REFERENCES ensemble_consensus_vectors(ensemble_vector_id)
);

CREATE TABLE similarity_sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    comparison_db_path TEXT NOT NULL,
    top_k INTEGER NOT NULL,
    min_overlap INTEGER NOT NULL,
    uncertainty_weight REAL NOT NULL,
    ranking_method TEXT NOT NULL DEFAULT 'cosine',
    ensemble_queries INTEGER NOT NULL DEFAULT 0,
    rr_candidates INTEGER NOT NULL DEFAULT 0,
    matches_written INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running',
    message TEXT
);

CREATE TABLE similarity_matches (
    session_id INTEGER NOT NULL,
    query_rank INTEGER NOT NULL,
    ensemble_vector_id TEXT NOT NULL,
    rr_vector_id TEXT NOT NULL,
    overlap_months INTEGER NOT NULL,
    exact_agreement_count INTEGER NOT NULL DEFAULT 0,
    cosine_similarity REAL NOT NULL,
    adjusted_score REAL NOT NULL,
    ensemble_uncertainty REAL,
    FOREIGN KEY (session_id) REFERENCES similarity_sessions(session_id)
);

CREATE INDEX idx_rr_vectors_station_year ON rr_monthly_vectors(station_file_id, year);
CREATE INDEX idx_rr_vectors_location ON rr_monthly_vectors(location_name);
CREATE INDEX idx_ensemble_vectors_file ON ensemble_consensus_vectors(file_id);
CREATE INDEX idx_ensemble_vectors_descriptor ON ensemble_consensus_vectors(descriptor);
CREATE INDEX idx_ensemble_member_values_vector ON ensemble_member_monthly_values(ensemble_vector_id);
CREATE INDEX idx_similarity_matches_session ON similarity_matches(session_id);
CREATE INDEX idx_similarity_matches_query ON similarity_matches(ensemble_vector_id, query_rank);
"""


def rebuild_schema(connection: sqlite3.Connection) -> None:
    """Drop and recreate the full comparison schema for a clean rebuild."""
    with connection:
        connection.executescript(SCHEMA_SQL)
