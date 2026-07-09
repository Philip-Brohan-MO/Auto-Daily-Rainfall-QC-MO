"""Ingestion orchestration for ensemble transcription JSON files."""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from .ensemble_parser import (
    EnsembleParseError,
    ParsedEnsembleFile,
    parse_ensemble_json,
)
from .ensemble_schema import rebuild_schema

DEFAULT_ENSEMBLE_ROOT = Path(
    "/data/scratch/philip.brohan/documents/Daily_Rainfall_UK/"
    "operational_sample/ensemble_transcriptions"
)


@dataclass(frozen=True)
class EnsembleIngestionResult:
    db_path: Path
    source_root: Path
    files_discovered: int
    files_ingested: int
    daily_rows: int
    total_rows: int
    errors: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def discover_ensemble_json_files(ensemble_root: Path) -> List[Path]:
    """Find all ensemble transcription JSON files under ensemble_root."""
    if not ensemble_root.exists():
        raise FileNotFoundError(f"Ensemble root not found: {ensemble_root}")

    candidates = sorted(ensemble_root.rglob("*.json"))
    return candidates


def _insert_file(conn: sqlite3.Connection, parsed: ParsedEnsembleFile) -> int:
    meta = parsed.metadata
    cursor = conn.execute(
        """
        INSERT INTO ensemble_files (
            file_name,
            source_path,
            year_start,
            year_end,
            descriptor,
            section_id,
            num_days
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            meta.file_name,
            meta.source_path,
            meta.year_start,
            meta.year_end,
            meta.descriptor,
            meta.section_id,
            meta.num_days,
        ),
    )
    return cursor.lastrowid


def ingest_ensemble_json(
    ensemble_root: Path,
    db_path: Path,
    *,
    max_files: Optional[int] = None,
) -> EnsembleIngestionResult:
    """Rebuild SQLite DB from ensemble transcription JSON files under ensemble_root."""
    json_files = discover_ensemble_json_files(ensemble_root)
    if max_files is not None:
        json_files = json_files[:max_files]

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    started_at = _utc_now()
    run_id = None
    try:
        rebuild_schema(conn)
        with conn:
            cursor = conn.execute(
                """
                INSERT INTO ensemble_ingestion_runs(
                    started_at, source_root, db_path, files_discovered
                )
                VALUES (?, ?, ?, ?)
                """,
                (started_at, str(ensemble_root), str(db_path), len(json_files)),
            )
            run_id = cursor.lastrowid

        files_ingested = 0
        daily_rows = 0
        total_rows = 0
        errors = 0

        for json_path in json_files:
            try:
                parsed = parse_ensemble_json(json_path)
                with conn:
                    file_id = _insert_file(conn, parsed)
                    conn.executemany(
                        """
                        INSERT INTO ensemble_daily_values(
                            file_id, day_of_month, month, ensemble_member, rainfall
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            (file_id, day, month, member, value)
                            for (day, month, member, value) in parsed.daily_rows
                        ),
                    )
                    conn.executemany(
                        """
                        INSERT INTO ensemble_monthly_totals(
                            file_id, month, ensemble_member, total
                        )
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            (file_id, month, member, value)
                            for (month, member, value) in parsed.total_rows
                        ),
                    )
                files_ingested += 1
                daily_rows += len(parsed.daily_rows)
                total_rows += len(parsed.total_rows)
            except (EnsembleParseError, ValueError, sqlite3.DatabaseError) as exc:
                errors += 1
                with conn:
                    conn.execute(
                        """
                        INSERT INTO ensemble_ingestion_file_errors(
                            run_id, source_path, error_message
                        )
                        VALUES (?, ?, ?)
                        """,
                        (run_id, str(json_path), str(exc)),
                    )

        with conn:
            conn.execute(
                """
                UPDATE ensemble_ingestion_runs
                SET completed_at = ?,
                    files_ingested = ?,
                    daily_rows = ?,
                    total_rows = ?,
                    errors = ?,
                    status = ?,
                    message = ?
                WHERE run_id = ?
                """,
                (
                    _utc_now(),
                    files_ingested,
                    daily_rows,
                    total_rows,
                    errors,
                    "success" if errors == 0 else "completed_with_errors",
                    None if errors == 0 else "Some files failed to parse; see ensemble_ingestion_file_errors",
                    run_id,
                ),
            )

        return EnsembleIngestionResult(
            db_path=db_path,
            source_root=ensemble_root,
            files_discovered=len(json_files),
            files_ingested=files_ingested,
            daily_rows=daily_rows,
            total_rows=total_rows,
            errors=errors,
        )
    except Exception as exc:
        if run_id is not None:
            with conn:
                conn.execute(
                    """
                    UPDATE ensemble_ingestion_runs
                    SET completed_at = ?, status = ?, message = ?
                    WHERE run_id = ?
                    """,
                    (_utc_now(), "failed", str(exc), run_id),
                )
        raise
    finally:
        conn.close()


def default_ensemble_root() -> Path:
    """Get default ensemble transcriptions root directory."""
    override = os.environ.get("ENSEMBLE_TRANSCRIPTIONS_ROOT")
    if override:
        return Path(override)
    return DEFAULT_ENSEMBLE_ROOT


def default_ensemble_db_path() -> Path:
    """Get default SQLite output location for ensemble transcriptions."""
    pdir = os.environ.get("PDIR")
    if pdir:
        return Path(pdir) / "ensemble_transcriptions.sqlite"
    return default_ensemble_root().parent / "ensemble_transcriptions.sqlite"
