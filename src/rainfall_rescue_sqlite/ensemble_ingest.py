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
from .ensemble_schema import rebuild_schema, rebuild_schema_tables_only, create_indexes

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


def _iter_daily_rows_for_merge(shard_conn: sqlite3.Connection, offset: int):
    """Yield daily rows with a stable shape, compatible with old shard DBs."""
    try:
        rows = shard_conn.execute(
            """
            SELECT file_id, day_of_month, month, ensemble_member, rainfall, is_missing
            FROM ensemble_daily_values
            """
        )
        return ((r[0] + offset, r[1], r[2], r[3], r[4], int(r[5])) for r in rows)
    except sqlite3.DatabaseError:
        rows = shard_conn.execute(
            """
            SELECT file_id, day_of_month, month, ensemble_member, rainfall
            FROM ensemble_daily_values
            """
        )
        return ((r[0] + offset, r[1], r[2], r[3], r[4], 0) for r in rows)


def _iter_total_rows_for_merge(shard_conn: sqlite3.Connection, offset: int):
    """Yield monthly total rows with a stable shape, compatible with old shards."""
    try:
        rows = shard_conn.execute(
            """
            SELECT file_id, month, ensemble_member, total, is_missing
            FROM ensemble_monthly_totals
            """
        )
        return ((r[0] + offset, r[1], r[2], r[3], int(r[4])) for r in rows)
    except sqlite3.DatabaseError:
        rows = shard_conn.execute(
            """
            SELECT file_id, month, ensemble_member, total
            FROM ensemble_monthly_totals
            """
        )
        return ((r[0] + offset, r[1], r[2], r[3], 0) for r in rows)


def ingest_ensemble_json(
    ensemble_root: Path,
    db_path: Path,
    *,
    max_files: Optional[int] = None,
    shard_index: Optional[int] = None,
    num_shards: Optional[int] = None,
) -> EnsembleIngestionResult:
    """Rebuild SQLite DB from ensemble transcription JSON files under ensemble_root.

    When ``shard_index`` and ``num_shards`` are both given, only the interleaved
    slice ``json_files[shard_index::num_shards]`` is ingested. Because
    :func:`discover_ensemble_json_files` returns a sorted (deterministic) list,
    every shard sees a disjoint slice and their union is the full set. Shards can
    run as independent processes (e.g. a SLURM array) and be combined afterwards
    with :func:`merge_ensemble_shards`.
    """
    if (shard_index is None) != (num_shards is None):
        raise ValueError("shard_index and num_shards must be given together")
    if num_shards is not None and not 0 <= shard_index < num_shards:
        raise ValueError(
            f"shard_index {shard_index} out of range for num_shards {num_shards}"
        )

    json_files = discover_ensemble_json_files(ensemble_root)
    if max_files is not None:
        json_files = json_files[:max_files]
    if num_shards is not None:
        json_files = json_files[shard_index::num_shards]

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
                            file_id, day_of_month, month, ensemble_member, rainfall, is_missing
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            (file_id, day, month, member, value, int(is_missing))
                            for (day, month, member, value, is_missing) in parsed.daily_rows
                        ),
                    )
                    conn.executemany(
                        """
                        INSERT INTO ensemble_monthly_totals(
                            file_id, month, ensemble_member, total, is_missing
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            (file_id, month, member, value, int(is_missing))
                            for (month, member, value, is_missing) in parsed.total_rows
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


def merge_ensemble_shards(
    db_path: Path,
    shard_paths: List[Path],
    ensemble_root: Path,
) -> EnsembleIngestionResult:
    """Combine per-shard ensemble DBs (from sharded ingest) into one database.

    Each shard database was written by :func:`ingest_ensemble_json` for a disjoint
    slice of the JSON files, so its ``file_id`` values start again from 1. When
    merging we offset every shard's ``file_id`` by the current maximum so the
    ``ensemble_daily_values`` / ``ensemble_monthly_totals`` foreign keys stay
    consistent. Each source JSON file lives in exactly one shard, so the
    ``source_path`` UNIQUE constraint is never violated.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    # file_id values are inserted explicitly (offset per shard), so foreign-key
    # enforcement is unnecessary and would only slow the bulk copy.
    conn.execute("PRAGMA foreign_keys = OFF")

    started_at = _utc_now()
    run_id = None
    try:
        # Build tables without secondary indexes and create them once after the
        # bulk load; maintaining indexes per insert dominates the merge runtime
        # on the full dataset (tens of millions of rows). The DB is built on
        # node-local scratch and re-run on failure, so durability pragmas can be
        # relaxed. temp_store is left on disk (FILE) so the end-of-load index
        # sort spills to node-local scratch instead of RAM, and the page cache
        # is capped, keeping peak memory well within the job's allocation.
        rebuild_schema_tables_only(conn)
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA temp_store = FILE")
        conn.execute("PRAGMA cache_size = -262144")  # ~256 MB page cache
        with conn:
            cursor = conn.execute(
                """
                INSERT INTO ensemble_ingestion_runs(
                    started_at, source_root, db_path, files_discovered
                )
                VALUES (?, ?, ?, ?)
                """,
                (started_at, str(ensemble_root), str(db_path), 0),
            )
            run_id = cursor.lastrowid

        files_ingested = 0
        daily_rows = 0
        total_rows = 0
        errors = 0

        for shard_path in shard_paths:
            shard_conn = sqlite3.connect(f"file:{shard_path}?immutable=1", uri=True)
            try:
                with conn:
                    offset = conn.execute(
                        "SELECT COALESCE(MAX(file_id), 0) FROM ensemble_files"
                    ).fetchone()[0]

                    file_rows = shard_conn.execute(
                        """
                        SELECT file_id, file_name, source_path, year_start, year_end,
                               descriptor, section_id, num_days
                        FROM ensemble_files
                        """
                    ).fetchall()
                    conn.executemany(
                        """
                        INSERT INTO ensemble_files(
                            file_id, file_name, source_path, year_start, year_end,
                            descriptor, section_id, num_days
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        ((r[0] + offset, *r[1:]) for r in file_rows),
                    )

                    daily_cursor = conn.executemany(
                        """
                        INSERT INTO ensemble_daily_values(
                            file_id, day_of_month, month, ensemble_member, rainfall, is_missing
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        _iter_daily_rows_for_merge(shard_conn, offset),
                    )
                    shard_daily = daily_cursor.rowcount

                    totals_cursor = conn.executemany(
                        """
                        INSERT INTO ensemble_monthly_totals(
                            file_id, month, ensemble_member, total, is_missing
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        _iter_total_rows_for_merge(shard_conn, offset),
                    )
                    shard_totals = totals_cursor.rowcount

                    # Preserve the per-file parse-error audit trail (best effort:
                    # older shards may not have the table).
                    try:
                        shard_errors = shard_conn.execute(
                            "SELECT source_path, error_message "
                            "FROM ensemble_ingestion_file_errors"
                        ).fetchall()
                    except sqlite3.DatabaseError:
                        shard_errors = []
                    if shard_errors:
                        conn.executemany(
                            """
                            INSERT INTO ensemble_ingestion_file_errors(
                                run_id, source_path, error_message
                            )
                            VALUES (?, ?, ?)
                            """,
                            ((run_id, sp, msg) for sp, msg in shard_errors),
                        )

                files_ingested += len(file_rows)
                daily_rows += shard_daily
                total_rows += shard_totals
                errors += len(shard_errors)
            finally:
                shard_conn.close()

        # Build the secondary indexes once, now that all rows are loaded.
        create_indexes(conn)

        with conn:
            conn.execute(
                """
                UPDATE ensemble_ingestion_runs
                SET completed_at = ?,
                    files_discovered = ?,
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
                    files_ingested,
                    daily_rows,
                    total_rows,
                    errors,
                    "success" if errors == 0 else "completed_with_errors",
                    None
                    if errors == 0
                    else "Some files failed to parse; see ensemble_ingestion_file_errors",
                    run_id,
                ),
            )

        return EnsembleIngestionResult(
            db_path=db_path,
            source_root=ensemble_root,
            files_discovered=files_ingested,
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
    override = os.environ.get("ENSEMBLE_ROOT") or os.environ.get("ENSEMBLE_TRANSCRIPTIONS_ROOT")
    if override:
        return Path(override)
    return DEFAULT_ENSEMBLE_ROOT


def default_ensemble_db_path() -> Path:
    """Get default SQLite output location for ensemble transcriptions."""
    pdir = os.environ.get("PDIR")
    if pdir:
        return Path(pdir) / "ensemble_transcriptions.sqlite"
    return default_ensemble_root().parent / "ensemble_transcriptions.sqlite"
