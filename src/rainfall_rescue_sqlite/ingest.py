"""Ingestion orchestration for Rainfall Rescue combined CSV files."""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from .parser import ParseError, ParsedCombinedFile, parse_combined_csv
from .schema import rebuild_schema

SOURCE_PREFIXES = ("TYRain_", "ERROR_", "MISFILED_")


@dataclass(frozen=True)
class IngestionResult:
    db_path: Path
    source_root: Path
    files_discovered: int
    files_ingested: int
    station_rows: int
    monthly_rows: int
    annual_rows: int
    errors: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def discover_combined_csv_files(data_root: Path) -> List[Path]:
    """Find combined CSV files under DATA, excluding source sheet CSVs."""
    if not data_root.exists():
        raise FileNotFoundError(f"Data root not found: {data_root}")

    candidates: List[Path] = []
    for path in data_root.rglob("*.csv"):
        name = path.name
        if name.startswith(SOURCE_PREFIXES):
            continue
        candidates.append(path)

    candidates.sort()
    return candidates


def _insert_station(conn: sqlite3.Connection, parsed: ParsedCombinedFile) -> None:
    station = parsed.station
    conn.execute(
        """
        INSERT INTO stations (
            station_file_id,
            station_folder,
            station_file_name,
            location_name,
            grid_reference,
            longitude,
            latitude,
            elevation_ft,
            station_number,
            source_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            station.station_file_id,
            station.station_folder,
            station.station_file_name,
            station.location_name,
            station.grid_reference,
            station.longitude,
            station.latitude,
            station.elevation_ft,
            station.station_number,
            station.source_path,
        ),
    )


def ingest_combined_csvs(
    rainfall_rescue_root: Path,
    db_path: Path,
    *,
    max_files: Optional[int] = None,
) -> IngestionResult:
    """Rebuild SQLite DB from combined CSV files under rainfall_rescue_root/DATA."""
    data_root = rainfall_rescue_root / "DATA"
    combined_files = discover_combined_csv_files(data_root)
    if max_files is not None:
        combined_files = combined_files[:max_files]

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
                INSERT INTO ingestion_runs(started_at, source_root, db_path, files_discovered)
                VALUES (?, ?, ?, ?)
                """,
                (started_at, str(data_root), str(db_path), len(combined_files)),
            )
            run_id = cursor.lastrowid

        files_ingested = 0
        station_rows = 0
        monthly_rows = 0
        annual_rows = 0
        errors = 0

        for csv_path in combined_files:
            try:
                parsed = parse_combined_csv(csv_path, data_root)
                with conn:
                    _insert_station(conn, parsed)
                    conn.executemany(
                        """
                        INSERT INTO monthly_rainfall(station_file_id, year, month, rainfall_in)
                        VALUES (?, ?, ?, ?)
                        """,
                        parsed.monthly_rows,
                    )
                    conn.executemany(
                        """
                        INSERT INTO annual_totals(station_file_id, year, total_in)
                        VALUES (?, ?, ?)
                        """,
                        parsed.annual_rows,
                    )
                files_ingested += 1
                station_rows += 1
                monthly_rows += len(parsed.monthly_rows)
                annual_rows += len(parsed.annual_rows)
            except (ParseError, ValueError, sqlite3.DatabaseError) as exc:
                errors += 1
                with conn:
                    conn.execute(
                        """
                        INSERT INTO ingestion_file_errors(run_id, source_path, error_message)
                        VALUES (?, ?, ?)
                        """,
                        (
                            run_id,
                            str(csv_path.relative_to(rainfall_rescue_root)),
                            str(exc),
                        ),
                    )

        with conn:
            conn.execute(
                """
                UPDATE ingestion_runs
                SET completed_at = ?,
                    files_ingested = ?,
                    station_rows = ?,
                    monthly_rows = ?,
                    annual_rows = ?,
                    errors = ?,
                    status = ?,
                    message = ?
                WHERE run_id = ?
                """,
                (
                    _utc_now(),
                    files_ingested,
                    station_rows,
                    monthly_rows,
                    annual_rows,
                    errors,
                    "success" if errors == 0 else "completed_with_errors",
                    None if errors == 0 else "Some files failed to parse; see ingestion_file_errors",
                    run_id,
                ),
            )

        return IngestionResult(
            db_path=db_path,
            source_root=data_root,
            files_discovered=len(combined_files),
            files_ingested=files_ingested,
            station_rows=station_rows,
            monthly_rows=monthly_rows,
            annual_rows=annual_rows,
            errors=errors,
        )
    except Exception as exc:
        if run_id is not None:
            with conn:
                conn.execute(
                    """
                    UPDATE ingestion_runs
                    SET completed_at = ?, status = ?, message = ?
                    WHERE run_id = ?
                    """,
                    (_utc_now(), "failed", str(exc), run_id),
                )
        raise
    finally:
        conn.close()


def default_rainfall_rescue_root() -> Path:
    """Get default Rainfall Rescue root from env var PDIR."""
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass --rainfall-rescue-root explicitly")
    return Path(pdir) / "Rainfall-Rescue"


def default_db_path() -> Path:
    """Get default SQLite output location based on PDIR."""
    return default_rainfall_rescue_root() / "rainfall_rescue.sqlite"
