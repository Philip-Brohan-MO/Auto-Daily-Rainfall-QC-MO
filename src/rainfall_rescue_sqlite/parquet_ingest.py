"""Parquet ingestion orchestration for Rainfall Rescue and ensemble datasets.

This module provides clean-slate rebuild paths that mirror the logical schema of
the SQLite pipelines, but write append-only Parquet batches for parallel-friendly
downstream processing with DuckDB.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .ensemble_ingest import default_ensemble_root, discover_ensemble_json_files
from .ensemble_parser import EnsembleParseError, parse_ensemble_json
from .ingest import default_rainfall_rescue_root, discover_combined_csv_files
from .parser import ParseError, parse_combined_csv


@dataclass(frozen=True)
class ParquetIngestionResult:
    dataset_root: Path
    source_root: Path
    files_discovered: int
    files_ingested: int
    daily_rows: int
    total_rows: int
    errors: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _reset_dataset_root(dataset_root: Path, *, overwrite: bool) -> None:
    if dataset_root.exists():
        if not overwrite:
            raise FileExistsError(
                f"Dataset root already exists: {dataset_root}. "
                "Pass overwrite=True to rebuild from scratch."
            )
        shutil.rmtree(dataset_root)
    dataset_root.mkdir(parents=True, exist_ok=True)


def _append_table(
    *,
    table_dir: Path,
    part_prefix: str,
    part_index: int,
    schema: pa.Schema,
    rows: list[dict],
) -> int:
    """Write one Parquet part for rows and return next part index."""
    if not rows:
        return part_index
    table_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    path = table_dir / f"{part_prefix}_{part_index:06d}.parquet"
    pq.write_table(table, path, compression="zstd")
    return part_index + 1


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


RR_STATIONS_SCHEMA = pa.schema(
    [
        pa.field("station_file_id", pa.string(), nullable=False),
        pa.field("station_folder", pa.string(), nullable=False),
        pa.field("station_file_name", pa.string(), nullable=False),
        pa.field("location_name", pa.string()),
        pa.field("grid_reference", pa.string()),
        pa.field("longitude", pa.float64()),
        pa.field("latitude", pa.float64()),
        pa.field("elevation_ft", pa.int32()),
        pa.field("station_number", pa.string()),
        pa.field("source_path", pa.string(), nullable=False),
    ]
)

RR_MONTHLY_SCHEMA = pa.schema(
    [
        pa.field("station_file_id", pa.string(), nullable=False),
        pa.field("year", pa.int32(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
        pa.field("rainfall_in", pa.float64(), nullable=False),
    ]
)

RR_ANNUAL_SCHEMA = pa.schema(
    [
        pa.field("station_file_id", pa.string(), nullable=False),
        pa.field("year", pa.int32(), nullable=False),
        pa.field("total_in", pa.float64(), nullable=False),
    ]
)

RR_ERRORS_SCHEMA = pa.schema(
    [
        pa.field("source_path", pa.string(), nullable=False),
        pa.field("error_message", pa.string(), nullable=False),
    ]
)


def ingest_rainfall_rescue_to_parquet(
    *,
    rainfall_rescue_root: Path,
    dataset_root: Path,
    max_files: Optional[int] = None,
    overwrite: bool = False,
    flush_every_files: int = 500,
) -> ParquetIngestionResult:
    """Build Rainfall Rescue Parquet datasets from combined CSV files."""
    source_root = rainfall_rescue_root / "DATA"
    csv_paths = discover_combined_csv_files(source_root)
    if max_files is not None:
        csv_paths = csv_paths[:max_files]

    _reset_dataset_root(dataset_root, overwrite=overwrite)

    stations_rows: list[dict] = []
    monthly_rows: list[dict] = []
    annual_rows: list[dict] = []
    error_rows: list[dict] = []

    part_stations = 0
    part_monthly = 0
    part_annual = 0
    part_errors = 0

    files_ingested = 0
    monthly_n = 0
    annual_n = 0
    errors = 0

    for idx, csv_path in enumerate(csv_paths, start=1):
        try:
            parsed = parse_combined_csv(csv_path, source_root)
            s = parsed.station
            stations_rows.append(
                {
                    "station_file_id": s.station_file_id,
                    "station_folder": s.station_folder,
                    "station_file_name": s.station_file_name,
                    "location_name": s.location_name,
                    "grid_reference": s.grid_reference,
                    "longitude": s.longitude,
                    "latitude": s.latitude,
                    "elevation_ft": s.elevation_ft,
                    "station_number": s.station_number,
                    "source_path": s.source_path,
                }
            )
            monthly_rows.extend(
                {
                    "station_file_id": station_file_id,
                    "year": year,
                    "month": month,
                    "rainfall_in": rainfall_in,
                }
                for (station_file_id, year, month, rainfall_in) in parsed.monthly_rows
            )
            annual_rows.extend(
                {
                    "station_file_id": station_file_id,
                    "year": year,
                    "total_in": total_in,
                }
                for (station_file_id, year, total_in) in parsed.annual_rows
            )
            files_ingested += 1
            monthly_n += len(parsed.monthly_rows)
            annual_n += len(parsed.annual_rows)
        except (ParseError, ValueError) as exc:
            errors += 1
            error_rows.append(
                {
                    "source_path": str(csv_path.relative_to(rainfall_rescue_root)),
                    "error_message": str(exc),
                }
            )

        if idx % flush_every_files == 0:
            part_stations = _append_table(
                table_dir=dataset_root / "stations",
                part_prefix="part",
                part_index=part_stations,
                schema=RR_STATIONS_SCHEMA,
                rows=stations_rows,
            )
            part_monthly = _append_table(
                table_dir=dataset_root / "monthly_rainfall",
                part_prefix="part",
                part_index=part_monthly,
                schema=RR_MONTHLY_SCHEMA,
                rows=monthly_rows,
            )
            part_annual = _append_table(
                table_dir=dataset_root / "annual_totals",
                part_prefix="part",
                part_index=part_annual,
                schema=RR_ANNUAL_SCHEMA,
                rows=annual_rows,
            )
            part_errors = _append_table(
                table_dir=dataset_root / "ingestion_file_errors",
                part_prefix="part",
                part_index=part_errors,
                schema=RR_ERRORS_SCHEMA,
                rows=error_rows,
            )
            stations_rows.clear()
            monthly_rows.clear()
            annual_rows.clear()
            error_rows.clear()

    part_stations = _append_table(
        table_dir=dataset_root / "stations",
        part_prefix="part",
        part_index=part_stations,
        schema=RR_STATIONS_SCHEMA,
        rows=stations_rows,
    )
    part_monthly = _append_table(
        table_dir=dataset_root / "monthly_rainfall",
        part_prefix="part",
        part_index=part_monthly,
        schema=RR_MONTHLY_SCHEMA,
        rows=monthly_rows,
    )
    part_annual = _append_table(
        table_dir=dataset_root / "annual_totals",
        part_prefix="part",
        part_index=part_annual,
        schema=RR_ANNUAL_SCHEMA,
        rows=annual_rows,
    )
    part_errors = _append_table(
        table_dir=dataset_root / "ingestion_file_errors",
        part_prefix="part",
        part_index=part_errors,
        schema=RR_ERRORS_SCHEMA,
        rows=error_rows,
    )

    run_payload = {
        "started_at": _utc_now(),
        "completed_at": _utc_now(),
        "source_root": str(source_root),
        "dataset_root": str(dataset_root),
        "files_discovered": len(csv_paths),
        "files_ingested": files_ingested,
        "station_rows": files_ingested,
        "monthly_rows": monthly_n,
        "annual_rows": annual_n,
        "errors": errors,
        "status": "success" if errors == 0 else "completed_with_errors",
        "part_counts": {
            "stations": part_stations,
            "monthly_rainfall": part_monthly,
            "annual_totals": part_annual,
            "ingestion_file_errors": part_errors,
        },
    }
    _write_json(dataset_root / "_metadata" / "ingestion_run.json", run_payload)

    return ParquetIngestionResult(
        dataset_root=dataset_root,
        source_root=source_root,
        files_discovered=len(csv_paths),
        files_ingested=files_ingested,
        daily_rows=monthly_n,
        total_rows=annual_n,
        errors=errors,
    )


ENSEMBLE_FILES_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.int64(), nullable=False),
        pa.field("file_name", pa.string(), nullable=False),
        pa.field("source_path", pa.string(), nullable=False),
        pa.field("year_start", pa.int32()),
        pa.field("year_end", pa.int32()),
        pa.field("descriptor", pa.string()),
        pa.field("section_id", pa.string()),
        pa.field("num_days", pa.int16(), nullable=False),
    ]
)

ENSEMBLE_DAILY_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.int64(), nullable=False),
        pa.field("day_of_month", pa.int8(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
        pa.field("ensemble_member", pa.int8(), nullable=False),
        pa.field("rainfall", pa.float64()),
        pa.field("is_missing", pa.int8(), nullable=False),
    ]
)

ENSEMBLE_TOTALS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.int64(), nullable=False),
        pa.field("month", pa.int8(), nullable=False),
        pa.field("ensemble_member", pa.int8(), nullable=False),
        pa.field("total", pa.float64()),
        pa.field("is_missing", pa.int8(), nullable=False),
    ]
)

ENSEMBLE_ERRORS_SCHEMA = pa.schema(
    [
        pa.field("source_path", pa.string(), nullable=False),
        pa.field("error_message", pa.string(), nullable=False),
    ]
)


def ingest_ensemble_to_parquet(
    *,
    ensemble_root: Path,
    dataset_root: Path,
    max_files: Optional[int] = None,
    shard_index: Optional[int] = None,
    num_shards: Optional[int] = None,
    overwrite: bool = False,
    flush_every_files: int = 200,
) -> ParquetIngestionResult:
    """Build ensemble transcription Parquet datasets from source JSON files."""
    if (shard_index is None) != (num_shards is None):
        raise ValueError("shard_index and num_shards must be given together")
    if num_shards is not None and not 0 <= int(shard_index) < int(num_shards):
        raise ValueError(
            f"shard_index {shard_index} out of range for num_shards {num_shards}"
        )

    json_paths = discover_ensemble_json_files(ensemble_root)
    if max_files is not None:
        json_paths = json_paths[:max_files]
    if num_shards is not None:
        json_paths = json_paths[int(shard_index) :: int(num_shards)]

    _reset_dataset_root(dataset_root, overwrite=overwrite)

    files_rows: list[dict] = []
    daily_rows: list[dict] = []
    totals_rows: list[dict] = []
    error_rows: list[dict] = []

    part_files = 0
    part_daily = 0
    part_totals = 0
    part_errors = 0

    files_ingested = 0
    daily_n = 0
    totals_n = 0
    errors = 0
    next_file_id = 1

    for idx, json_path in enumerate(json_paths, start=1):
        try:
            parsed = parse_ensemble_json(json_path)
            meta = parsed.metadata
            file_id = next_file_id
            next_file_id += 1

            files_rows.append(
                {
                    "file_id": file_id,
                    "file_name": meta.file_name,
                    "source_path": meta.source_path,
                    "year_start": meta.year_start,
                    "year_end": meta.year_end,
                    "descriptor": meta.descriptor,
                    "section_id": meta.section_id,
                    "num_days": meta.num_days,
                }
            )

            daily_rows.extend(
                {
                    "file_id": file_id,
                    "day_of_month": day,
                    "month": month,
                    "ensemble_member": member,
                    "rainfall": value,
                    "is_missing": int(is_missing),
                }
                for (day, month, member, value, is_missing) in parsed.daily_rows
            )

            totals_rows.extend(
                {
                    "file_id": file_id,
                    "month": month,
                    "ensemble_member": member,
                    "total": value,
                    "is_missing": int(is_missing),
                }
                for (month, member, value, is_missing) in parsed.total_rows
            )

            files_ingested += 1
            daily_n += len(parsed.daily_rows)
            totals_n += len(parsed.total_rows)
        except (EnsembleParseError, ValueError) as exc:
            errors += 1
            error_rows.append(
                {
                    "source_path": str(json_path),
                    "error_message": str(exc),
                }
            )

        if idx % flush_every_files == 0:
            part_files = _append_table(
                table_dir=dataset_root / "ensemble_files",
                part_prefix="part",
                part_index=part_files,
                schema=ENSEMBLE_FILES_SCHEMA,
                rows=files_rows,
            )
            part_daily = _append_table(
                table_dir=dataset_root / "ensemble_daily_values",
                part_prefix="part",
                part_index=part_daily,
                schema=ENSEMBLE_DAILY_SCHEMA,
                rows=daily_rows,
            )
            part_totals = _append_table(
                table_dir=dataset_root / "ensemble_monthly_totals",
                part_prefix="part",
                part_index=part_totals,
                schema=ENSEMBLE_TOTALS_SCHEMA,
                rows=totals_rows,
            )
            part_errors = _append_table(
                table_dir=dataset_root / "ensemble_ingestion_file_errors",
                part_prefix="part",
                part_index=part_errors,
                schema=ENSEMBLE_ERRORS_SCHEMA,
                rows=error_rows,
            )

            files_rows.clear()
            daily_rows.clear()
            totals_rows.clear()
            error_rows.clear()

    part_files = _append_table(
        table_dir=dataset_root / "ensemble_files",
        part_prefix="part",
        part_index=part_files,
        schema=ENSEMBLE_FILES_SCHEMA,
        rows=files_rows,
    )
    part_daily = _append_table(
        table_dir=dataset_root / "ensemble_daily_values",
        part_prefix="part",
        part_index=part_daily,
        schema=ENSEMBLE_DAILY_SCHEMA,
        rows=daily_rows,
    )
    part_totals = _append_table(
        table_dir=dataset_root / "ensemble_monthly_totals",
        part_prefix="part",
        part_index=part_totals,
        schema=ENSEMBLE_TOTALS_SCHEMA,
        rows=totals_rows,
    )
    part_errors = _append_table(
        table_dir=dataset_root / "ensemble_ingestion_file_errors",
        part_prefix="part",
        part_index=part_errors,
        schema=ENSEMBLE_ERRORS_SCHEMA,
        rows=error_rows,
    )

    run_payload = {
        "started_at": _utc_now(),
        "completed_at": _utc_now(),
        "source_root": str(ensemble_root),
        "dataset_root": str(dataset_root),
        "files_discovered": len(json_paths),
        "files_ingested": files_ingested,
        "daily_rows": daily_n,
        "total_rows": totals_n,
        "errors": errors,
        "status": "success" if errors == 0 else "completed_with_errors",
        "shard_index": int(shard_index) if shard_index is not None else None,
        "num_shards": int(num_shards) if num_shards is not None else None,
        "part_counts": {
            "ensemble_files": part_files,
            "ensemble_daily_values": part_daily,
            "ensemble_monthly_totals": part_totals,
            "ensemble_ingestion_file_errors": part_errors,
        },
    }
    _write_json(dataset_root / "_metadata" / "ingestion_run.json", run_payload)

    return ParquetIngestionResult(
        dataset_root=dataset_root,
        source_root=ensemble_root,
        files_discovered=len(json_paths),
        files_ingested=files_ingested,
        daily_rows=daily_n,
        total_rows=totals_n,
        errors=errors,
    )


def default_rainfall_rescue_parquet_root() -> Path:
    """Default Rainfall Rescue Parquet dataset root."""
    root = default_rainfall_rescue_root()
    return root / "rainfall_rescue_parquet"


def default_ensemble_parquet_root() -> Path:
    """Default ensemble Parquet dataset root."""
    pdir = os.environ.get("PDIR")
    if pdir:
        return Path(pdir) / "ensemble_transcriptions_parquet"
    return default_ensemble_root().parent / "ensemble_transcriptions_parquet"
