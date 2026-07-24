"""Baseline monthly-profile matching on Parquet datasets via DuckDB.

This module mirrors the logical behavior of the SQLite baseline matching
pipeline, but reads/writes Parquet datasets and uses DuckDB for scans.
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional, Sequence, Tuple

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .comparison_baseline import (
    EXACT_MATCH_DECIMALS,
    EXACT_MATCH_TOLERANCE,
    RANKING_METHOD_EXACT_ANY_MEMBER,
)
from .parquet_ingest import (
    default_ensemble_parquet_root,
    default_rainfall_rescue_parquet_root,
)

MONTHS = tuple(range(1, 13))


@dataclass(frozen=True)
class BuildResult:
    comparison_root: Path
    rr_vectors: int
    ensemble_vectors: int


@dataclass(frozen=True)
class MatchResult:
    comparison_root: Path
    session_id: int
    ensemble_queries: int
    rr_candidates: int
    matches_written: int


@dataclass(frozen=True)
class RRVector:
    rr_vector_id: str
    station_file_id: str
    year: int
    location_name: Optional[str]
    station_number: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    raw_vector: List[Optional[float]]
    norm_vector: List[Optional[float]]
    completeness: float


@dataclass(frozen=True)
class EnsembleConsensusVector:
    ensemble_vector_id: str
    file_id: int
    file_name: str
    descriptor: Optional[str]
    section_id: Optional[str]
    year_start: Optional[int]
    year_end: Optional[int]
    raw_vector: List[Optional[float]]
    norm_vector: List[Optional[float]]
    monthly_iqr: List[Optional[float]]
    uncertainty_score: Optional[float]
    completeness: float


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _percent_complete(vector: Sequence[Optional[float]]) -> float:
    present = sum(1 for v in vector if v is not None)
    return present / len(vector)


def _coerce_numeric(value: object) -> float:
    if value is None:
        return 0.0
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(result) or math.isinf(result):
        return 0.0
    return result


def _safe_mean(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _safe_stdev(values: Sequence[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    mu = _safe_mean(values)
    if mu is None:
        return None
    var = sum((v - mu) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(var)


def _quartiles(values: Sequence[float]) -> Tuple[float, float]:
    ordered = sorted(values)
    n = len(ordered)
    if n == 0:
        raise ValueError("Cannot compute quartiles for empty values")
    mid = n // 2
    if n % 2 == 0:
        lower = ordered[:mid]
        upper = ordered[mid:]
    else:
        lower = ordered[:mid]
        upper = ordered[mid + 1 :]

    q1 = ordered[0] if not lower else median(lower)
    q3 = ordered[-1] if not upper else median(upper)
    return q1, q3


def _iqr(values: Sequence[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    q1, q3 = _quartiles(values)
    return q3 - q1


def _normalize_shape(vector: Sequence[Optional[float]]) -> List[Optional[float]]:
    present = [v for v in vector if v is not None]
    if len(present) < 2:
        return [None] * len(vector)

    center = median(present)
    deviations = [abs(v - center) for v in present]
    mad = median(deviations)
    if mad <= 1.0e-9:
        std = _safe_stdev(present)
        scale = std if std and std > 1.0e-9 else 1.0
    else:
        scale = mad

    return [None if value is None else (value - center) / scale for value in vector]


def _vector_to_array(vector: Sequence[Optional[float]]) -> np.ndarray:
    return np.array([np.nan if value is None else float(value) for value in vector], dtype=np.float32)


def _glob_sql(dir_path: Path) -> str:
    return str((dir_path / "*.parquet").resolve())


def _configure_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    """Apply a memory limit / spill directory from the environment.

    On SLURM, DuckDB otherwise sizes its memory limit from the node's physical
    RAM (ignoring the cgroup ``--mem`` allocation), which can trigger an
    out-of-memory kill even when the query itself is streamable. Setting
    ``DUCKDB_MEMORY_LIMIT`` keeps it within the job's allocation, and
    ``DUCKDB_TEMP_DIR`` lets large aggregations spill to fast node-local
    scratch instead of failing.
    """
    mem_limit = os.environ.get("DUCKDB_MEMORY_LIMIT")
    if mem_limit:
        conn.execute(f"PRAGMA memory_limit='{mem_limit}'")
    temp_dir = os.environ.get("DUCKDB_TEMP_DIR")
    if temp_dir:
        conn.execute(f"PRAGMA temp_directory='{temp_dir}'")


def default_comparison_parquet_root() -> Path:
    from os import environ

    pdir = environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass comparison_root explicitly")
    return Path(pdir) / "monthly_similarity_parquet"


def _finalize_rr_vector(payload: Dict[str, object]) -> RRVector:
    months = payload["months"]  # type: ignore[index]
    raw_vector = [_coerce_numeric(months[m]) for m in MONTHS]  # type: ignore[index]
    norm_vector = _normalize_shape(raw_vector)
    station_file_id = payload["station_file_id"]
    year = payload["year"]
    return RRVector(
        rr_vector_id=f"{station_file_id}::{year}",
        station_file_id=str(station_file_id),
        year=int(year),  # type: ignore[arg-type]
        location_name=payload["location_name"],  # type: ignore[arg-type]
        station_number=payload["station_number"],  # type: ignore[arg-type]
        latitude=payload["latitude"],  # type: ignore[arg-type]
        longitude=payload["longitude"],  # type: ignore[arg-type]
        raw_vector=raw_vector,
        norm_vector=norm_vector,
        completeness=_percent_complete(raw_vector),
    )


def _iter_rr_vectors(rr_dataset_root: Path, batch_rows: int = 100_000):
    """Yield RRVector objects one station-year at a time.

    The monthly rows are streamed from DuckDB in bounded record batches and
    grouped on the fly (the query is ordered by station/year/month), so peak
    Python memory is one batch plus the current station-year, not the whole
    ~3.4M-row dataset.
    """
    conn = duckdb.connect()
    _configure_duckdb(conn)
    sql = f"""
        SELECT
            CAST(m.station_file_id AS VARCHAR) AS station_file_id,
            CAST(m.year AS BIGINT) AS year,
            CAST(m.month AS BIGINT) AS month,
            m.rainfall_in AS rainfall_in,
            CAST(s.location_name AS VARCHAR) AS location_name,
            CAST(s.station_number AS VARCHAR) AS station_number,
            CAST(s.latitude AS DOUBLE) AS latitude,
            CAST(s.longitude AS DOUBLE) AS longitude
        FROM read_parquet('{_glob_sql(rr_dataset_root / 'monthly_rainfall')}') m
        JOIN read_parquet('{_glob_sql(rr_dataset_root / 'stations')}') s
          ON s.station_file_id = m.station_file_id
        ORDER BY m.station_file_id, m.year, m.month
    """
    try:
        reader = conn.execute(sql).fetch_record_batch(batch_rows)
        current_key: Optional[Tuple[str, int]] = None
        current: Optional[Dict[str, object]] = None
        for batch in reader:
            d = batch.to_pydict()
            sfid = d["station_file_id"]
            yr = d["year"]
            mo = d["month"]
            rain = d["rainfall_in"]
            loc = d["location_name"]
            stn = d["station_number"]
            lat = d["latitude"]
            lon = d["longitude"]
            for r in range(batch.num_rows):
                key = (sfid[r], int(yr[r]))
                if key != current_key:
                    if current is not None:
                        yield _finalize_rr_vector(current)
                    current_key = key
                    current = {
                        "station_file_id": sfid[r],
                        "year": int(yr[r]),
                        "location_name": loc[r],
                        "station_number": stn[r],
                        "latitude": lat[r],
                        "longitude": lon[r],
                        "months": {m: None for m in MONTHS},
                    }
                current["months"][int(mo[r])] = rain[r]  # type: ignore[index]
        if current is not None:
            yield _finalize_rr_vector(current)
    finally:
        conn.close()


def _finalize_ensemble_vector(payload: Dict[str, object]) -> EnsembleConsensusVector:
    months = payload["months"]  # type: ignore[index]
    raw_vector: List[Optional[float]] = []
    iqr_vector: List[Optional[float]] = []
    spread_values: List[float] = []

    for month in MONTHS:
        member_values = months[month]  # type: ignore[index]
        if not member_values:
            raw_vector.append(None)
            iqr_vector.append(None)
            continue
        consensus = float(median(member_values))
        spread = _iqr(member_values)
        raw_vector.append(consensus)
        iqr_vector.append(spread)
        if spread is not None:
            spread_values.append(spread)

    file_id = payload["file_id"]
    return EnsembleConsensusVector(
        ensemble_vector_id=f"ensemble_file::{file_id}",
        file_id=int(file_id),  # type: ignore[arg-type]
        file_name=str(payload["file_name"]),
        descriptor=payload["descriptor"],  # type: ignore[arg-type]
        section_id=payload["section_id"],  # type: ignore[arg-type]
        year_start=payload["year_start"],  # type: ignore[arg-type]
        year_end=payload["year_end"],  # type: ignore[arg-type]
        raw_vector=raw_vector,
        norm_vector=_normalize_shape(raw_vector),
        monthly_iqr=iqr_vector,
        uncertainty_score=_safe_mean(spread_values),
        completeness=_percent_complete(raw_vector),
    )


def _iter_ensemble_consensus_vectors(
    ensemble_dataset_root: Path, batch_rows: int = 100_000
):
    """Yield EnsembleConsensusVector objects one file at a time.

    The per-(file, month) member totals are aggregated into lists inside
    DuckDB, so Python receives ~584k*12 compact rows (each a small member
    list) instead of the ~35M raw member rows. Rows are streamed in bounded
    record batches and grouped per file on the fly; peak Python memory is one
    batch plus the current file's 12 month-lists.
    """
    conn = duckdb.connect()
    _configure_duckdb(conn)
    totals_glob = _glob_sql(ensemble_dataset_root / "ensemble_monthly_totals")
    files_glob = _glob_sql(ensemble_dataset_root / "ensemble_files")
    sql = f"""
        SELECT
            CAST(f.file_id AS BIGINT) AS file_id,
            CAST(f.file_name AS VARCHAR) AS file_name,
            CAST(f.descriptor AS VARCHAR) AS descriptor,
            CAST(f.section_id AS VARCHAR) AS section_id,
            CAST(f.year_start AS BIGINT) AS year_start,
            CAST(f.year_end AS BIGINT) AS year_end,
            CAST(t.month AS BIGINT) AS month,
            t.members AS members
        FROM (
            SELECT
                file_id,
                month,
                list(total) FILTER (WHERE is_missing = 0 OR is_missing IS NULL) AS members
            FROM read_parquet('{totals_glob}')
            GROUP BY file_id, month
        ) t
        JOIN read_parquet('{files_glob}') f ON f.file_id = t.file_id
        ORDER BY f.file_id, t.month
    """
    try:
        reader = conn.execute(sql).fetch_record_batch(batch_rows)
        current_id: Optional[int] = None
        current: Optional[Dict[str, object]] = None
        for batch in reader:
            d = batch.to_pydict()
            fid = d["file_id"]
            fname = d["file_name"]
            desc = d["descriptor"]
            sec = d["section_id"]
            ys = d["year_start"]
            ye = d["year_end"]
            mo = d["month"]
            mem = d["members"]
            for r in range(batch.num_rows):
                file_id = int(fid[r])
                if file_id != current_id:
                    if current is not None:
                        yield _finalize_ensemble_vector(current)
                    current_id = file_id
                    current = {
                        "file_id": file_id,
                        "file_name": fname[r],
                        "descriptor": desc[r],
                        "section_id": sec[r],
                        "year_start": ys[r],
                        "year_end": ye[r],
                        "months": {m: [] for m in MONTHS},
                    }
                members = mem[r] or []
                current["months"][int(mo[r])] = [  # type: ignore[index]
                    _coerce_numeric(x) for x in members
                ]
        if current is not None:
            yield _finalize_ensemble_vector(current)
    finally:
        conn.close()


def _write_table(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


RR_VECTOR_SCHEMA = pa.schema(
    [
        ("rr_vector_id", pa.string()),
        ("station_file_id", pa.string()),
        ("year", pa.int64()),
        ("location_name", pa.string()),
        ("station_number", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("completeness", pa.float64()),
        ("raw_vector_json", pa.string()),
        ("norm_vector_json", pa.string()),
    ]
)

ENSEMBLE_VECTOR_SCHEMA = pa.schema(
    [
        ("ensemble_vector_id", pa.string()),
        ("file_id", pa.int64()),
        ("file_name", pa.string()),
        ("descriptor", pa.string()),
        ("section_id", pa.string()),
        ("year_start", pa.int64()),
        ("year_end", pa.int64()),
        ("completeness", pa.float64()),
        ("uncertainty_score", pa.float64()),
        ("monthly_iqr_json", pa.string()),
        ("raw_vector_json", pa.string()),
        ("norm_vector_json", pa.string()),
    ]
)


def _rr_row_dict(v: RRVector) -> dict:
    return {
        "rr_vector_id": v.rr_vector_id,
        "station_file_id": v.station_file_id,
        "year": v.year,
        "location_name": v.location_name,
        "station_number": v.station_number,
        "latitude": v.latitude,
        "longitude": v.longitude,
        "completeness": v.completeness,
        "raw_vector_json": json.dumps(v.raw_vector),
        "norm_vector_json": json.dumps(v.norm_vector),
    }


def _ensemble_row_dict(v: EnsembleConsensusVector) -> dict:
    return {
        "ensemble_vector_id": v.ensemble_vector_id,
        "file_id": v.file_id,
        "file_name": v.file_name,
        "descriptor": v.descriptor,
        "section_id": v.section_id,
        "year_start": v.year_start,
        "year_end": v.year_end,
        "completeness": v.completeness,
        "uncertainty_score": v.uncertainty_score,
        "monthly_iqr_json": json.dumps(v.monthly_iqr),
        "raw_vector_json": json.dumps(v.raw_vector),
        "norm_vector_json": json.dumps(v.norm_vector),
    }


def _write_vectors_streaming(
    path: Path,
    schema: pa.Schema,
    rows,
    *,
    batch_size: int = 50_000,
) -> int:
    """Write dict rows to a single Parquet file, flushing in bounded batches.

    The ParquetWriter is opened eagerly with an explicit schema, so a valid
    (possibly empty) file is always produced and never more than ``batch_size``
    rows are held in Python memory at once.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(path, schema, compression="zstd")
    buffer: List[dict] = []
    total = 0
    try:
        for row in rows:
            buffer.append(row)
            if len(buffer) >= batch_size:
                writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                total += len(buffer)
                buffer.clear()
        if buffer:
            writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
            total += len(buffer)
    finally:
        writer.close()
    return total


def _write_member_values(
    *,
    ensemble_dataset_root: Path,
    comparison_root: Path,
) -> int:
    conn = duckdb.connect()
    _configure_duckdb(conn)
    try:
        out_path = (comparison_root / "ensemble_member_monthly_values" / "part_000000.parquet").resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(
            f"""
            COPY (
                SELECT
                    'ensemble_file::' || CAST(file_id AS VARCHAR) AS ensemble_vector_id,
                    month,
                    ensemble_member,
                    total,
                    is_missing
                FROM read_parquet('{_glob_sql(ensemble_dataset_root / 'ensemble_monthly_totals')}')
                ORDER BY file_id, month, ensemble_member
            ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        n_rows = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]
        return int(n_rows)
    finally:
        conn.close()


def build_comparison_vectors_parquet(
    *,
    rr_dataset_root: Path,
    ensemble_dataset_root: Path,
    comparison_root: Path,
    overwrite: bool = True,
) -> BuildResult:
    """Build comparison vector datasets as Parquet.

    Vectors are streamed to disk in bounded batches (see
    :func:`_write_vectors_streaming`) rather than materialised in full, so this
    stays within a small, roughly constant Python memory footprint regardless
    of dataset size.
    """
    if comparison_root.exists() and overwrite:
        import shutil

        shutil.rmtree(comparison_root)
    comparison_root.mkdir(parents=True, exist_ok=True)

    rr_count = _write_vectors_streaming(
        comparison_root / "rr_monthly_vectors" / "part_000000.parquet",
        RR_VECTOR_SCHEMA,
        (_rr_row_dict(v) for v in _iter_rr_vectors(rr_dataset_root)),
    )
    ensemble_count = _write_vectors_streaming(
        comparison_root / "ensemble_consensus_vectors" / "part_000000.parquet",
        ENSEMBLE_VECTOR_SCHEMA,
        (
            _ensemble_row_dict(v)
            for v in _iter_ensemble_consensus_vectors(ensemble_dataset_root)
        ),
    )
    member_rows = _write_member_values(
        ensemble_dataset_root=ensemble_dataset_root,
        comparison_root=comparison_root,
    )

    _write_json(
        comparison_root / "_metadata" / "build_run.json",
        {
            "started_at": _utc_now(),
            "completed_at": _utc_now(),
            "rr_dataset_root": str(rr_dataset_root),
            "ensemble_dataset_root": str(ensemble_dataset_root),
            "comparison_root": str(comparison_root),
            "rr_vectors": rr_count,
            "ensemble_vectors": ensemble_count,
            "ensemble_member_rows": member_rows,
            "status": "success",
        },
    )

    return BuildResult(
        comparison_root=comparison_root,
        rr_vectors=rr_count,
        ensemble_vectors=ensemble_count,
    )


def _load_rr_candidates(
    conn: duckdb.DuckDBPyConnection,
    comparison_root: Path,
    max_rr_candidates: Optional[int],
) -> Tuple[List[str], np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    sql = (
        f"SELECT rr_vector_id, norm_vector_json, raw_vector_json "
        f"FROM read_parquet('{_glob_sql(comparison_root / 'rr_monthly_vectors')}') "
        f"ORDER BY rr_vector_id"
    )
    if max_rr_candidates is not None:
        sql += f" LIMIT {int(max_rr_candidates)}"
    rows = conn.execute(sql).fetchall()

    rr_ids: List[str] = []
    rr_norm_values: List[np.ndarray] = []
    rr_raw_values: List[np.ndarray] = []
    for row in rows:
        rr_ids.append(str(row[0]))
        rr_norm_values.append(_vector_to_array(json.loads(row[1])))
        rr_raw_values.append(_vector_to_array(json.loads(row[2])))

    rr_norm_matrix = np.vstack(rr_norm_values) if rr_norm_values else np.empty((0, 12), dtype=np.float32)
    rr_raw_matrix = np.vstack(rr_raw_values) if rr_raw_values else np.empty((0, 12), dtype=np.float32)
    rr_norm_mask = np.isfinite(rr_norm_matrix)
    rr_raw_mask = np.isfinite(rr_raw_matrix)
    rr_raw_rounded = np.round(rr_raw_matrix, EXACT_MATCH_DECIMALS)
    return rr_ids, rr_norm_matrix, rr_norm_mask, rr_raw_matrix, rr_raw_mask, rr_raw_rounded


def _sql_quoted(values: Sequence[str]) -> str:
    escaped = ["'" + value.replace("'", "''") + "'" for value in values]
    return ", ".join(escaped)


def _load_ensemble_member_monthly_map(
    conn: duckdb.DuckDBPyConnection,
    comparison_root: Path,
    ensemble_vector_ids: Sequence[str],
) -> Dict[str, np.ndarray]:
    member_values = {
        ensemble_vector_id: np.full((12, 5), np.nan, dtype=np.float32)
        for ensemble_vector_id in ensemble_vector_ids
    }
    if not ensemble_vector_ids:
        return member_values

    member_table = _glob_sql(comparison_root / "ensemble_member_monthly_values")
    chunk_size = 500
    for start in range(0, len(ensemble_vector_ids), chunk_size):
        chunk = ensemble_vector_ids[start : start + chunk_size]
        rows = conn.execute(
            f"""
            SELECT ensemble_vector_id, month, ensemble_member, total, is_missing
            FROM read_parquet('{member_table}')
            WHERE ensemble_vector_id IN ({_sql_quoted(chunk)})
            ORDER BY ensemble_vector_id, month, ensemble_member
            """
        ).fetchall()

        for row in rows:
            ensemble_vector_id = str(row[0])
            month = int(row[1])
            member = int(row[2])
            value = row[3]
            is_missing = int(row[4]) if row[4] is not None else 0
            if is_missing or value is None:
                continue
            member_values[ensemble_vector_id][month - 1, member - 1] = float(value)

    return member_values


def _load_ensemble_queries(
    conn: duckdb.DuckDBPyConnection,
    comparison_root: Path,
    max_ensemble_queries: Optional[int],
    *,
    shard_index: Optional[int] = None,
    num_shards: Optional[int] = None,
) -> List[Tuple[str, np.ndarray, np.ndarray, Optional[float], np.ndarray, np.ndarray]]:
    vec_glob = _glob_sql(comparison_root / "ensemble_consensus_vectors")
    sql = (
        f"SELECT ensemble_vector_id, norm_vector_json, uncertainty_score "
        f"FROM read_parquet('{vec_glob}') "
        f"ORDER BY ensemble_vector_id"
    )
    if shard_index is not None and num_shards is not None:
        total = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{vec_glob}')"
        ).fetchone()[0]
        shard_size = math.ceil(total / num_shards)
        offset = shard_index * shard_size
        sql += f" LIMIT {shard_size} OFFSET {offset}"
    elif max_ensemble_queries is not None:
        sql += f" LIMIT {int(max_ensemble_queries)}"
    rows = conn.execute(sql).fetchall()

    ensemble_vector_ids = [str(row[0]) for row in rows]
    member_lookup = _load_ensemble_member_monthly_map(conn, comparison_root, ensemble_vector_ids)

    queries = []
    for row in rows:
        ensemble_vector_id = str(row[0])
        vec = _vector_to_array(json.loads(row[1]))
        member_values = member_lookup[ensemble_vector_id]
        member_values_rounded = np.round(member_values, EXACT_MATCH_DECIMALS)
        member_month_mask = np.isfinite(member_values).any(axis=1)
        queries.append(
            (
                ensemble_vector_id,
                vec,
                np.isfinite(vec),
                None if row[2] is None else float(row[2]),
                member_values_rounded,
                member_month_mask,
            )
        )
    return queries


def _batched_query_topk(
    query_vec: np.ndarray,
    query_mask: np.ndarray,
    uncertainty_score: Optional[float],
    rr_ids: Sequence[str],
    rr_matrix: np.ndarray,
    rr_mask: np.ndarray,
    rr_raw_mask: np.ndarray,
    rr_raw_rounded: np.ndarray,
    query_member_values_rounded: np.ndarray,
    query_member_month_mask: np.ndarray,
    *,
    min_overlap: int,
    top_k: int,
    uncertainty_weight: float,
    batch_size: int,
) -> List[Tuple[str, int, int, float, float, Optional[float]]]:
    candidates: List[Tuple[str, int, int, float, float, Optional[float]]] = []
    uncertainty = uncertainty_score if uncertainty_score is not None else 0.0
    n_rr = rr_matrix.shape[0]
    query_member_mask = np.isfinite(query_member_values_rounded)

    for start in range(0, n_rr, batch_size):
        end = min(start + batch_size, n_rr)
        batch_values = rr_matrix[start:end]
        batch_mask = rr_mask[start:end]
        batch_raw_mask = rr_raw_mask[start:end]
        batch_raw_rounded = rr_raw_rounded[start:end]

        overlap_mask = batch_mask & query_mask
        valid_month_mask = batch_raw_mask & query_member_month_mask[np.newaxis, :]
        overlap_counts = valid_month_mask.sum(axis=1)
        valid = overlap_counts >= min_overlap
        if not np.any(valid):
            continue

        batch_vals_filled = np.where(batch_mask, batch_values, 0.0).astype(np.float32, copy=False)
        q_vals_filled = np.where(query_mask, query_vec, 0.0).astype(np.float32, copy=False)
        masked_batch = batch_vals_filled * overlap_mask
        masked_query = q_vals_filled * overlap_mask

        dot = np.sum(masked_batch * masked_query, axis=1)
        batch_norm = np.sqrt(np.sum(masked_batch * masked_batch, axis=1))
        query_norm = np.sqrt(np.sum(masked_query * masked_query, axis=1))
        denom = batch_norm * query_norm
        with np.errstate(divide="ignore", invalid="ignore"):
            cosine = np.where(denom > 0.0, dot / denom, -np.inf)
        cosine = np.where(np.isfinite(cosine), cosine, -np.inf)

        exact_equal = (
            np.abs(batch_raw_rounded[:, :, np.newaxis] - query_member_values_rounded[np.newaxis, :, :])
            <= EXACT_MATCH_TOLERANCE
        ) & query_member_mask[np.newaxis, :, :]
        month_agreement = np.any(exact_equal, axis=2) & valid_month_mask
        exact_counts = month_agreement.sum(axis=1)

        valid_idx = np.where(valid)[0]
        if valid_idx.size == 0:
            continue

        adjusted = cosine - (uncertainty_weight * uncertainty)
        take = min(top_k, valid_idx.size)
        if take <= 0:
            continue

        local_exact = exact_counts[valid_idx]
        local_overlap = overlap_counts[valid_idx]
        local_adjusted = adjusted[valid_idx]
        order = np.lexsort((-local_adjusted, -local_overlap, -local_exact))
        top_local_idx = valid_idx[order[:take]]

        for idx in top_local_idx:
            global_idx = start + int(idx)
            candidates.append(
                (
                    rr_ids[global_idx],
                    int(overlap_counts[idx]),
                    int(exact_counts[idx]),
                    float(cosine[idx]),
                    float(adjusted[idx]),
                    uncertainty_score,
                )
            )

    candidates.sort(key=lambda row: (row[2], row[1], row[4], row[3]), reverse=True)
    return candidates[:top_k]


def _next_session_id(comparison_root: Path) -> int:
    sessions_dir = comparison_root / "similarity_sessions"
    if not sessions_dir.exists():
        return 1
    ids: List[int] = []
    for path in sessions_dir.glob("session_*.parquet"):
        stem = path.stem
        try:
            ids.append(int(stem.split("_")[1]))
        except (IndexError, ValueError):
            continue
    return (max(ids) + 1) if ids else 1


def run_baseline_matching_parquet(
    *,
    comparison_root: Path,
    top_k: int = 10,
    min_overlap: int = 10,
    uncertainty_weight: float = 0.15,
    max_ensemble_queries: Optional[int] = None,
    max_rr_candidates: Optional[int] = None,
    batch_size: int = 8192,
    progress_interval: int = 0,
) -> MatchResult:
    """Run exhaustive matching and write sessions/matches as Parquet."""
    conn = duckdb.connect()
    try:
        rr_ids, rr_matrix, rr_mask, _raw, rr_raw_mask, rr_raw_rounded = _load_rr_candidates(
            conn, comparison_root, max_rr_candidates
        )
        ensemble_queries = _load_ensemble_queries(conn, comparison_root, max_ensemble_queries)
    finally:
        conn.close()

    session_id = _next_session_id(comparison_root)
    started_at = _utc_now()
    match_rows: List[dict] = []

    total_queries = len(ensemble_queries)
    loop_start = time.monotonic()

    for processed, (
        ensemble_vector_id,
        ensemble_vec,
        ensemble_mask,
        uncertainty_score,
        ensemble_member_values_rounded,
        ensemble_member_month_mask,
    ) in enumerate(ensemble_queries, start=1):
        candidates = _batched_query_topk(
            query_vec=ensemble_vec,
            query_mask=ensemble_mask,
            uncertainty_score=uncertainty_score,
            rr_ids=rr_ids,
            rr_matrix=rr_matrix,
            rr_mask=rr_mask,
            rr_raw_mask=rr_raw_mask,
            rr_raw_rounded=rr_raw_rounded,
            query_member_values_rounded=ensemble_member_values_rounded,
            query_member_month_mask=ensemble_member_month_mask,
            min_overlap=min_overlap,
            top_k=top_k,
            uncertainty_weight=uncertainty_weight,
            batch_size=batch_size,
        )

        for rank, candidate in enumerate(candidates, start=1):
            (
                rr_vector_id,
                overlap,
                exact_agreement_count,
                cosine_similarity,
                adjusted_score,
                unc,
            ) = candidate
            match_rows.append(
                {
                    "session_id": session_id,
                    "query_rank": rank,
                    "ensemble_vector_id": ensemble_vector_id,
                    "rr_vector_id": rr_vector_id,
                    "overlap_months": overlap,
                    "exact_agreement_count": exact_agreement_count,
                    "cosine_similarity": cosine_similarity,
                    "adjusted_score": adjusted_score,
                    "ensemble_uncertainty": unc,
                }
            )

        if progress_interval > 0 and (
            processed % progress_interval == 0 or processed == total_queries
        ):
            elapsed = time.monotonic() - loop_start
            rate = processed / elapsed if elapsed > 0 else 0.0
            remaining = total_queries - processed
            eta = remaining / rate if rate > 0 else float("inf")
            print(
                f"[match] {processed}/{total_queries} queries"
                f"  elapsed={elapsed:.1f}s"
                f"  rate={rate:.1f} q/s"
                f"  eta={eta:.1f}s"
                f"  matches={len(match_rows)}",
                flush=True,
            )

    sessions_table = pa.Table.from_pylist(
        [
            {
                "session_id": session_id,
                "started_at": started_at,
                "completed_at": _utc_now(),
                "comparison_root": str(comparison_root),
                "top_k": top_k,
                "min_overlap": min_overlap,
                "uncertainty_weight": uncertainty_weight,
                "ranking_method": RANKING_METHOD_EXACT_ANY_MEMBER,
                "ensemble_queries": len(ensemble_queries),
                "rr_candidates": len(rr_ids),
                "matches_written": len(match_rows),
                "status": "success",
                "message": None,
            }
        ]
    )
    _write_table(
        comparison_root / "similarity_sessions" / f"session_{session_id:06d}.parquet",
        sessions_table,
    )

    _write_table(
        comparison_root / "similarity_matches" / f"session_{session_id:06d}.parquet",
        pa.Table.from_pylist(match_rows),
    )

    _write_json(
        comparison_root / "_metadata" / f"match_session_{session_id:06d}.json",
        {
            "session_id": session_id,
            "started_at": started_at,
            "completed_at": _utc_now(),
            "comparison_root": str(comparison_root),
            "top_k": top_k,
            "min_overlap": min_overlap,
            "uncertainty_weight": uncertainty_weight,
            "ranking_method": RANKING_METHOD_EXACT_ANY_MEMBER,
            "ensemble_queries": len(ensemble_queries),
            "rr_candidates": len(rr_ids),
            "matches_written": len(match_rows),
            "status": "success",
        },
    )

    return MatchResult(
        comparison_root=comparison_root,
        session_id=session_id,
        ensemble_queries=len(ensemble_queries),
        rr_candidates=len(rr_ids),
        matches_written=len(match_rows),
    )


def run_matching_shard_parquet(
    *,
    comparison_root: Path,
    shard_output_path: Path,
    shard_index: int,
    num_shards: int,
    top_k: int = 10,
    min_overlap: int = 10,
    uncertainty_weight: float = 0.15,
    max_rr_candidates: Optional[int] = None,
    batch_size: int = 8192,
    progress_interval: int = 0,
) -> int:
    """Match one shard of ensemble queries against all RR candidates.

    Writes per-shard match rows to ``shard_output_path`` (a .parquet file).
    The merge step (``merge_similarity_shards_parquet``) consolidates all
    shard files into a single similarity session inside ``comparison_root``.

    Returns the number of match rows written.
    """
    conn = duckdb.connect()
    try:
        rr_ids, rr_matrix, rr_mask, _raw, rr_raw_mask, rr_raw_rounded = _load_rr_candidates(
            conn, comparison_root, max_rr_candidates
        )
        ensemble_queries = _load_ensemble_queries(
            conn, comparison_root, None, shard_index=shard_index, num_shards=num_shards
        )
    finally:
        conn.close()

    match_rows: List[dict] = []
    total_queries = len(ensemble_queries)
    loop_start = time.monotonic()

    for processed, (
        ensemble_vector_id,
        ensemble_vec,
        ensemble_mask,
        uncertainty_score,
        ensemble_member_values_rounded,
        ensemble_member_month_mask,
    ) in enumerate(ensemble_queries, start=1):
        candidates = _batched_query_topk(
            query_vec=ensemble_vec,
            query_mask=ensemble_mask,
            uncertainty_score=uncertainty_score,
            rr_ids=rr_ids,
            rr_matrix=rr_matrix,
            rr_mask=rr_mask,
            rr_raw_mask=rr_raw_mask,
            rr_raw_rounded=rr_raw_rounded,
            query_member_values_rounded=ensemble_member_values_rounded,
            query_member_month_mask=ensemble_member_month_mask,
            min_overlap=min_overlap,
            top_k=top_k,
            uncertainty_weight=uncertainty_weight,
            batch_size=batch_size,
        )

        for rank, candidate in enumerate(candidates, start=1):
            (
                rr_vector_id,
                overlap,
                exact_agreement_count,
                cosine_similarity,
                adjusted_score,
                unc,
            ) = candidate
            match_rows.append(
                {
                    "query_rank": rank,
                    "ensemble_vector_id": ensemble_vector_id,
                    "rr_vector_id": rr_vector_id,
                    "overlap_months": overlap,
                    "exact_agreement_count": exact_agreement_count,
                    "cosine_similarity": cosine_similarity,
                    "adjusted_score": adjusted_score,
                    "ensemble_uncertainty": unc,
                }
            )

        if progress_interval > 0 and (
            processed % progress_interval == 0 or processed == total_queries
        ):
            elapsed = time.monotonic() - loop_start
            rate = processed / elapsed if elapsed > 0 else 0.0
            remaining = total_queries - processed
            eta = remaining / rate if rate > 0 else float("inf")
            print(
                f"[shard {shard_index}/{num_shards}] {processed}/{total_queries} queries"
                f"  elapsed={elapsed:.1f}s"
                f"  rate={rate:.1f} q/s"
                f"  eta={eta:.1f}s"
                f"  matches={len(match_rows)}",
                flush=True,
            )

    shard_output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_table(shard_output_path, pa.Table.from_pylist(match_rows) if match_rows else pa.table(
        {
            "query_rank": pa.array([], type=pa.int32()),
            "ensemble_vector_id": pa.array([], type=pa.string()),
            "rr_vector_id": pa.array([], type=pa.string()),
            "overlap_months": pa.array([], type=pa.int32()),
            "exact_agreement_count": pa.array([], type=pa.int32()),
            "cosine_similarity": pa.array([], type=pa.float64()),
            "adjusted_score": pa.array([], type=pa.float64()),
            "ensemble_uncertainty": pa.array([], type=pa.float64()),
        }
    ))
    return len(match_rows)


@dataclass(frozen=True)
class MergeResult:
    comparison_root: Path
    session_id: int
    shards_merged: int
    matches_written: int


def merge_similarity_shards_parquet(
    *,
    comparison_root: Path,
    shard_dir: Path,
    top_k: int = 10,
    min_overlap: int = 10,
    uncertainty_weight: float = 0.15,
    expected_shards: Optional[int] = None,
) -> MergeResult:
    """Consolidate per-shard match parquet files into a single similarity session.

    Reads all ``similarity_shard_*.parquet`` files from ``shard_dir``,
    assigns a new ``session_id``, and writes the combined
    ``similarity_sessions`` / ``similarity_matches`` parquet files to
    ``comparison_root``.
    """
    shard_paths = sorted(shard_dir.glob("similarity_shard_*.parquet"))
    if not shard_paths:
        raise SystemExit(f"No similarity_shard_*.parquet files found in {shard_dir}")
    if expected_shards is not None and len(shard_paths) != expected_shards:
        raise SystemExit(
            f"Expected {expected_shards} shards but found {len(shard_paths)} in {shard_dir}"
        )

    session_id = _next_session_id(comparison_root)
    started_at = _utc_now()

    out_matches_path = (
        comparison_root / "similarity_matches" / f"session_{session_id:06d}.parquet"
    ).resolve()
    out_matches_path.parent.mkdir(parents=True, exist_ok=True)
    glob_pattern = str(shard_dir / "similarity_shard_*.parquet")

    conn = duckdb.connect()
    _configure_duckdb(conn)
    try:
        # Add the session_id column and order the rows entirely inside DuckDB,
        # streaming straight to Parquet. Materialising all ~5.8M merged rows in
        # Python previously OOM-killed this job.
        conn.execute(
            f"""
            COPY (
                SELECT
                    CAST({session_id} AS BIGINT) AS session_id,
                    CAST(query_rank AS BIGINT) AS query_rank,
                    ensemble_vector_id,
                    rr_vector_id,
                    CAST(overlap_months AS BIGINT) AS overlap_months,
                    CAST(exact_agreement_count AS BIGINT) AS exact_agreement_count,
                    CAST(cosine_similarity AS DOUBLE) AS cosine_similarity,
                    CAST(adjusted_score AS DOUBLE) AS adjusted_score,
                    CAST(ensemble_uncertainty AS DOUBLE) AS ensemble_uncertainty
                FROM read_parquet('{glob_pattern}')
                ORDER BY ensemble_vector_id, query_rank
            ) TO '{out_matches_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        matches_written, n_queries = conn.execute(
            f"""
            SELECT COUNT(*), COUNT(DISTINCT ensemble_vector_id)
            FROM read_parquet('{out_matches_path}')
            """
        ).fetchone()
    finally:
        conn.close()

    matches_written = int(matches_written)
    n_queries = int(n_queries)
    completed_at = _utc_now()

    sessions_table = pa.Table.from_pylist(
        [
            {
                "session_id": session_id,
                "started_at": started_at,
                "completed_at": completed_at,
                "comparison_root": str(comparison_root),
                "top_k": top_k,
                "min_overlap": min_overlap,
                "uncertainty_weight": uncertainty_weight,
                "ranking_method": RANKING_METHOD_EXACT_ANY_MEMBER,
                "ensemble_queries": n_queries,
                "rr_candidates": None,
                "matches_written": matches_written,
                "status": "success",
                "message": None,
            }
        ]
    )
    _write_table(
        comparison_root / "similarity_sessions" / f"session_{session_id:06d}.parquet",
        sessions_table,
    )

    _write_json(
        comparison_root / "_metadata" / f"match_session_{session_id:06d}.json",
        {
            "session_id": session_id,
            "started_at": started_at,
            "completed_at": completed_at,
            "comparison_root": str(comparison_root),
            "top_k": top_k,
            "min_overlap": min_overlap,
            "uncertainty_weight": uncertainty_weight,
            "ranking_method": RANKING_METHOD_EXACT_ANY_MEMBER,
            "ensemble_queries": n_queries,
            "matches_written": matches_written,
            "shards_merged": len(shard_paths),
            "status": "success",
        },
    )

    return MergeResult(
        comparison_root=comparison_root,
        session_id=session_id,
        shards_merged=len(shard_paths),
        matches_written=matches_written,
    )


def default_roots() -> tuple[Path, Path, Path]:
    """Return default RR, ensemble, and comparison Parquet roots from PDIR."""
    return (
        default_rainfall_rescue_parquet_root(),
        default_ensemble_parquet_root(),
        default_comparison_parquet_root(),
    )
