"""Baseline monthly-profile matching between RR and ensemble consensus vectors."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from .comparison_schema import rebuild_schema

MONTHS = tuple(range(1, 13))
EXACT_MATCH_DECIMALS = 2
EXACT_MATCH_TOLERANCE = 0.015  # midpoint between 0.01 and 0.02; captures values ≤0.01 apart after rounding to 2dp
RANKING_METHOD_EXACT_ANY_MEMBER = "exact_agreement_any_member_round2_tol01"


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


@dataclass(frozen=True)
class BuildResult:
    comparison_db_path: Path
    rr_vectors: int
    ensemble_vectors: int


@dataclass(frozen=True)
class MatchResult:
    comparison_db_path: Path
    session_id: int
    ensemble_queries: int
    rr_candidates: int
    matches_written: int


@dataclass(frozen=True)
class ShardResult:
    shard_output_path: Path
    shard_index: int
    num_shards: int
    query_offset: int
    query_limit: int
    ensemble_queries: int
    rr_candidates: int
    matches_written: int


SHARD_SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

DROP TABLE IF EXISTS shard_matches;
DROP TABLE IF EXISTS shard_meta;

CREATE TABLE shard_matches (
    query_rank INTEGER NOT NULL,
    ensemble_vector_id TEXT NOT NULL,
    rr_vector_id TEXT NOT NULL,
    overlap_months INTEGER NOT NULL,
    exact_agreement_count INTEGER NOT NULL,
    cosine_similarity REAL NOT NULL,
    adjusted_score REAL NOT NULL,
    ensemble_uncertainty REAL
);

CREATE TABLE shard_meta (
    shard_index INTEGER NOT NULL,
    num_shards INTEGER NOT NULL,
    query_offset INTEGER NOT NULL,
    query_limit INTEGER NOT NULL,
    ensemble_queries INTEGER NOT NULL,
    rr_candidates INTEGER NOT NULL,
    top_k INTEGER NOT NULL,
    min_overlap INTEGER NOT NULL,
    uncertainty_weight REAL NOT NULL,
    ranking_method TEXT NOT NULL,
    matches_written INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL
);
"""



def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _percent_complete(vector: Sequence[Optional[float]]) -> float:
    present = sum(1 for v in vector if v is not None)
    return present / len(vector)


def _coerce_numeric(value: object) -> float:
    """Coerce a value to float, mapping None/null/non-numeric/NaN/inf to 0.0."""
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

    if not lower:
        q1 = ordered[0]
    else:
        q1 = median(lower)

    if not upper:
        q3 = ordered[-1]
    else:
        q3 = median(upper)

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

    normalized: List[Optional[float]] = []
    for value in vector:
        if value is None:
            normalized.append(None)
        else:
            normalized.append((value - center) / scale)
    return normalized


def _rr_vectors(rr_db_path: Path) -> List[RRVector]:
    rr_sql = """
    SELECT
        m.station_file_id,
        m.year,
        m.month,
        m.rainfall_in,
        s.location_name,
        s.station_number,
        s.latitude,
        s.longitude
    FROM monthly_rainfall m
    JOIN stations s ON s.station_file_id = m.station_file_id
    ORDER BY m.station_file_id, m.year, m.month
    """

    grouped: Dict[Tuple[str, int], Dict[str, object]] = {}

    # immutable=1: read-only, no POSIX locking (works on shared cluster FS).
    with sqlite3.connect(f"file:{rr_db_path}?immutable=1", uri=True) as conn:
        for row in conn.execute(rr_sql):
            station_file_id = str(row[0])
            year = int(row[1])
            month = int(row[2])
            rainfall = row[3]

            key = (station_file_id, year)
            if key not in grouped:
                grouped[key] = {
                    "location_name": row[4],
                    "station_number": row[5],
                    "latitude": row[6],
                    "longitude": row[7],
                    "months": {m: None for m in MONTHS},
                }

            grouped[key]["months"][month] = rainfall

    vectors: List[RRVector] = []
    for (station_file_id, year), payload in grouped.items():
        raw_vector = [_coerce_numeric(payload["months"][m]) for m in MONTHS]
        norm_vector = _normalize_shape(raw_vector)
        vectors.append(
            RRVector(
                rr_vector_id=f"{station_file_id}::{year}",
                station_file_id=station_file_id,
                year=year,
                location_name=payload["location_name"],
                station_number=payload["station_number"],
                latitude=payload["latitude"],
                longitude=payload["longitude"],
                raw_vector=raw_vector,
                norm_vector=norm_vector,
                completeness=_percent_complete(raw_vector),
            )
        )

    vectors.sort(key=lambda v: (v.station_file_id, v.year))
    return vectors


def _ensemble_consensus_vectors(ensemble_db_path: Path) -> List[EnsembleConsensusVector]:
    ens_sql = """
    SELECT
        f.file_id,
        f.file_name,
        f.descriptor,
        f.section_id,
        f.year_start,
        f.year_end,
        t.month,
        t.ensemble_member,
        t.total
    FROM ensemble_monthly_totals t
    JOIN ensemble_files f ON f.file_id = t.file_id
    ORDER BY f.file_id, t.month, t.ensemble_member
    """

    grouped: Dict[int, Dict[str, object]] = {}

    # immutable=1: read-only, no POSIX locking (works on shared cluster FS).
    with sqlite3.connect(f"file:{ensemble_db_path}?immutable=1", uri=True) as conn:
        for row in conn.execute(ens_sql):
            file_id = int(row[0])
            if file_id not in grouped:
                grouped[file_id] = {
                    "file_name": row[1],
                    "descriptor": row[2],
                    "section_id": row[3],
                    "year_start": row[4],
                    "year_end": row[5],
                    "months": {m: [] for m in MONTHS},
                }

            month = int(row[6])
            grouped[file_id]["months"][month].append(_coerce_numeric(row[8]))

    vectors: List[EnsembleConsensusVector] = []
    for file_id, payload in grouped.items():
        raw_vector: List[Optional[float]] = []
        iqr_vector: List[Optional[float]] = []
        spread_values: List[float] = []

        for month in MONTHS:
            member_values = payload["months"][month]
            if not member_values:
                raw_vector.append(0.0)
                iqr_vector.append(None)
                continue

            consensus = float(median(member_values))
            spread = _iqr(member_values)
            raw_vector.append(consensus)
            iqr_vector.append(spread)
            if spread is not None:
                spread_values.append(spread)

        norm_vector = _normalize_shape(raw_vector)
        uncertainty_score = _safe_mean(spread_values)

        vectors.append(
            EnsembleConsensusVector(
                ensemble_vector_id=f"ensemble_file::{file_id}",
                file_id=file_id,
                file_name=str(payload["file_name"]),
                descriptor=payload["descriptor"],
                section_id=payload["section_id"],
                year_start=payload["year_start"],
                year_end=payload["year_end"],
                raw_vector=raw_vector,
                norm_vector=norm_vector,
                monthly_iqr=iqr_vector,
                uncertainty_score=uncertainty_score,
                completeness=_percent_complete(raw_vector),
            )
        )

    vectors.sort(key=lambda v: v.file_id)
    return vectors


def build_comparison_vectors(
    rr_db_path: Path,
    ensemble_db_path: Path,
    comparison_db_path: Path,
) -> BuildResult:
    """Rebuild comparison DB and store normalized RR + ensemble consensus vectors."""
    rr_vectors = _rr_vectors(rr_db_path)
    ensemble_vectors = _ensemble_consensus_vectors(ensemble_db_path)

    comparison_db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(comparison_db_path)
    try:
        rebuild_schema(conn)
        with conn:
            conn.executemany(
                """
                INSERT INTO rr_monthly_vectors(
                    rr_vector_id,
                    station_file_id,
                    year,
                    location_name,
                    station_number,
                    latitude,
                    longitude,
                    completeness,
                    raw_vector_json,
                    norm_vector_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        vector.rr_vector_id,
                        vector.station_file_id,
                        vector.year,
                        vector.location_name,
                        vector.station_number,
                        vector.latitude,
                        vector.longitude,
                        vector.completeness,
                        json.dumps(vector.raw_vector),
                        json.dumps(vector.norm_vector),
                    )
                    for vector in rr_vectors
                ),
            )

            conn.executemany(
                """
                INSERT INTO ensemble_consensus_vectors(
                    ensemble_vector_id,
                    file_id,
                    file_name,
                    descriptor,
                    section_id,
                    year_start,
                    year_end,
                    completeness,
                    uncertainty_score,
                    monthly_iqr_json,
                    raw_vector_json,
                    norm_vector_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        vector.ensemble_vector_id,
                        vector.file_id,
                        vector.file_name,
                        vector.descriptor,
                        vector.section_id,
                        vector.year_start,
                        vector.year_end,
                        vector.completeness,
                        vector.uncertainty_score,
                        json.dumps(vector.monthly_iqr),
                        json.dumps(vector.raw_vector),
                        json.dumps(vector.norm_vector),
                    )
                    for vector in ensemble_vectors
                ),
            )

            with sqlite3.connect(f"file:{ensemble_db_path}?immutable=1", uri=True) as src:
                rows = src.execute(
                    """
                    SELECT
                        file_id,
                        month,
                        ensemble_member,
                        total
                    FROM ensemble_monthly_totals
                    ORDER BY file_id, month, ensemble_member
                    """
                )
                conn.executemany(
                    """
                    INSERT INTO ensemble_member_monthly_values(
                        ensemble_vector_id,
                        month,
                        ensemble_member,
                        total
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        (
                            f"ensemble_file::{int(row[0])}",
                            int(row[1]),
                            int(row[2]),
                            None if row[3] is None else float(row[3]),
                        )
                        for row in rows
                    ),
                )

        return BuildResult(
            comparison_db_path=comparison_db_path,
            rr_vectors=len(rr_vectors),
            ensemble_vectors=len(ensemble_vectors),
        )
    finally:
        conn.close()


def _vector_to_array(vector: Sequence[Optional[float]]) -> np.ndarray:
    return np.array([np.nan if value is None else float(value) for value in vector], dtype=np.float32)


def _load_rr_candidates(
    conn: sqlite3.Connection, max_rr_candidates: Optional[int]
) -> Tuple[List[str], np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    sql = (
        "SELECT rr_vector_id, norm_vector_json, raw_vector_json "
        "FROM rr_monthly_vectors ORDER BY rr_vector_id"
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

    if rr_norm_values:
        rr_norm_matrix = np.vstack(rr_norm_values)
    else:
        rr_norm_matrix = np.empty((0, 12), dtype=np.float32)

    if rr_raw_values:
        rr_raw_matrix = np.vstack(rr_raw_values)
    else:
        rr_raw_matrix = np.empty((0, 12), dtype=np.float32)

    rr_norm_mask = np.isfinite(rr_norm_matrix)
    rr_raw_mask = np.isfinite(rr_raw_matrix)
    rr_raw_rounded = np.round(rr_raw_matrix, EXACT_MATCH_DECIMALS)
    return rr_ids, rr_norm_matrix, rr_norm_mask, rr_raw_matrix, rr_raw_mask, rr_raw_rounded


def _load_ensemble_member_monthly_map(
    conn: sqlite3.Connection,
    ensemble_vector_ids: Sequence[str],
) -> Dict[str, np.ndarray]:
    member_values = {
        ensemble_vector_id: np.full((12, 5), np.nan, dtype=np.float32)
        for ensemble_vector_id in ensemble_vector_ids
    }
    if not ensemble_vector_ids:
        return member_values

    chunk_size = 500
    for start in range(0, len(ensemble_vector_ids), chunk_size):
        chunk = ensemble_vector_ids[start : start + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT ensemble_vector_id, month, ensemble_member, total
            FROM ensemble_member_monthly_values
            WHERE ensemble_vector_id IN ({placeholders})
            ORDER BY ensemble_vector_id, month, ensemble_member
            """,
            tuple(chunk),
        ).fetchall()

        for row in rows:
            ensemble_vector_id = str(row[0])
            month = int(row[1])
            member = int(row[2])
            value = row[3]
            if value is None:
                continue
            member_values[ensemble_vector_id][month - 1, member - 1] = float(value)

    return member_values


def _load_ensemble_queries(
    conn: sqlite3.Connection,
    max_ensemble_queries: Optional[int],
    offset: int = 0,
) -> List[Tuple[str, np.ndarray, np.ndarray, Optional[float], np.ndarray, np.ndarray]]:
    sql = (
        "SELECT ensemble_vector_id, norm_vector_json, uncertainty_score "
        "FROM ensemble_consensus_vectors ORDER BY ensemble_vector_id"
    )
    if max_ensemble_queries is not None:
        sql += f" LIMIT {int(max_ensemble_queries)}"
        if offset:
            sql += f" OFFSET {int(offset)}"
    elif offset:
        sql += f" LIMIT -1 OFFSET {int(offset)}"
    rows = conn.execute(sql).fetchall()

    ensemble_vector_ids = [str(row[0]) for row in rows]
    member_lookup = _load_ensemble_member_monthly_map(conn, ensemble_vector_ids)

    queries: List[Tuple[str, np.ndarray, np.ndarray, Optional[float], np.ndarray, np.ndarray]] = []
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


def run_baseline_matching(
    comparison_db_path: Path,
    *,
    top_k: int = 10,
    min_overlap: int = 10,
    uncertainty_weight: float = 0.15,
    max_ensemble_queries: Optional[int] = None,
    max_rr_candidates: Optional[int] = None,
    batch_size: int = 8192,
    progress_interval: int = 0,
) -> MatchResult:
    """Run exhaustive matching with exact month agreement as primary ranking.

    Set ``progress_interval`` > 0 to print progress with elapsed time and ETA
    every N processed ensemble queries.
    """
    conn = sqlite3.connect(comparison_db_path)
    started_at = _utc_now()
    session_id = None

    try:
        rr_ids, rr_matrix, rr_mask, _rr_raw_matrix, rr_raw_mask, rr_raw_rounded = _load_rr_candidates(
            conn, max_rr_candidates
        )
        ensemble_queries = _load_ensemble_queries(conn, max_ensemble_queries)

        with conn:
            cursor = conn.execute(
                """
                INSERT INTO similarity_sessions(
                    started_at,
                    comparison_db_path,
                    top_k,
                    min_overlap,
                    uncertainty_weight,
                    ranking_method,
                    ensemble_queries,
                    rr_candidates
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    started_at,
                    str(comparison_db_path),
                    top_k,
                    min_overlap,
                    uncertainty_weight,
                    RANKING_METHOD_EXACT_ANY_MEMBER,
                    len(ensemble_queries),
                    len(rr_ids),
                ),
            )
            session_id = cursor.lastrowid

        match_rows: List[Tuple[int, int, str, str, int, int, float, float, Optional[float]]] = []

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
                    (
                        int(session_id),
                        rank,
                        ensemble_vector_id,
                        rr_vector_id,
                        overlap,
                        exact_agreement_count,
                        cosine_similarity,
                        adjusted_score,
                        unc,
                    )
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

        with conn:
            if match_rows:
                conn.executemany(
                    """
                    INSERT INTO similarity_matches(
                        session_id,
                        query_rank,
                        ensemble_vector_id,
                        rr_vector_id,
                        overlap_months,
                        exact_agreement_count,
                        cosine_similarity,
                        adjusted_score,
                        ensemble_uncertainty
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    match_rows,
                )

            conn.execute(
                """
                UPDATE similarity_sessions
                SET completed_at = ?,
                    matches_written = ?,
                    status = ?,
                    message = ?
                WHERE session_id = ?
                """,
                (
                    _utc_now(),
                    len(match_rows),
                    "success",
                    None,
                    session_id,
                ),
            )

        return MatchResult(
            comparison_db_path=comparison_db_path,
            session_id=int(session_id),
            ensemble_queries=len(ensemble_queries),
            rr_candidates=len(rr_ids),
            matches_written=len(match_rows),
        )
    except Exception as exc:
        if session_id is not None:
            with conn:
                conn.execute(
                    """
                    UPDATE similarity_sessions
                    SET completed_at = ?, status = ?, message = ?
                    WHERE session_id = ?
                    """,
                    (_utc_now(), "failed", str(exc), int(session_id)),
                )
        raise
    finally:
        conn.close()


def _shard_bounds(total: int, num_shards: int, shard_index: int) -> Tuple[int, int]:
    """Return (offset, limit) for a contiguous, evenly balanced shard slice."""
    if num_shards <= 0:
        raise ValueError("num_shards must be positive")
    if not 0 <= shard_index < num_shards:
        raise ValueError("shard_index must be in [0, num_shards)")
    base = total // num_shards
    remainder = total % num_shards
    offset = shard_index * base + min(shard_index, remainder)
    limit = base + (1 if shard_index < remainder else 0)
    return offset, limit


def run_matching_shard(
    comparison_db_path: Path,
    shard_output_path: Path,
    *,
    shard_index: int,
    num_shards: int,
    top_k: int = 10,
    min_overlap: int = 10,
    uncertainty_weight: float = 0.15,
    max_rr_candidates: Optional[int] = None,
    batch_size: int = 8192,
    progress_interval: int = 0,
) -> ShardResult:
    """Match one shard of ensemble queries against all RR candidates.

    Reads the prebuilt comparison vectors DB read-only (``immutable=1`` so many
    SLURM array tasks can share it without locking) and writes this shard's
    top-K matches to ``shard_output_path``. Merge shards later with
    :func:`merge_shard_matches`.
    """
    src = sqlite3.connect(f"file:{comparison_db_path}?immutable=1", uri=True)
    try:
        total_queries = src.execute(
            "SELECT COUNT(*) FROM ensemble_consensus_vectors"
        ).fetchone()[0]
        query_offset, query_limit = _shard_bounds(total_queries, num_shards, shard_index)
        rr_ids, rr_matrix, rr_mask, _rr_raw_matrix, rr_raw_mask, rr_raw_rounded = _load_rr_candidates(
            src, max_rr_candidates
        )
        ensemble_queries = _load_ensemble_queries(src, query_limit, offset=query_offset)
    finally:
        src.close()

    started_at = _utc_now()
    loop_start = time.monotonic()
    shard_total = len(ensemble_queries)
    match_rows: List[Tuple[int, str, str, int, int, float, float, Optional[float]]] = []

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
                (
                    rank,
                    ensemble_vector_id,
                    rr_vector_id,
                    overlap,
                    exact_agreement_count,
                    cosine_similarity,
                    adjusted_score,
                    unc,
                )
            )

        if progress_interval > 0 and (
            processed % progress_interval == 0 or processed == shard_total
        ):
            elapsed = time.monotonic() - loop_start
            rate = processed / elapsed if elapsed > 0 else 0.0
            remaining = shard_total - processed
            eta = remaining / rate if rate > 0 else float("inf")
            print(
                f"[shard {shard_index}/{num_shards}] {processed}/{shard_total} queries"
                f"  elapsed={elapsed:.1f}s"
                f"  rate={rate:.1f} q/s"
                f"  eta={eta:.1f}s"
                f"  matches={len(match_rows)}",
                flush=True,
            )

    completed_at = _utc_now()

    shard_output_path = Path(shard_output_path)
    shard_output_path.parent.mkdir(parents=True, exist_ok=True)
    out = sqlite3.connect(shard_output_path)
    try:
        with out:
            out.executescript(SHARD_SCHEMA_SQL)
            if match_rows:
                out.executemany(
                    """
                    INSERT INTO shard_matches(
                        query_rank,
                        ensemble_vector_id,
                        rr_vector_id,
                        overlap_months,
                        exact_agreement_count,
                        cosine_similarity,
                        adjusted_score,
                        ensemble_uncertainty
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    match_rows,
                )
            out.execute(
                """
                INSERT INTO shard_meta(
                    shard_index,
                    num_shards,
                    query_offset,
                    query_limit,
                    ensemble_queries,
                    rr_candidates,
                    top_k,
                    min_overlap,
                    uncertainty_weight,
                    ranking_method,
                    matches_written,
                    started_at,
                    completed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    shard_index,
                    num_shards,
                    query_offset,
                    query_limit,
                    shard_total,
                    len(rr_ids),
                    top_k,
                    min_overlap,
                    uncertainty_weight,
                    RANKING_METHOD_EXACT_ANY_MEMBER,
                    len(match_rows),
                    started_at,
                    completed_at,
                ),
            )
    finally:
        out.close()

    return ShardResult(
        shard_output_path=shard_output_path,
        shard_index=shard_index,
        num_shards=num_shards,
        query_offset=query_offset,
        query_limit=query_limit,
        ensemble_queries=shard_total,
        rr_candidates=len(rr_ids),
        matches_written=len(match_rows),
    )


def merge_shard_matches(
    comparison_db_path: Path,
    shard_paths: Sequence[Path],
    *,
    top_k: int = 10,
    min_overlap: int = 10,
    uncertainty_weight: float = 0.15,
) -> MatchResult:
    """Merge per-shard match files into a single similarity session.

    Creates one ``similarity_sessions`` row in ``comparison_db_path`` and copies
    every shard's ``shard_matches`` rows into ``similarity_matches`` under it.
    """
    conn = sqlite3.connect(comparison_db_path)
    started_at = _utc_now()
    session_id = None
    try:
        with conn:
            cursor = conn.execute(
                """
                INSERT INTO similarity_sessions(
                    started_at,
                    comparison_db_path,
                    top_k,
                    min_overlap,
                    uncertainty_weight,
                    ranking_method,
                    ensemble_queries,
                    rr_candidates
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    started_at,
                    str(comparison_db_path),
                    top_k,
                    min_overlap,
                    uncertainty_weight,
                    RANKING_METHOD_EXACT_ANY_MEMBER,
                    0,
                    0,
                ),
            )
            session_id = cursor.lastrowid

        total_matches = 0
        total_queries = 0
        rr_candidates = 0

        for shard_path in shard_paths:
            shard = sqlite3.connect(f"file:{shard_path}?immutable=1", uri=True)
            try:
                meta = shard.execute(
                    "SELECT ensemble_queries, rr_candidates FROM shard_meta"
                ).fetchone()
                rows = shard.execute(
                    """
                    SELECT
                        query_rank,
                        ensemble_vector_id,
                        rr_vector_id,
                        overlap_months,
                        exact_agreement_count,
                        cosine_similarity,
                        adjusted_score,
                        ensemble_uncertainty
                    FROM shard_matches
                    """
                ).fetchall()
            finally:
                shard.close()

            if meta is not None:
                total_queries += int(meta[0])
                rr_candidates = max(rr_candidates, int(meta[1]))

            if rows:
                with conn:
                    conn.executemany(
                        """
                        INSERT INTO similarity_matches(
                            session_id,
                            query_rank,
                            ensemble_vector_id,
                            rr_vector_id,
                            overlap_months,
                            exact_agreement_count,
                            cosine_similarity,
                            adjusted_score,
                            ensemble_uncertainty
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [(int(session_id), *row) for row in rows],
                    )
                total_matches += len(rows)

        with conn:
            conn.execute(
                """
                UPDATE similarity_sessions
                SET completed_at = ?,
                    matches_written = ?,
                    ensemble_queries = ?,
                    rr_candidates = ?,
                    status = ?,
                    message = ?
                WHERE session_id = ?
                """,
                (
                    _utc_now(),
                    total_matches,
                    total_queries,
                    rr_candidates,
                    "success",
                    f"merged from {len(shard_paths)} shards",
                    session_id,
                ),
            )

        return MatchResult(
            comparison_db_path=comparison_db_path,
            session_id=int(session_id),
            ensemble_queries=total_queries,
            rr_candidates=rr_candidates,
            matches_written=total_matches,
        )
    except Exception as exc:
        if session_id is not None:
            with conn:
                conn.execute(
                    """
                    UPDATE similarity_sessions
                    SET completed_at = ?, status = ?, message = ?
                    WHERE session_id = ?
                    """,
                    (_utc_now(), "failed", str(exc), int(session_id)),
                )
        raise
    finally:
        conn.close()

