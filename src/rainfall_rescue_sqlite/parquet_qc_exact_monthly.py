"""Exact-monthly QC check on Parquet datasets using DuckDB.

This mirrors the logical behavior of the SQLite QC pipeline but writes Parquet
artifacts and consolidates with DuckDB.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_ingest import default_ensemble_parquet_root, default_rainfall_rescue_parquet_root
from .parquet_similarity import _configure_duckdb, default_comparison_parquet_root

CHECK_NAME = "exact_monthly_consistency"
CHECK_VERSION = "v1"


@dataclass(frozen=True)
class ExactMonthlyQCResult:
    qc_session_id: int
    files_processed: int
    day_rows_written: int
    pass_rows: int
    fail_rows: int
    exact_files_seen: int
    non_exact_files_seen: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _glob_sql(dir_path: Path) -> str:
    return str((dir_path / "*.parquet").resolve())


def _connect() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with the env-based memory/temp limits applied.

    On SLURM, DuckDB otherwise sizes its memory limit from the node's physical
    RAM (ignoring the cgroup ``--mem`` allocation) and can be OOM-killed. The
    QC sbatch scripts export ``DUCKDB_MEMORY_LIMIT`` / ``DUCKDB_TEMP_DIR`` which
    ``_configure_duckdb`` reads to cap memory and spill large aggregations to
    node-local scratch.
    """
    conn = duckdb.connect()
    _configure_duckdb(conn)
    return conn


def default_qc_parquet_root() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass qc_root explicitly")
    return Path(pdir) / "qc_parquet"


def default_qc_shard_dir() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass shard_dir explicitly")
    return Path(pdir) / "qc_shards_parquet"


def _resolve_similarity_session_id(comparison_root: Path, explicit: Optional[int]) -> int:
    if explicit is not None:
        return int(explicit)
    conn = _connect()
    try:
        value = conn.execute(
            f"SELECT MAX(session_id) FROM read_parquet('{_glob_sql(comparison_root / 'similarity_sessions')}')"
        ).fetchone()[0]
    finally:
        conn.close()
    if value is None:
        raise ValueError("No similarity sessions found in comparison dataset")
    return int(value)


def _next_qc_session_id(qc_root: Path) -> int:
    sessions_dir = qc_root / "qc_sessions"
    if not sessions_dir.exists():
        return 1
    ids: List[int] = []
    for path in sessions_dir.glob("session_*.parquet"):
        stem = path.stem
        try:
            ids.append(int(stem.split("_")[1]))
        except (IndexError, ValueError):
            continue
    return max(ids) + 1 if ids else 1


def _file_filter_sql(start_file_id: Optional[int], end_file_id: Optional[int]) -> str:
    clauses: List[str] = []
    if start_file_id is not None:
        clauses.append(f"file_id >= {int(start_file_id)}")
    if end_file_id is not None:
        clauses.append(f"file_id <= {int(end_file_id)}")
    if not clauses:
        return ""
    return " AND " + " AND ".join(clauses)


def _load_rr_monthlies_for_exact_files(
    *,
    comparison_root: Path,
    rr_dataset_root: Path,
    similarity_session_id: int,
    start_file_id: Optional[int],
    end_file_id: Optional[int],
) -> Dict[int, Dict[int, Optional[float]]]:
    conn = _connect()
    try:
        rows = conn.execute(
            f"""
            SELECT
                CAST(REPLACE(sm.ensemble_vector_id, 'ensemble_file::', '') AS BIGINT) AS file_id,
                rm.month,
                rm.rainfall_in
            FROM read_parquet('{_glob_sql(comparison_root / 'similarity_matches')}') sm
            JOIN read_parquet('{_glob_sql(comparison_root / 'rr_monthly_vectors')}') rv
              ON rv.rr_vector_id = sm.rr_vector_id
            LEFT JOIN read_parquet('{_glob_sql(rr_dataset_root / 'monthly_rainfall')}') rm
              ON rm.station_file_id = rv.station_file_id
             AND rm.year = rv.year
            WHERE sm.session_id = {int(similarity_session_id)}
              AND sm.query_rank = 1
              AND sm.exact_agreement_count >= 9
              {_file_filter_sql(start_file_id, end_file_id)}
            """
        ).fetchall()
    finally:
        conn.close()

    out: Dict[int, Dict[int, Optional[float]]] = {}
    for file_id, month, rainfall_in in rows:
        if month is None:
            continue
        out.setdefault(int(file_id), {})[int(month)] = (
            float(rainfall_in) if rainfall_in is not None else None
        )
    return out


def _iter_daily_medians(
    *,
    ensemble_dataset_root: Path,
    start_file_id: Optional[int],
    end_file_id: Optional[int],
) -> Sequence[Tuple[int, int, int, float]]:
    conn = _connect()
    try:
        rows = conn.execute(
            f"""
            SELECT
                file_id,
                month,
                day_of_month,
                median(COALESCE(rainfall, 0.0)) AS day_median
            FROM read_parquet('{_glob_sql(ensemble_dataset_root / 'ensemble_daily_values')}')
            WHERE 1 = 1
              {_file_filter_sql(start_file_id, end_file_id)}
            GROUP BY file_id, month, day_of_month
            ORDER BY file_id, month, day_of_month
            """
        ).fetchall()
        return rows
    finally:
        conn.close()


def _emit_file_month_flags(
    *,
    file_id: int,
    month_days: Dict[int, Set[int]],
    month_totals: Dict[int, float],
    rr_monthlies: Dict[int, Dict[int, Optional[float]]],
    tolerance: float,
    created_at: str,
) -> List[dict]:
    rows: List[dict] = []
    exact_months = rr_monthlies.get(file_id)

    for month, days in month_days.items():
        monthly_total = month_totals.get(month, 0.0)
        rr_total = exact_months.get(month) if exact_months is not None else None
        if exact_months is None:
            qc_flag = "fail"
            score = 0.0
            details = {
                "reason": "non_exact_specifier",
                "monthly_total_consensus": monthly_total,
                "rr_monthly_total": None,
                "tolerance": tolerance,
            }
        elif rr_total is None:
            qc_flag = "fail"
            score = 0.0
            details = {
                "reason": "missing_rr_monthly_value",
                "monthly_total_consensus": monthly_total,
                "rr_monthly_total": None,
                "tolerance": tolerance,
            }
        else:
            diff = abs(monthly_total - rr_total)
            passed = diff <= tolerance
            qc_flag = "pass" if passed else "fail"
            score = 1.0 if passed else 0.0
            details = {
                "reason": "exact_monthly_compare",
                "monthly_total_consensus": monthly_total,
                "rr_monthly_total": rr_total,
                "absolute_difference": diff,
                "tolerance": tolerance,
            }

        details_json = json.dumps(details, sort_keys=True)
        for day in sorted(days):
            rows.append(
                {
                    "file_id": file_id,
                    "day_of_month": day,
                    "month": month,
                    "check_name": CHECK_NAME,
                    "check_version": CHECK_VERSION,
                    "qc_score": score,
                    "qc_flag": qc_flag,
                    "details_json": details_json,
                    "created_at": created_at,
                }
            )

    return rows


def _compute_rows(
    *,
    ensemble_dataset_root: Path,
    comparison_root: Path,
    rr_dataset_root: Path,
    similarity_session_id: int,
    tolerance: float,
    start_file_id: Optional[int],
    end_file_id: Optional[int],
) -> Tuple[List[dict], int, int, int, int, int, int]:
    rr_monthlies = _load_rr_monthlies_for_exact_files(
        comparison_root=comparison_root,
        rr_dataset_root=rr_dataset_root,
        similarity_session_id=similarity_session_id,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
    )

    files_processed = 0
    pass_rows = 0
    fail_rows = 0
    exact_files_seen = 0
    non_exact_files_seen = 0
    all_rows: List[dict] = []

    current_file: Optional[int] = None
    month_totals: Dict[int, float] = {}
    month_days: Dict[int, Set[int]] = {}
    created_at = _utc_now()

    def flush_file() -> None:
        nonlocal files_processed, pass_rows, fail_rows, exact_files_seen, non_exact_files_seen
        nonlocal month_totals, month_days, current_file
        if current_file is None:
            return
        rows = _emit_file_month_flags(
            file_id=current_file,
            month_days=month_days,
            month_totals=month_totals,
            rr_monthlies=rr_monthlies,
            tolerance=tolerance,
            created_at=created_at,
        )
        all_rows.extend(rows)
        files_processed += 1
        pass_rows += sum(1 for r in rows if r["qc_flag"] == "pass")
        fail_rows += sum(1 for r in rows if r["qc_flag"] == "fail")
        if current_file in rr_monthlies:
            exact_files_seen += 1
        else:
            non_exact_files_seen += 1
        month_totals = {}
        month_days = {}

    for file_id, month, day_of_month, day_median in _iter_daily_medians(
        ensemble_dataset_root=ensemble_dataset_root,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
    ):
        file_id = int(file_id)
        month = int(month)
        day_of_month = int(day_of_month)
        day_median = float(day_median)

        if current_file is None:
            current_file = file_id
        if file_id != current_file:
            flush_file()
            current_file = file_id

        month_totals[month] = month_totals.get(month, 0.0) + day_median
        month_days.setdefault(month, set()).add(day_of_month)

    flush_file()

    return (
        all_rows,
        files_processed,
        len(all_rows),
        pass_rows,
        fail_rows,
        exact_files_seen,
        non_exact_files_seen,
    )


def _write_results_and_status(
    *,
    qc_root: Path,
    qc_session_id: int,
    rows: List[dict],
    promotion_threshold: float,
) -> Tuple[int, int]:
    results_dir = qc_root / "daily_qc_results"
    status_dir = qc_root / "daily_qc_status"
    results_dir.mkdir(parents=True, exist_ok=True)
    status_dir.mkdir(parents=True, exist_ok=True)

    if rows:
        out_rows = [{"qc_session_id": qc_session_id, **r} for r in rows]
        pq.write_table(
            pa.Table.from_pylist(out_rows),
            results_dir / f"session_{qc_session_id:06d}.parquet",
            compression="zstd",
        )
    else:
        pq.write_table(
            pa.table(
                {
                    "qc_session_id": pa.array([], type=pa.int64()),
                    "file_id": pa.array([], type=pa.int64()),
                    "day_of_month": pa.array([], type=pa.int8()),
                    "month": pa.array([], type=pa.int8()),
                    "check_name": pa.array([], type=pa.string()),
                    "check_version": pa.array([], type=pa.string()),
                    "qc_score": pa.array([], type=pa.float64()),
                    "qc_flag": pa.array([], type=pa.string()),
                    "details_json": pa.array([], type=pa.string()),
                    "created_at": pa.array([], type=pa.string()),
                }
            ),
            results_dir / f"session_{qc_session_id:06d}.parquet",
            compression="zstd",
        )

    conn = _connect()
    try:
        in_path = (results_dir / f"session_{qc_session_id:06d}.parquet").resolve()
        out_path = (status_dir / f"session_{qc_session_id:06d}.parquet").resolve()
        now = _utc_now()
        conn.execute(
            f"""
            COPY (
                SELECT
                    {qc_session_id} AS qc_session_id,
                    file_id,
                    day_of_month,
                    month,
                    AVG(qc_score) AS final_score,
                    CASE
                        WHEN SUM(CASE WHEN qc_flag = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'fail'
                        WHEN SUM(CASE WHEN qc_flag = 'review' THEN 1 ELSE 0 END) > 0 THEN 'review'
                        ELSE 'pass'
                    END AS final_flag,
                    CASE
                        WHEN SUM(CASE WHEN qc_flag = 'fail' THEN 1 ELSE 0 END) = 0
                         AND SUM(CASE WHEN qc_flag = 'review' THEN 1 ELSE 0 END) = 0
                         AND COALESCE(AVG(qc_score), 0.0) >= {float(promotion_threshold)}
                        THEN 1 ELSE 0
                    END AS promoted_good,
                    CASE
                        WHEN SUM(CASE WHEN qc_flag = 'fail' THEN 1 ELSE 0 END) = 0
                         AND SUM(CASE WHEN qc_flag = 'review' THEN 1 ELSE 0 END) = 0
                         AND COALESCE(AVG(qc_score), 0.0) >= {float(promotion_threshold)}
                        THEN '{now}' ELSE NULL
                    END AS promoted_at
                FROM read_parquet('{in_path}')
                GROUP BY file_id, day_of_month, month
            ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        final_counts = conn.execute(
            f"""
            SELECT final_flag, COUNT(*)
            FROM read_parquet('{out_path}')
            GROUP BY final_flag
            """
        ).fetchall()
    finally:
        conn.close()

    pass_rows = sum(int(n) for flag, n in final_counts if flag == "pass")
    fail_rows = sum(int(n) for flag, n in final_counts if flag == "fail")
    return pass_rows, fail_rows


def _write_session_row(
    *,
    qc_root: Path,
    qc_session_id: int,
    config: dict,
    promotion_threshold: float,
    status: str,
    message: Optional[str],
    started_at: str,
) -> None:
    sessions_dir = qc_root / "qc_sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [
            {
                "qc_session_id": qc_session_id,
                "started_at": started_at,
                "completed_at": _utc_now(),
                "status": status,
                "parent_qc_session_id": None,
                "config_json": json.dumps(config, sort_keys=True),
                "promotion_threshold": promotion_threshold,
                "message": message,
            }
        ]
    )
    pq.write_table(table, sessions_dir / f"session_{qc_session_id:06d}.parquet", compression="zstd")


def run_exact_monthly_consistency_check_parquet(
    *,
    ensemble_dataset_root: Path,
    comparison_root: Path,
    rr_dataset_root: Path,
    qc_root: Path,
    tolerance: float = 0.01,
    qc_session_id: Optional[int] = None,
    similarity_session_id: Optional[int] = None,
    start_file_id: Optional[int] = None,
    end_file_id: Optional[int] = None,
    promotion_threshold: float = 0.95,
) -> ExactMonthlyQCResult:
    similarity_session_id = _resolve_similarity_session_id(comparison_root, similarity_session_id)
    qc_root.mkdir(parents=True, exist_ok=True)
    if qc_session_id is None:
        qc_session_id = _next_qc_session_id(qc_root)

    started_at = _utc_now()
    rows, files_processed, day_rows_written, pass_rows, fail_rows, exact_files_seen, non_exact_files_seen = _compute_rows(
        ensemble_dataset_root=ensemble_dataset_root,
        comparison_root=comparison_root,
        rr_dataset_root=rr_dataset_root,
        similarity_session_id=similarity_session_id,
        tolerance=tolerance,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
    )

    final_pass, final_fail = _write_results_and_status(
        qc_root=qc_root,
        qc_session_id=qc_session_id,
        rows=rows,
        promotion_threshold=promotion_threshold,
    )

    _write_session_row(
        qc_root=qc_root,
        qc_session_id=qc_session_id,
        config={
            "check": CHECK_NAME,
            "check_version": CHECK_VERSION,
            "tolerance": tolerance,
            "similarity_session_id": similarity_session_id,
            "start_file_id": start_file_id,
            "end_file_id": end_file_id,
        },
        promotion_threshold=promotion_threshold,
        status="success",
        message=(
            f"{CHECK_NAME} wrote {day_rows_written} day rows "
            f"({pass_rows} pass, {fail_rows} fail); "
            f"final status ({final_pass} pass, {final_fail} fail)"
        ),
        started_at=started_at,
    )

    return ExactMonthlyQCResult(
        qc_session_id=qc_session_id,
        files_processed=files_processed,
        day_rows_written=day_rows_written,
        pass_rows=pass_rows,
        fail_rows=fail_rows,
        exact_files_seen=exact_files_seen,
        non_exact_files_seen=non_exact_files_seen,
    )


def run_exact_monthly_consistency_shard_parquet(
    *,
    ensemble_dataset_root: Path,
    comparison_root: Path,
    rr_dataset_root: Path,
    tolerance: float,
    shard_output_path: Path,
    similarity_session_id: Optional[int],
    start_file_id: int,
    end_file_id: int,
) -> ExactMonthlyQCResult:
    similarity_session_id = _resolve_similarity_session_id(comparison_root, similarity_session_id)
    rows, files_processed, day_rows_written, pass_rows, fail_rows, exact_files_seen, non_exact_files_seen = _compute_rows(
        ensemble_dataset_root=ensemble_dataset_root,
        comparison_root=comparison_root,
        rr_dataset_root=rr_dataset_root,
        similarity_session_id=similarity_session_id,
        tolerance=tolerance,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
    )

    shard_output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), shard_output_path, compression="zstd")

    return ExactMonthlyQCResult(
        qc_session_id=-1,
        files_processed=files_processed,
        day_rows_written=day_rows_written,
        pass_rows=pass_rows,
        fail_rows=fail_rows,
        exact_files_seen=exact_files_seen,
        non_exact_files_seen=non_exact_files_seen,
    )


def merge_exact_monthly_qc_shards_parquet(
    *,
    qc_root: Path,
    shard_paths: Sequence[Path],
    tolerance: float,
    similarity_session_id: Optional[int],
    num_shards: Optional[int] = None,
    promotion_threshold: float = 0.95,
) -> ExactMonthlyQCResult:
    shard_paths = list(shard_paths)
    if not shard_paths:
        raise ValueError("No shard paths provided")
    if num_shards is not None and len(shard_paths) != int(num_shards):
        raise ValueError(f"Expected {num_shards} shards but found {len(shard_paths)}")

    qc_root.mkdir(parents=True, exist_ok=True)
    qc_session_id = _next_qc_session_id(qc_root)
    started_at = _utc_now()

    shard_list = ", ".join("'" + str(p.resolve()).replace("'", "''") + "'" for p in shard_paths)
    source_expr = f"read_parquet([{shard_list}])"

    results_dir = qc_root / "daily_qc_results"
    status_dir = qc_root / "daily_qc_status"
    results_dir.mkdir(parents=True, exist_ok=True)
    status_dir.mkdir(parents=True, exist_ok=True)

    out_results = (results_dir / f"session_{qc_session_id:06d}.parquet").resolve()
    out_status = (status_dir / f"session_{qc_session_id:06d}.parquet").resolve()

    conn = _connect()
    try:
        conn.execute(
            f"""
            COPY (
                SELECT
                    {qc_session_id} AS qc_session_id,
                    file_id,
                    day_of_month,
                    month,
                    check_name,
                    check_version,
                    qc_score,
                    qc_flag,
                    details_json,
                    created_at
                FROM {source_expr}
            ) TO '{out_results}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        total_rows, pass_rows, fail_rows = conn.execute(
            f"""
            SELECT
                COUNT(*) AS n,
                SUM(CASE WHEN qc_flag = 'pass' THEN 1 ELSE 0 END) AS n_pass,
                SUM(CASE WHEN qc_flag = 'fail' THEN 1 ELSE 0 END) AS n_fail
            FROM {source_expr}
            """
        ).fetchone()

        now = _utc_now()
        conn.execute(
            f"""
            COPY (
                SELECT
                    {qc_session_id} AS qc_session_id,
                    file_id,
                    day_of_month,
                    month,
                    AVG(qc_score) AS final_score,
                    CASE
                        WHEN SUM(CASE WHEN qc_flag = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'fail'
                        WHEN SUM(CASE WHEN qc_flag = 'review' THEN 1 ELSE 0 END) > 0 THEN 'review'
                        ELSE 'pass'
                    END AS final_flag,
                    CASE
                        WHEN SUM(CASE WHEN qc_flag = 'fail' THEN 1 ELSE 0 END) = 0
                         AND SUM(CASE WHEN qc_flag = 'review' THEN 1 ELSE 0 END) = 0
                         AND COALESCE(AVG(qc_score), 0.0) >= {float(promotion_threshold)}
                        THEN 1 ELSE 0
                    END AS promoted_good,
                    CASE
                        WHEN SUM(CASE WHEN qc_flag = 'fail' THEN 1 ELSE 0 END) = 0
                         AND SUM(CASE WHEN qc_flag = 'review' THEN 1 ELSE 0 END) = 0
                         AND COALESCE(AVG(qc_score), 0.0) >= {float(promotion_threshold)}
                        THEN '{now}' ELSE NULL
                    END AS promoted_at
                FROM {source_expr}
                GROUP BY file_id, day_of_month, month
            ) TO '{out_status}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    finally:
        conn.close()

    _write_session_row(
        qc_root=qc_root,
        qc_session_id=qc_session_id,
        config={
            "check": CHECK_NAME,
            "check_version": CHECK_VERSION,
            "tolerance": tolerance,
            "similarity_session_id": similarity_session_id,
            "shards": len(shard_paths),
        },
        promotion_threshold=promotion_threshold,
        status="success",
        message=(
            f"{CHECK_NAME} merged {len(shard_paths)} shards, "
            f"{int(total_rows or 0)} rows ({int(pass_rows or 0)} pass, {int(fail_rows or 0)} fail)"
        ),
        started_at=started_at,
    )

    return ExactMonthlyQCResult(
        qc_session_id=qc_session_id,
        files_processed=-1,
        day_rows_written=int(total_rows or 0),
        pass_rows=int(pass_rows or 0),
        fail_rows=int(fail_rows or 0),
        exact_files_seen=-1,
        non_exact_files_seen=-1,
    )


def default_roots() -> tuple[Path, Path, Path, Path]:
    """Return default ensemble/comparison/rr/qc roots from PDIR."""
    return (
        default_ensemble_parquet_root(),
        default_comparison_parquet_root(),
        default_rainfall_rescue_parquet_root(),
        default_qc_parquet_root(),
    )
