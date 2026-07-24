"""First QC check: exact-match monthly consistency against RR monthly data.

Algorithm:
1) Only specifiers with an exact rank-1 match are eligible to pass.
2) For each specifier/month, compute monthly total by summing consensus daily
   values (median across ensemble members per day).
3) Compare that monthly total to matched RR monthly value.
4) If abs(diff) <= tolerance, all day-level observations in that file/month pass;
   otherwise they fail. Non-exact specifiers fail all day-level observations.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from statistics import median
from typing import Dict, Iterable, Optional, Set

from .ensemble_schema import ensure_qc_schema
from .ingest import default_db_path
from .qc_pipeline import (
    QCCheckRow,
    complete_qc_session,
    consolidate_qc_session,
    insert_qc_check_rows,
    start_qc_session,
)

# ------------------------------------------------------------------
# Shard SQLite helpers
# ------------------------------------------------------------------

SHARD_SCHEMA_SQL = """
PRAGMA journal_mode = OFF;
PRAGMA synchronous = OFF;
CREATE TABLE IF NOT EXISTS qc_shard_results (
    file_id       INTEGER NOT NULL,
    day_of_month  INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    check_name    TEXT NOT NULL,
    check_version TEXT NOT NULL,
    qc_score      REAL,
    qc_flag       TEXT NOT NULL,
    details_json  TEXT,
    created_at    TEXT NOT NULL
);
"""


def _write_shard(shard_path, rows: list[QCCheckRow], created_at: str) -> None:
    """Write QC check rows to a shard SQLite on local scratch."""
    import json as _json

    conn = sqlite3.connect(shard_path)
    conn.executescript(SHARD_SCHEMA_SQL)
    with conn:
        conn.executemany(
            """
            INSERT INTO qc_shard_results(
                file_id, day_of_month, month, check_name, check_version,
                qc_score, qc_flag, details_json, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    r.file_id, r.day_of_month, r.month,
                    r.check_name, r.check_version,
                    r.qc_score, r.qc_flag,
                    _json.dumps(r.details, sort_keys=True) if r.details is not None else None,
                    created_at,
                )
                for r in rows
            ],
        )
    conn.close()

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


def _build_file_filter_clause(
    start_file_id: Optional[int],
    end_file_id: Optional[int],
) -> tuple[str, list[int]]:
    clauses = []
    params: list[int] = []
    if start_file_id is not None:
        clauses.append("file_id >= ?")
        params.append(start_file_id)
    if end_file_id is not None:
        clauses.append("file_id <= ?")
        params.append(end_file_id)
    if not clauses:
        return "", []
    return " AND " + " AND ".join(clauses), params


def _load_rr_monthlies_for_exact_files(
    conn: sqlite3.Connection,
    *,
    file_filter_clause: str,
    file_filter_params: list[int],
) -> Dict[int, Dict[int, Optional[float]]]:
    """Return {file_id: {month: rr_monthly_total}} for exact rank-1 matches."""
    rows = conn.execute(
        f"""
        SELECT
            ef.file_id,
            rm.month,
            rm.rainfall_in
        FROM ensemble_files ef
        JOIN cmp.similarity_matches sm
          ON sm.ensemble_vector_id = ('ensemble_file::' || ef.file_id)
         AND sm.session_id = ef.match_source_session_id
         AND sm.query_rank = 1
         AND sm.exact_agreement_count >= 9
        JOIN cmp.rr_monthly_vectors rv
          ON rv.rr_vector_id = sm.rr_vector_id
        LEFT JOIN rr.monthly_rainfall rm
          ON rm.station_file_id = rv.station_file_id
         AND rm.year = rv.year
        WHERE ef.match_type = 'exact'
        {file_filter_clause}
        """,
        file_filter_params,
    ).fetchall()

    out: Dict[int, Dict[int, Optional[float]]] = {}
    for file_id, month, rainfall_in in rows:
        if month is None:
            continue
        out.setdefault(int(file_id), {})[int(month)] = (
            float(rainfall_in) if rainfall_in is not None else None
        )
    return out


def _iter_consensus_daily_rows(
    conn: sqlite3.Connection,
    *,
    file_filter_clause: str,
    file_filter_params: list[int],
) -> Iterable[tuple[int, int, int, Optional[float]]]:
    """Yield (file_id, month, day_of_month, rainfall) sorted for streaming reduce.

    Includes rows where rainfall is NULL so the caller can emit explicit fail
    flags for day-level observations that have no usable consensus value.
    """
    cursor = conn.execute(
        f"""
        SELECT file_id, month, day_of_month, rainfall
        FROM ensemble_daily_values
        WHERE 1 = 1
        {file_filter_clause}
        ORDER BY file_id, month, day_of_month, ensemble_member
        """,
        file_filter_params,
    )
    for row in cursor:
        rainfall = float(row[3]) if row[3] is not None else None
        yield int(row[0]), int(row[1]), int(row[2]), rainfall


def _emit_file_month_flags(
    *,
    file_id: int,
    month_days: Dict[int, Set[int]],
    month_totals: Dict[int, float],
    rr_monthlies: Dict[int, Dict[int, Optional[float]]],
    tolerance: float,
) -> list[QCCheckRow]:
    rows: list[QCCheckRow] = []
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

        for day in sorted(days):
            rows.append(
                QCCheckRow(
                    file_id=file_id,
                    day_of_month=day,
                    month=month,
                    check_name=CHECK_NAME,
                    check_version=CHECK_VERSION,
                    qc_score=score,
                    qc_flag=qc_flag,
                    details=details,
                )
            )

    return rows


def run_exact_monthly_consistency_check(
    *,
    ensemble_db_path,
    comparison_db_path,
    rr_db_path=None,
    tolerance: float = 0.01,
    qc_session_id: Optional[int] = None,
    start_file_id: Optional[int] = None,
    end_file_id: Optional[int] = None,
) -> ExactMonthlyQCResult:
    """Run the first QC check and write outputs into daily_qc_results/status."""
    if rr_db_path is None:
        rr_db_path = default_db_path()

    conn = sqlite3.connect(f"file:{ensemble_db_path}", uri=True)
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        ensure_qc_schema(conn)

        conn.execute(
            "ATTACH DATABASE ? AS cmp",
            (f"file:{comparison_db_path}?immutable=1",),
        )
        conn.execute(
            "ATTACH DATABASE ? AS rr",
            (f"file:{rr_db_path}?immutable=1",),
        )

        file_filter_clause, file_filter_params = _build_file_filter_clause(
            start_file_id=start_file_id,
            end_file_id=end_file_id,
        )

        if qc_session_id is None:
            config = {
                "check": CHECK_NAME,
                "check_version": CHECK_VERSION,
                "tolerance": tolerance,
                "start_file_id": start_file_id,
                "end_file_id": end_file_id,
            }
            qc_session_id = start_qc_session(
                conn,
                config=config,
                promotion_threshold=0.95,
            )

        rr_monthlies = _load_rr_monthlies_for_exact_files(
            conn,
            file_filter_clause=file_filter_clause,
            file_filter_params=file_filter_params,
        )

        files_processed = 0
        day_rows_written = 0
        pass_rows = 0
        fail_rows = 0
        exact_files_seen = 0
        non_exact_files_seen = 0

        current_file: Optional[int] = None
        current_key: Optional[tuple[int, int]] = None  # (month, day)
        current_values: list[float] = []
        month_totals: Dict[int, float] = {}
        month_days: Dict[int, Set[int]] = {}
        def flush_day() -> None:
            nonlocal current_key, current_values, month_totals, month_days
            if current_key is None:
                return
            month, day = current_key
            month_days.setdefault(month, set()).add(day)
            day_median = float(median(current_values))
            month_totals[month] = month_totals.get(month, 0.0) + day_median

        def flush_file() -> None:
            nonlocal files_processed, day_rows_written, pass_rows, fail_rows
            nonlocal exact_files_seen, non_exact_files_seen
            nonlocal month_totals, month_days, current_file

            if current_file is None:
                return

            rows = _emit_file_month_flags(
                file_id=current_file,
                month_days=month_days,
                month_totals=month_totals,
                rr_monthlies=rr_monthlies,
                tolerance=tolerance,
            )
            insert_qc_check_rows(conn, qc_session_id=qc_session_id, rows=rows)

            files_processed += 1
            day_rows_written += len(rows)
            pass_rows += sum(1 for r in rows if r.qc_flag == "pass")
            fail_rows += sum(1 for r in rows if r.qc_flag == "fail")
            if current_file in rr_monthlies:
                exact_files_seen += 1
            else:
                non_exact_files_seen += 1

            month_totals = {}
            month_days = {}

        for file_id, month, day, rainfall in _iter_consensus_daily_rows(
            conn,
            file_filter_clause=file_filter_clause,
            file_filter_params=file_filter_params,
        ):
            if current_file is None:
                current_file = file_id
                current_key = (month, day)
                current_values = [rainfall if rainfall is not None else 0.0]
                continue

            if file_id != current_file:
                flush_day()
                flush_file()
                current_file = file_id
                current_key = (month, day)
                current_values = [rainfall if rainfall is not None else 0.0]
                continue

            key = (month, day)
            if key != current_key:
                flush_day()
                current_key = key
                current_values = [rainfall if rainfall is not None else 0.0]
            else:
                current_values.append(rainfall if rainfall is not None else 0.0)

        flush_day()
        flush_file()

        consolidate_qc_session(conn, qc_session_id=qc_session_id)
        complete_qc_session(
            conn,
            qc_session_id=qc_session_id,
            status="success",
            message=(
                f"{CHECK_NAME} wrote {day_rows_written} day rows "
                f"({pass_rows} pass, {fail_rows} fail)"
            ),
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
    finally:
        conn.close()


# ------------------------------------------------------------------
# Shard entry point (for SLURM array tasks)
# ------------------------------------------------------------------

def run_exact_monthly_consistency_shard(
    *,
    ensemble_db_path,
    comparison_db_path,
    rr_db_path=None,
    tolerance: float = 0.01,
    shard_output_path,
    start_file_id: int,
    end_file_id: int,
) -> ExactMonthlyQCResult:
    """Compute QC check rows for a file-id slice and write to a shard SQLite.

    Does not create a QC session or consolidate; the merge step handles that.
    """
    from datetime import datetime, timezone

    if rr_db_path is None:
        rr_db_path = default_db_path()

    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    conn = sqlite3.connect(f"file:{ensemble_db_path}?immutable=1", uri=True)

    try:
        conn.execute(
            "ATTACH DATABASE ? AS cmp",
            (f"file:{comparison_db_path}?immutable=1",),
        )
        conn.execute(
            "ATTACH DATABASE ? AS rr",
            (f"file:{rr_db_path}?immutable=1",),
        )

        file_filter_clause, file_filter_params = _build_file_filter_clause(
            start_file_id=start_file_id,
            end_file_id=end_file_id,
        )

        rr_monthlies = _load_rr_monthlies_for_exact_files(
            conn,
            file_filter_clause=file_filter_clause,
            file_filter_params=file_filter_params,
        )

        all_rows: list[QCCheckRow] = []
        files_processed = 0
        pass_rows = 0
        fail_rows = 0
        exact_files_seen = 0
        non_exact_files_seen = 0

        current_file: Optional[int] = None
        current_key: Optional[tuple[int, int]] = None
        current_values: list[float] = []
        month_totals: Dict[int, float] = {}
        month_days: Dict[int, Set[int]] = {}

        def flush_day() -> None:
            nonlocal current_key, current_values, month_totals, month_days
            if current_key is None:
                return
            month, day = current_key
            day_median = float(median(current_values))
            month_totals[month] = month_totals.get(month, 0.0) + day_median
            month_days.setdefault(month, set()).add(day)

        def flush_file() -> None:
            nonlocal files_processed, pass_rows, fail_rows
            nonlocal exact_files_seen, non_exact_files_seen
            nonlocal month_totals, month_days, current_file

            if current_file is None:
                return

            rows = _emit_file_month_flags(
                file_id=current_file,
                month_days=month_days,
                month_totals=month_totals,
                rr_monthlies=rr_monthlies,
                tolerance=tolerance,
            )
            all_rows.extend(rows)
            files_processed += 1
            pass_rows += sum(1 for r in rows if r.qc_flag == "pass")
            fail_rows += sum(1 for r in rows if r.qc_flag == "fail")
            if current_file in rr_monthlies:
                exact_files_seen += 1
            else:
                non_exact_files_seen += 1

            month_totals = {}
            month_days = {}

        for file_id, month, day, rainfall in _iter_consensus_daily_rows(
            conn,
            file_filter_clause=file_filter_clause,
            file_filter_params=file_filter_params,
        ):
            if current_file is None:
                current_file = file_id
                current_key = (month, day)
                current_values = [rainfall if rainfall is not None else 0.0]
                continue
            if file_id != current_file:
                flush_day()
                flush_file()
                current_file = file_id
                current_key = (month, day)
                current_values = [rainfall if rainfall is not None else 0.0]
                continue
            key = (month, day)
            if key != current_key:
                flush_day()
                current_key = key
                current_values = [rainfall if rainfall is not None else 0.0]
            else:
                current_values.append(rainfall if rainfall is not None else 0.0)

        flush_day()
        flush_file()

        _write_shard(shard_output_path, all_rows, created_at)

        return ExactMonthlyQCResult(
            qc_session_id=-1,  # not assigned until merge
            files_processed=files_processed,
            day_rows_written=len(all_rows),
            pass_rows=pass_rows,
            fail_rows=fail_rows,
            exact_files_seen=exact_files_seen,
            non_exact_files_seen=non_exact_files_seen,
        )
    finally:
        conn.close()


# ------------------------------------------------------------------
# Shard merge entry point
# ------------------------------------------------------------------

def merge_exact_monthly_qc_shards(
    *,
    ensemble_db_path,
    shard_paths,
    tolerance: float = 0.01,
    num_shards: Optional[int] = None,
) -> ExactMonthlyQCResult:
    """Combine shard SQLites into one QC session in the ensemble DB."""
    shard_paths = list(shard_paths)
    if not shard_paths:
        raise ValueError("No shard paths provided")
    if num_shards is not None and len(shard_paths) != num_shards:
        raise ValueError(
            f"Expected {num_shards} shards but found {len(shard_paths)}"
        )

    conn = sqlite3.connect(f"file:{ensemble_db_path}", uri=True)
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        ensure_qc_schema(conn)

        config = {
            "check": CHECK_NAME,
            "check_version": CHECK_VERSION,
            "tolerance": tolerance,
            "shards": len(shard_paths),
        }
        qc_session_id = start_qc_session(
            conn,
            config=config,
            promotion_threshold=0.95,
        )

        total_rows = 0
        pass_rows = 0
        fail_rows = 0

        # Bulk-load tuning: this job is append-only and followed by a single
        # consolidation pass, so favor write throughput over durability.
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA temp_store = MEMORY")

        with conn:
            for shard_path in shard_paths:
                sc = sqlite3.connect(f"file:{shard_path}?immutable=1", uri=True)
                try:
                    count_row = sc.execute(
                        """
                        SELECT
                            COUNT(*) AS n,
                            SUM(CASE WHEN qc_flag = 'pass' THEN 1 ELSE 0 END) AS n_pass,
                            SUM(CASE WHEN qc_flag = 'fail' THEN 1 ELSE 0 END) AS n_fail
                        FROM qc_shard_results
                        """
                    ).fetchone()

                    total_rows += int(count_row[0] or 0)
                    pass_rows += int(count_row[1] or 0)
                    fail_rows += int(count_row[2] or 0)

                    cursor = sc.execute(
                        """
                        SELECT
                            file_id, day_of_month, month,
                            check_name, check_version, qc_score, qc_flag,
                            details_json, created_at
                        FROM qc_shard_results
                        """
                    )
                    while True:
                        batch = cursor.fetchmany(50000)
                        if not batch:
                            break
                        conn.executemany(
                            """
                            INSERT INTO daily_qc_results(
                                qc_session_id, file_id, day_of_month, month,
                                check_name, check_version, qc_score, qc_flag,
                                details_json, created_at
                            ) VALUES (?,?,?,?,?,?,?,?,?,?)
                            """,
                            [
                                (
                                    qc_session_id,
                                    int(r[0]), int(r[1]), int(r[2]),
                                    str(r[3]), str(r[4]),
                                    float(r[5]) if r[5] is not None else None,
                                    str(r[6]),
                                    r[7],
                                    str(r[8]),
                                )
                                for r in batch
                            ],
                        )
                finally:
                    sc.close()

        conn.execute("PRAGMA foreign_keys = ON")

        consolidate_qc_session(conn, qc_session_id=qc_session_id)
        complete_qc_session(
            conn,
            qc_session_id=qc_session_id,
            status="success",
            message=(
                f"{CHECK_NAME} merged {len(shard_paths)} shards, "
                f"{total_rows} rows ({pass_rows} pass, {fail_rows} fail)"
            ),
        )

        return ExactMonthlyQCResult(
            qc_session_id=qc_session_id,
            files_processed=-1,
            day_rows_written=total_rows,
            pass_rows=pass_rows,
            fail_rows=fail_rows,
            exact_files_seen=-1,
            non_exact_files_seen=-1,
        )
    finally:
        conn.close()
