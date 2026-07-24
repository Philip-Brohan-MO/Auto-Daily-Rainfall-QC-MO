"""QC session and write-path helpers for iterative daily-rainfall quality control.

This module provides a lightweight API to:
- start/finish QC sessions,
- write day-level QC features,
- write per-check QC results,
- derive a final per-day QC status with automatic promotion.

It is designed to be called from both local notebook checks and distributed
shard runners.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class QCFeatureRow:
    file_id: int
    day_of_month: int
    month: int
    climatology_median: Optional[float]
    climatology_mad: Optional[float]
    nearby_median: Optional[float]
    nearby_mad: Optional[float]
    temporal_delta: Optional[float]
    feature_version: str = "v1"


@dataclass(frozen=True)
class QCCheckRow:
    file_id: int
    day_of_month: int
    month: int
    check_name: str
    check_version: str
    qc_score: Optional[float]
    qc_flag: str
    details: Optional[dict] = None


def start_qc_session(
    connection: sqlite3.Connection,
    *,
    config: dict,
    parent_qc_session_id: Optional[int] = None,
    promotion_threshold: float = 0.95,
) -> int:
    """Create and return a new QC session id."""
    with connection:
        cursor = connection.execute(
            """
            INSERT INTO qc_sessions(
                started_at,
                status,
                parent_qc_session_id,
                config_json,
                promotion_threshold
            ) VALUES (?, 'running', ?, ?, ?)
            """,
            (
                _utc_now(),
                parent_qc_session_id,
                json.dumps(config, sort_keys=True),
                promotion_threshold,
            ),
        )
    return int(cursor.lastrowid)


def complete_qc_session(
    connection: sqlite3.Connection,
    *,
    qc_session_id: int,
    status: str,
    message: Optional[str] = None,
) -> None:
    """Mark a QC session as completed/failed."""
    if status not in {"success", "failed", "completed_with_warnings"}:
        raise ValueError("status must be one of success|failed|completed_with_warnings")
    with connection:
        connection.execute(
            """
            UPDATE qc_sessions
            SET completed_at = ?,
                status = ?,
                message = ?
            WHERE qc_session_id = ?
            """,
            (_utc_now(), status, message, qc_session_id),
        )


def upsert_qc_features(connection: sqlite3.Connection, rows: Iterable[QCFeatureRow]) -> None:
    """Insert/update day-level feature rows."""
    now = _utc_now()
    payload = [
        (
            r.file_id,
            r.day_of_month,
            r.month,
            r.climatology_median,
            r.climatology_mad,
            r.nearby_median,
            r.nearby_mad,
            r.temporal_delta,
            r.feature_version,
            now,
        )
        for r in rows
    ]
    if not payload:
        return
    with connection:
        connection.executemany(
            """
            INSERT INTO daily_qc_features(
                file_id,
                day_of_month,
                month,
                climatology_median,
                climatology_mad,
                nearby_median,
                nearby_mad,
                temporal_delta,
                feature_version,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_id, day_of_month, month) DO UPDATE SET
                climatology_median = excluded.climatology_median,
                climatology_mad = excluded.climatology_mad,
                nearby_median = excluded.nearby_median,
                nearby_mad = excluded.nearby_mad,
                temporal_delta = excluded.temporal_delta,
                feature_version = excluded.feature_version,
                updated_at = excluded.updated_at
            """,
            payload,
        )


def insert_qc_check_rows(
    connection: sqlite3.Connection,
    *,
    qc_session_id: int,
    rows: Iterable[QCCheckRow],
) -> None:
    """Insert per-check QC rows for a session."""
    now = _utc_now()
    payload = []
    for r in rows:
        if r.qc_flag not in {"pass", "review", "fail"}:
            raise ValueError(f"Invalid qc_flag: {r.qc_flag}")
        payload.append(
            (
                qc_session_id,
                r.file_id,
                r.day_of_month,
                r.month,
                r.check_name,
                r.check_version,
                r.qc_score,
                r.qc_flag,
                json.dumps(r.details, sort_keys=True) if r.details is not None else None,
                now,
            )
        )
    if not payload:
        return
    with connection:
        connection.executemany(
            """
            INSERT INTO daily_qc_results(
                qc_session_id,
                file_id,
                day_of_month,
                month,
                check_name,
                check_version,
                qc_score,
                qc_flag,
                details_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )


def consolidate_qc_session(connection: sqlite3.Connection, *, qc_session_id: int) -> None:
    """Build final per-day QC status for a session from per-check rows.

    Rule:
    - Any fail -> final fail
    - Else any review -> final review
    - Else final pass
    Score = average of non-null per-check scores
    Promotion = 1 only for final pass and score >= promotion_threshold
    """
    with connection:
        threshold = connection.execute(
            "SELECT promotion_threshold FROM qc_sessions WHERE qc_session_id = ?",
            (qc_session_id,),
        ).fetchone()
        if threshold is None:
            raise ValueError(f"qc_session_id {qc_session_id} not found")
        promotion_threshold = float(threshold[0])

        connection.execute(
            "DELETE FROM daily_qc_status WHERE qc_session_id = ?",
            (qc_session_id,),
        )

        connection.execute(
            """
            INSERT INTO daily_qc_status(
                qc_session_id,
                file_id,
                day_of_month,
                month,
                final_score,
                final_flag,
                promoted_good,
                promoted_at
            )
            SELECT
                qc_session_id,
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
                     AND COALESCE(AVG(qc_score), 0.0) >= ? THEN 1
                    ELSE 0
                END AS promoted_good,
                CASE
                    WHEN SUM(CASE WHEN qc_flag = 'fail' THEN 1 ELSE 0 END) = 0
                     AND SUM(CASE WHEN qc_flag = 'review' THEN 1 ELSE 0 END) = 0
                     AND COALESCE(AVG(qc_score), 0.0) >= ? THEN ?
                    ELSE NULL
                END AS promoted_at
            FROM daily_qc_results
            WHERE qc_session_id = ?
            GROUP BY qc_session_id, file_id, day_of_month, month
            """,
            (promotion_threshold, promotion_threshold, _utc_now(), qc_session_id),
        )
