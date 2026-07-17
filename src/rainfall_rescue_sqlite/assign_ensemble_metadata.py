"""Assign Rainfall Rescue metadata to ensemble records using similarity matches."""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

from .ingest import default_db_path


@dataclass(frozen=True)
class AssignmentResult:
    """Result summary of metadata assignment run."""
    ensemble_db_path: Path
    comparison_db_path: Path
    session_id: int
    total_ensemble_files: int
    exact_matches: int
    approximate_matches: int
    unmatched: int
    failures: int


def _utc_now() -> str:
    """Return current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _load_rank_matches(
    cmp_conn: sqlite3.Connection,
    ensemble_vector_id: str,
    ranks: list = None,
) -> list:
    """Load top N ranked matches for an ensemble file.
    
    Returns list of (rank, rr_vector_id, year, latitude, longitude, elevation_ft, location_name).

    Elevation is sourced from the attached RR ``stations`` table (aliased ``rr``),
    since ``rr_monthly_vectors`` does not carry it.
    """
    if ranks is None:
        ranks = [1, 2, 3]

    placeholders = ",".join("?" * len(ranks))
    rows = cmp_conn.execute(
        f"""
        SELECT
            m.query_rank,
            m.rr_vector_id,
            r.year,
            r.latitude,
            r.longitude,
            st.elevation_ft,
            r.location_name
        FROM similarity_matches m
        JOIN rr_monthly_vectors r ON r.rr_vector_id = m.rr_vector_id
        LEFT JOIN rr.stations st ON st.station_file_id = r.station_file_id
        WHERE m.ensemble_vector_id = ?
          AND m.query_rank IN ({placeholders})
        ORDER BY m.query_rank ASC
        """,
        [ensemble_vector_id] + ranks,
    ).fetchall()

    return [(int(r[0]), str(r[1]), int(r[2]), r[3], r[4], r[5], r[6]) for r in rows]


def _check_year_consensus(years: list) -> Tuple[bool, Optional[int]]:
    """Check if all years are identical.
    
    Returns (all_same, consensus_year).
    If not all same or any None, returns (False, None).
    """
    if not years or any(y is None for y in years):
        return False, None
    if len(set(years)) == 1:
        return True, years[0]
    return False, None


def _check_spatial_proximity(
    lats: list,
    lons: list,
    threshold: float = 1.0,
) -> Tuple[bool, Optional[float], Optional[float]]:
    """Check if all points are within Euclidean distance threshold of each other.
    
    Returns (all_within_threshold, centroid_lat, centroid_lon).
    Threshold is in degrees (assumes small enough area for Euclidean approximation).
    """
    if not lats or not lons or len(lats) != len(lons):
        return False, None, None

    # Filter out None values
    valid_pairs = [(lat, lon) for lat, lon in zip(lats, lons) if lat is not None and lon is not None]
    if len(valid_pairs) < len(lats):
        # Not all have valid coordinates
        return False, None, None

    if not valid_pairs:
        return False, None, None

    # Check all pairwise distances
    for i in range(len(valid_pairs)):
        for j in range(i + 1, len(valid_pairs)):
            lat1, lon1 = valid_pairs[i]
            lat2, lon2 = valid_pairs[j]
            # Euclidean distance in degrees
            distance = math.sqrt((lat2 - lat1) ** 2 + (lon2 - lon1) ** 2)
            if distance > threshold:
                return False, None, None

    # All pairs within threshold; compute centroid
    centroid_lat = sum(lat for lat, _ in valid_pairs) / len(valid_pairs)
    centroid_lon = sum(lon for _, lon in valid_pairs) / len(valid_pairs)
    return True, centroid_lat, centroid_lon


def assign_ensemble_metadata(
    ensemble_db_path: Path,
    comparison_db_path: Path,
    session_id: Optional[int] = None,
    rr_db_path: Optional[Path] = None,
) -> AssignmentResult:
    """Assign RR metadata to ensemble files using similarity matches.
    
    Two-phase matching:
    1. Exact match: rank-1 with exact_agreement_count >= 9 → copy all metadata
    2. Approximate match: top-3 ranks by cosine score
       - Year consensus: all 3 must have same year
       - Spatial proximity: all 3 within Euclidean distance 1.0 degree → assign centroid
       - Both must pass to record match; otherwise all metadata NULL
    
    Clears existing metadata on each run (idempotent).
    """
    if rr_db_path is None:
        rr_db_path = default_db_path()

    ens_conn = sqlite3.connect(ensemble_db_path)
    cmp_conn = sqlite3.connect(f"file:{comparison_db_path}?immutable=1", uri=True)
    # Elevation lives in the RR stations table, not in rr_monthly_vectors, so
    # attach the RR database (read-only) and join on station_file_id.
    cmp_conn.execute(
        "ATTACH DATABASE ? AS rr", (f"file:{rr_db_path}?immutable=1",)
    )

    if session_id is None:
        session_id = cmp_conn.execute(
            "SELECT MAX(session_id) FROM similarity_sessions"
        ).fetchone()[0]

    try:
        # Count total ensemble files
        total_files = ens_conn.execute(
            "SELECT COUNT(*) FROM ensemble_files"
        ).fetchone()[0]

        # Clear existing metadata
        ens_conn.execute(
            """
            UPDATE ensemble_files
            SET matched_location_name = NULL,
                matched_year = NULL,
                matched_latitude = NULL,
                matched_longitude = NULL,
                matched_elevation_ft = NULL,
                match_type = NULL,
                match_source_session_id = NULL
            """
        )

        exact_count = 0
        approx_count = 0
        failures = 0
        processed = 0

        # Fetch all ensemble file IDs
        file_ids = ens_conn.execute(
            "SELECT file_id FROM ensemble_files ORDER BY file_id"
        ).fetchall()

        for (file_id,) in file_ids:
            processed += 1
            ensemble_vector_id = f"ensemble_file::{file_id}"

            try:
                # Query rank-1 match to check for exact agreement
                rank1_row = cmp_conn.execute(
                    """
                    SELECT m.exact_agreement_count, m.rr_vector_id,
                           r.location_name, r.year, r.latitude, r.longitude, st.elevation_ft
                    FROM similarity_matches m
                    JOIN rr_monthly_vectors r ON r.rr_vector_id = m.rr_vector_id
                    LEFT JOIN rr.stations st ON st.station_file_id = r.station_file_id
                    WHERE m.ensemble_vector_id = ?
                      AND m.session_id = ?
                      AND m.query_rank = 1
                    LIMIT 1
                    """,
                    (ensemble_vector_id, session_id),
                ).fetchone()

                if rank1_row is None:
                    # No match at all
                    continue

                exact_agreement = int(rank1_row[0])

                if exact_agreement >= 9:
                    # Exact match: copy all metadata from rank-1
                    location_name = rank1_row[2]
                    year = int(rank1_row[3]) if rank1_row[3] is not None else None
                    lat = rank1_row[4]
                    lon = rank1_row[5]
                    elev = rank1_row[6]

                    ens_conn.execute(
                        """
                        UPDATE ensemble_files
                        SET matched_location_name = ?,
                            matched_year = ?,
                            matched_latitude = ?,
                            matched_longitude = ?,
                            matched_elevation_ft = ?,
                            match_type = ?,
                            match_source_session_id = ?
                        WHERE file_id = ?
                        """,
                        (location_name, year, lat, lon, elev, "exact", session_id, file_id),
                    )
                    exact_count += 1
                else:
                    # Try approximate match: check top-3 by cosine score
                    rank_matches = _load_rank_matches(cmp_conn, ensemble_vector_id, ranks=[1, 2, 3])

                    if len(rank_matches) < 3:
                        # Not enough matches for approximate
                        continue

                    years = [m[2] for m in rank_matches]
                    lats = [m[3] for m in rank_matches]
                    lons = [m[4] for m in rank_matches]

                    year_ok, consensus_year = _check_year_consensus(years)
                    spatial_ok, centroid_lat, centroid_lon = _check_spatial_proximity(lats, lons)

                    if year_ok and spatial_ok:
                        # Both constraints pass: assign metadata
                        ens_conn.execute(
                            """
                            UPDATE ensemble_files
                            SET matched_year = ?,
                                matched_latitude = ?,
                                matched_longitude = ?,
                                match_type = ?,
                                match_source_session_id = ?
                            WHERE file_id = ?
                            """,
                            (consensus_year, centroid_lat, centroid_lon, "approximate", session_id, file_id),
                        )
                        approx_count += 1
                    # else: no match assigned, all NULL (implicitly handled by clearing above)

            except Exception as e:
                failures += 1
                print(f"Warning: Failed to process file_id={file_id}: {e}")

        ens_conn.commit()

        unmatched = total_files - exact_count - approx_count

        return AssignmentResult(
            ensemble_db_path=ensemble_db_path,
            comparison_db_path=comparison_db_path,
            session_id=session_id,
            total_ensemble_files=total_files,
            exact_matches=exact_count,
            approximate_matches=approx_count,
            unmatched=unmatched,
            failures=failures,
        )

    finally:
        ens_conn.close()
        cmp_conn.close()
