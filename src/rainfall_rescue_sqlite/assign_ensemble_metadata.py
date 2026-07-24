"""Assign Rainfall Rescue metadata to ensemble records using similarity matches."""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

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


@dataclass(frozen=True)
class ParquetAssignmentResult:
    """Result summary of a Parquet-backend metadata assignment run."""

    comparison_root: Path
    output_path: Path
    session_id: int
    total_ensemble_files: int
    exact_matches: int
    approximate_matches: int
    unmatched: int


ENSEMBLE_METADATA_SCHEMA = pa.schema(
    [
        ("file_id", pa.int64()),
        ("file_name", pa.string()),
        ("matched_location_name", pa.string()),
        ("matched_year", pa.int64()),
        ("matched_latitude", pa.float64()),
        ("matched_longitude", pa.float64()),
        ("matched_elevation_ft", pa.float64()),
        ("match_type", pa.string()),
        ("match_source_session_id", pa.int64()),
    ]
)


def _parquet_glob(dir_path: Path) -> str:
    return str((dir_path / "*.parquet").resolve())


def assign_ensemble_metadata_parquet(
    *,
    comparison_root: Optional[Path] = None,
    ensemble_dataset_root: Optional[Path] = None,
    rr_dataset_root: Optional[Path] = None,
    session_id: Optional[int] = None,
    output_root: Optional[Path] = None,
    batch_rows: int = 100_000,
) -> ParquetAssignmentResult:
    """Assign RR metadata to ensemble files from the Parquet similarity outputs.

    This is the DuckDB/Parquet equivalent of :func:`assign_ensemble_metadata`.
    Rather than updating a mutable SQLite table in place, it writes a fresh
    ``ensemble_metadata/session_XXXXXX.parquet`` table under ``output_root``
    (defaults to ``comparison_root``) with one row per ensemble file — NULL
    metadata for unmatched files — so downstream code can join on ``file_id``.

    Matching rules are identical to the SQLite version:

    1. Exact match: rank-1 with ``exact_agreement_count >= 9`` copies all
       metadata (location, year, lat/lon, elevation) from the rank-1 station.
    2. Approximate match: requires the top-3 ranks to be present, agree on year,
       and lie within 1.0 degree of each other; assigns the consensus year and
       centroid position (location name and elevation left NULL).
    3. Otherwise the file is unmatched and all metadata stay NULL.
    """
    from .parquet_ingest import (
        default_ensemble_parquet_root,
        default_rainfall_rescue_parquet_root,
    )
    from .parquet_similarity import default_comparison_parquet_root

    if comparison_root is None:
        comparison_root = default_comparison_parquet_root()
    if ensemble_dataset_root is None:
        ensemble_dataset_root = default_ensemble_parquet_root()
    if rr_dataset_root is None:
        rr_dataset_root = default_rainfall_rescue_parquet_root()
    if output_root is None:
        output_root = comparison_root

    conn = duckdb.connect()

    if session_id is None:
        session_id = conn.execute(
            f"SELECT MAX(session_id) FROM read_parquet("
            f"'{_parquet_glob(comparison_root / 'similarity_sessions')}')"
        ).fetchone()[0]
    if session_id is None:
        conn.close()
        raise SystemExit(
            "No similarity sessions found; run the matching pipeline first."
        )
    session_id = int(session_id)

    total_files = int(
        conn.execute(
            f"SELECT COUNT(*) FROM read_parquet("
            f"'{_parquet_glob(ensemble_dataset_root / 'ensemble_files')}')"
        ).fetchone()[0]
    )

    # --- Phase 1: decide an assignment for each ensemble file with matches ---
    match_sql = f"""
        SELECT
            m.ensemble_vector_id AS ensemble_vector_id,
            CAST(m.query_rank AS BIGINT) AS query_rank,
            CAST(m.exact_agreement_count AS BIGINT) AS exact_agreement_count,
            CAST(r.location_name AS VARCHAR) AS location_name,
            CAST(r.year AS BIGINT) AS year,
            CAST(r.latitude AS DOUBLE) AS latitude,
            CAST(r.longitude AS DOUBLE) AS longitude,
            CAST(st.elevation_ft AS DOUBLE) AS elevation_ft
        FROM read_parquet('{_parquet_glob(comparison_root / 'similarity_matches')}') m
        JOIN read_parquet('{_parquet_glob(comparison_root / 'rr_monthly_vectors')}') r
          ON r.rr_vector_id = m.rr_vector_id
        LEFT JOIN read_parquet('{_parquet_glob(rr_dataset_root / 'stations')}') st
          ON st.station_file_id = r.station_file_id
        WHERE m.session_id = {session_id}
          AND m.query_rank IN (1, 2, 3)
        ORDER BY m.ensemble_vector_id, m.query_rank
    """

    assignments: Dict[int, dict] = {}
    exact_count = 0
    approx_count = 0

    def _decide(vid: str, ranks_by_num: Dict[int, dict]) -> None:
        nonlocal exact_count, approx_count
        rank1 = ranks_by_num.get(1)
        if rank1 is None:
            return
        file_id = int(vid.split("::", 1)[1])

        if int(rank1["exact_agreement_count"]) >= 9:
            assignments[file_id] = {
                "matched_location_name": rank1["location_name"],
                "matched_year": None if rank1["year"] is None else int(rank1["year"]),
                "matched_latitude": rank1["latitude"],
                "matched_longitude": rank1["longitude"],
                "matched_elevation_ft": rank1["elevation_ft"],
                "match_type": "exact",
            }
            exact_count += 1
            return

        ordered = [ranks_by_num.get(k) for k in (1, 2, 3)]
        if any(x is None for x in ordered):
            return
        years = [x["year"] for x in ordered]
        lats = [x["latitude"] for x in ordered]
        lons = [x["longitude"] for x in ordered]
        year_ok, consensus_year = _check_year_consensus(years)
        spatial_ok, centroid_lat, centroid_lon = _check_spatial_proximity(lats, lons)
        if year_ok and spatial_ok:
            assignments[file_id] = {
                "matched_location_name": None,
                "matched_year": consensus_year,
                "matched_latitude": centroid_lat,
                "matched_longitude": centroid_lon,
                "matched_elevation_ft": None,
                "match_type": "approximate",
            }
            approx_count += 1

    reader = conn.execute(match_sql).fetch_record_batch(batch_rows)
    cur_vid: Optional[str] = None
    cur_ranks: Dict[int, dict] = {}
    for batch in reader:
        d = batch.to_pydict()
        vids = d["ensemble_vector_id"]
        rank = d["query_rank"]
        exact = d["exact_agreement_count"]
        loc = d["location_name"]
        yr = d["year"]
        lat = d["latitude"]
        lon = d["longitude"]
        elev = d["elevation_ft"]
        for i in range(batch.num_rows):
            vid = vids[i]
            if vid != cur_vid:
                if cur_vid is not None:
                    _decide(cur_vid, cur_ranks)
                cur_vid = vid
                cur_ranks = {}
            cur_ranks[int(rank[i])] = {
                "exact_agreement_count": exact[i],
                "location_name": loc[i],
                "year": yr[i],
                "latitude": lat[i],
                "longitude": lon[i],
                "elevation_ft": elev[i],
            }
    if cur_vid is not None:
        _decide(cur_vid, cur_ranks)

    # --- Phase 2: write one metadata row per ensemble file (NULL if unmatched)
    out_path = (
        output_root / "ensemble_metadata" / f"session_{session_id:06d}.parquet"
    ).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    files_reader = conn.execute(
        f"SELECT CAST(file_id AS BIGINT) AS file_id, "
        f"CAST(file_name AS VARCHAR) AS file_name "
        f"FROM read_parquet('{_parquet_glob(ensemble_dataset_root / 'ensemble_files')}') "
        f"ORDER BY file_id"
    ).fetch_record_batch(batch_rows)

    writer = pq.ParquetWriter(out_path, ENSEMBLE_METADATA_SCHEMA, compression="zstd")
    buffer: list = []
    try:
        for batch in files_reader:
            d = batch.to_pydict()
            fids = d["file_id"]
            fnames = d["file_name"]
            for i in range(batch.num_rows):
                file_id = int(fids[i])
                a = assignments.get(file_id)
                if a is None:
                    buffer.append(
                        {
                            "file_id": file_id,
                            "file_name": fnames[i],
                            "matched_location_name": None,
                            "matched_year": None,
                            "matched_latitude": None,
                            "matched_longitude": None,
                            "matched_elevation_ft": None,
                            "match_type": None,
                            "match_source_session_id": None,
                        }
                    )
                else:
                    buffer.append(
                        {
                            "file_id": file_id,
                            "file_name": fnames[i],
                            "matched_location_name": a["matched_location_name"],
                            "matched_year": a["matched_year"],
                            "matched_latitude": a["matched_latitude"],
                            "matched_longitude": a["matched_longitude"],
                            "matched_elevation_ft": a["matched_elevation_ft"],
                            "match_type": a["match_type"],
                            "match_source_session_id": session_id,
                        }
                    )
                if len(buffer) >= 50_000:
                    writer.write_table(
                        pa.Table.from_pylist(buffer, schema=ENSEMBLE_METADATA_SCHEMA)
                    )
                    buffer.clear()
        if buffer:
            writer.write_table(
                pa.Table.from_pylist(buffer, schema=ENSEMBLE_METADATA_SCHEMA)
            )
    finally:
        writer.close()
        conn.close()

    unmatched = total_files - exact_count - approx_count

    return ParquetAssignmentResult(
        comparison_root=comparison_root,
        output_path=out_path,
        session_id=session_id,
        total_ensemble_files=total_files,
        exact_matches=exact_count,
        approximate_matches=approx_count,
        unmatched=unmatched,
    )
