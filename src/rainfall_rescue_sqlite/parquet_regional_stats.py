"""Regional neighbour statistics on Parquet datasets using DuckDB.

Stage one of the second QC check. For every *located* station-day (a station
with an assigned ``matched_latitude`` / ``matched_longitude`` / ``matched_year``)
this computes robust neighbour statistics drawn from station-days that **passed**
the first QC check (``daily_qc_status.final_flag = 'pass'``):

* median of the neighbours' consensus rainfall for the same calendar day,
* the number of such neighbours, and
* the median absolute deviation (MAD) of the neighbours' values,

at two search radii (20 km and 50 km). The consensus station itself is always
excluded from its own neighbour set.

"Same calendar day" means the same ``matched_year`` (each ensemble file is a
single station-year), ``month`` and ``day_of_month``. Distances use an
equirectangular approximation, which is accurate at UK scale, with a lat/lon
bounding-box pre-filter so DuckDB can range-join rather than cross-join.

The expensive daily-consensus median is computed only for the files that can
actually participate for the requested target slice -- the targets themselves
plus located stations that share a target year and fall within the target
bounding box expanded by the large search radius. This keeps the ``median``
hash aggregate small (a few thousand files) instead of spanning every file_id
in the dataset (which would need well over the RAM of a small workstation).

Results are written as a Parquet table ``regional_daily_stats`` under an output
root (defaults to ``$PDIR/regional_stats_parquet``), one row per located target
station-day. Targets with no neighbours are still emitted (counts 0, stats NULL)
so downstream QC has complete coverage.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

from .parquet_ingest import default_ensemble_parquet_root
from .parquet_similarity import _configure_duckdb, default_comparison_parquet_root

# Search radii for the two neighbour rings. Column names in the output table
# (``*_20km`` / ``*_50km``) assume these values.
RADIUS_SMALL_KM = 20.0
RADIUS_LARGE_KM = 50.0

# Great-circle constants for the equirectangular distance / bounding box.
EARTH_RADIUS_KM = 6371.0
KM_PER_DEG_LAT = 111.0


@dataclass(frozen=True)
class RegionalStatsResult:
    """Summary of a regional-statistics compute run."""

    metadata_session_id: int
    qc_session_id: int
    target_rows_written: int
    targets_with_neighbours: int
    output_path: Path


@dataclass(frozen=True)
class DailyConsensusResult:
    """Summary of a daily-consensus precompute run."""

    rows_written: int
    output_path: Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _glob_sql(dir_path: Path) -> str:
    return str((dir_path / "*.parquet").resolve())


def _connect() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection honouring the env-based memory/temp limits.

    Mirrors the QC pipeline: ``DUCKDB_MEMORY_LIMIT`` / ``DUCKDB_TEMP_DIR`` (set
    by the SLURM sbatch scripts) are applied so large spatial joins stay within
    the job's cgroup allocation and spill to node-local scratch.
    """
    conn = duckdb.connect()
    _configure_duckdb(conn)
    return conn


def default_regional_stats_parquet_root() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass output_root explicitly")
    return Path(pdir) / "regional_stats_parquet"


def default_regional_stats_shard_dir() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass shard_dir explicitly")
    return Path(pdir) / "regional_stats_shards"


def default_daily_consensus_parquet_root() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass consensus_root explicitly")
    return Path(pdir) / "daily_consensus_parquet"


def default_daily_consensus_shard_dir() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass shard_dir explicitly")
    return Path(pdir) / "daily_consensus_shards"


def compute_daily_consensus_shard_parquet(
    *,
    ensemble_dataset_root: Optional[Path] = None,
    output_path: Path,
    start_file_id: Optional[int] = None,
    end_file_id: Optional[int] = None,
) -> DailyConsensusResult:
    """Precompute the daily consensus (member median) for a file_id slice.

    Writes one row per ``(file_id, month, day_of_month)`` with
    ``consensus_value = median(COALESCE(rainfall, 0.0))`` across the ensemble
    members, restricted to ``start_file_id..end_file_id``. Because the slice is
    a *contiguous* file_id range, DuckDB prunes ``ensemble_daily_values`` row
    groups and only the slice's member-day rows are buffered by the holistic
    ``median`` -- so each shard stays small regardless of the total dataset
    size. The merged output is what the regional-stats shards read instead of
    re-running this median over a nationally-scattered file pool.
    """
    if ensemble_dataset_root is None:
        ensemble_dataset_root = default_ensemble_parquet_root()
    daily_glob = _glob_sql(ensemble_dataset_root / "ensemble_daily_values")

    out_path = Path(output_path).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    clauses = []
    if start_file_id is not None:
        clauses.append(f"file_id >= {int(start_file_id)}")
    if end_file_id is not None:
        clauses.append(f"file_id <= {int(end_file_id)}")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    conn = _connect()
    try:
        conn.execute(
            f"""
            COPY (
                SELECT file_id,
                       CAST(month AS TINYINT) AS month,
                       CAST(day_of_month AS TINYINT) AS day_of_month,
                       median(COALESCE(rainfall, 0.0)) AS consensus_value
                FROM read_parquet('{daily_glob}')
                {where}
                GROUP BY file_id, month, day_of_month
            ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        n = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]
    finally:
        conn.close()

    return DailyConsensusResult(rows_written=int(n or 0), output_path=out_path)


def merge_daily_consensus_shards_parquet(
    *,
    consensus_root: Path,
    shard_paths: "list[Path]",
    num_shards: Optional[int] = None,
) -> DailyConsensusResult:
    """Concatenate daily-consensus shards into one ``daily_consensus`` dataset.

    Shards cover disjoint, contiguous file_id ranges, so this is a low-memory
    streaming concat (no global sort). The shards are already emitted in file_id
    order, so row-group file_id min/max stats stay tight enough for the regional
    reader to prune.
    """
    if not shard_paths:
        raise ValueError("No consensus shard files to merge")
    if num_shards is not None and len(shard_paths) != num_shards:
        raise ValueError(
            f"Expected {num_shards} consensus shards but got {len(shard_paths)}"
        )

    shard_list_sql = "[" + ", ".join(
        f"'{Path(p).resolve()}'" for p in shard_paths
    ) + "]"

    out_dir = consensus_root / "daily_consensus"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = (out_dir / "daily_consensus.parquet").resolve()

    conn = _connect()
    try:
        conn.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet({shard_list_sql})
            ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        n = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]
    finally:
        conn.close()

    return DailyConsensusResult(rows_written=int(n or 0), output_path=out_path)


def default_roots() -> tuple[Path, Path, Path, Path]:
    """Return ``(ensemble_root, comparison_root, qc_root, regional_root)``."""
    from .parquet_qc_exact_monthly import default_qc_parquet_root

    return (
        default_ensemble_parquet_root(),
        default_comparison_parquet_root(),
        default_qc_parquet_root(),
        default_regional_stats_parquet_root(),
    )


def _resolve_metadata_session_id(
    conn: duckdb.DuckDBPyConnection, comparison_root: Path, explicit: Optional[int]
) -> int:
    if explicit is not None:
        return int(explicit)
    value = conn.execute(
        f"SELECT MAX(match_source_session_id) FROM read_parquet("
        f"'{_glob_sql(comparison_root / 'ensemble_metadata')}')"
    ).fetchone()[0]
    if value is None:
        raise ValueError("No ensemble_metadata sessions found; run metadata assignment first")
    return int(value)


def _resolve_qc_session_id(
    conn: duckdb.DuckDBPyConnection, qc_root: Path, explicit: Optional[int]
) -> int:
    if explicit is not None:
        return int(explicit)
    value = conn.execute(
        f"SELECT MAX(qc_session_id) FROM read_parquet("
        f"'{_glob_sql(qc_root / 'daily_qc_status')}')"
    ).fetchone()[0]
    if value is None:
        raise ValueError("No QC sessions found in qc_root; run the first QC check first")
    return int(value)


def compute_regional_daily_stats_parquet(
    *,
    ensemble_dataset_root: Optional[Path] = None,
    comparison_root: Optional[Path] = None,
    qc_root: Path,
    output_root: Optional[Path] = None,
    output_path: Optional[Path] = None,
    consensus_root: Optional[Path] = None,
    qc_session_id: Optional[int] = None,
    metadata_session_id: Optional[int] = None,
    start_file_id: Optional[int] = None,
    end_file_id: Optional[int] = None,
) -> RegionalStatsResult:
    """Compute neighbour statistics for located station-days and write Parquet.

    Parameters
    ----------
    ensemble_dataset_root, comparison_root
        Roots of the ensemble and comparison Parquet datasets. Default to the
        ``$PDIR``-derived locations.
    qc_root
        Root of the first-QC-check Parquet outputs (holds ``daily_qc_status``).
    output_root
        Where to write ``regional_daily_stats`` when ``output_path`` is not
        given. Defaults to ``$PDIR/regional_stats_parquet``.
    output_path
        Explicit destination Parquet file. When set, the output is written here
        (parent dirs created) and ``output_root`` is ignored -- used by the
        sharded SLURM runner to write one file per shard.
    consensus_root
        Root of the precomputed daily-consensus dataset (holding
        ``daily_consensus/*.parquet``). When set, the daily consensus is read
        from there instead of being recomputed with a holistic ``median`` over
        ``ensemble_daily_values`` -- this is what keeps the sharded SLURM run
        within memory, since a nationally-scattered file_id shard would
        otherwise buffer hundreds of millions of member-day values at once.
    qc_session_id
        QC session whose passing station-days form the neighbour pool. Defaults
        to the latest session in ``daily_qc_status``.
    metadata_session_id
        Metadata (matching) session providing station locations. Defaults to the
        latest ``ensemble_metadata`` session.
    start_file_id, end_file_id
        Optional inclusive ``file_id`` range restricting the *target* stations.
        The neighbour pool is every passing station-day that could lie within
        the large radius of those targets (same year, target bounding box
        expanded by the radius); the daily-consensus median is scoped to just
        those files so a small target slice stays cheap. Used for smoke tests
        and, later, sharding.
    """
    if ensemble_dataset_root is None:
        ensemble_dataset_root = default_ensemble_parquet_root()
    if comparison_root is None:
        comparison_root = default_comparison_parquet_root()
    if output_path is None and output_root is None:
        output_root = default_regional_stats_parquet_root()

    metadata_glob = _glob_sql(comparison_root / "ensemble_metadata")
    daily_glob = _glob_sql(ensemble_dataset_root / "ensemble_daily_values")
    status_glob = _glob_sql(qc_root / "daily_qc_status")

    conn = _connect()
    try:
        metadata_session_id = _resolve_metadata_session_id(
            conn, comparison_root, metadata_session_id
        )
        qc_session_id = _resolve_qc_session_id(conn, qc_root, qc_session_id)

        if output_path is not None:
            out_path = Path(output_path).resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            out_dir = output_root / "regional_daily_stats"
            out_dir.mkdir(parents=True, exist_ok=True)
            suffix = (
                "all"
                if start_file_id is None and end_file_id is None
                else f"{start_file_id if start_file_id is not None else 'min'}"
                f"_{end_file_id if end_file_id is not None else 'max'}"
            )
            out_path = (
                out_dir
                / f"session_meta{metadata_session_id:06d}_qc{qc_session_id:06d}_{suffix}.parquet"
            ).resolve()

        # Optional file_id range restricting the target stations only.
        target_clauses = []
        if start_file_id is not None:
            target_clauses.append(f"m.file_id >= {int(start_file_id)}")
        if end_file_id is not None:
            target_clauses.append(f"m.file_id <= {int(end_file_id)}")
        target_filter = ("AND " + " AND ".join(target_clauses)) if target_clauses else ""

        # Bounding-box half-widths (degrees) for the larger radius. Longitude
        # width scales with 1/cos(lat) and is evaluated per target row in SQL.
        dlat = RADIUS_LARGE_KM / KM_PER_DEG_LAT
        now = _utc_now()

        # Daily consensus source: read the precomputed table when available
        # (bounded memory), otherwise fall back to an inline holistic median
        # scoped to the needed files (fine for small, local slices only).
        if consensus_root is not None:
            consensus_glob = _glob_sql(Path(consensus_root) / "daily_consensus")
            daily_cte = f"""daily AS (
                    -- consensus read from the precomputed daily_consensus table
                    -- (no per-shard holistic median), scoped to needed files
                    SELECT file_id, month, day_of_month, consensus_value AS value
                    FROM read_parquet('{consensus_glob}')
                    WHERE file_id IN (SELECT file_id FROM needed_files)
                )"""
        else:
            daily_cte = f"""daily AS (
                    -- consensus daily rainfall computed ONLY for files we need
                    -- (targets + candidate neighbours), so the median hash
                    -- aggregate stays small instead of spanning every file_id
                    SELECT file_id, month, day_of_month,
                           median(COALESCE(rainfall, 0.0)) AS value
                    FROM read_parquet('{daily_glob}')
                    WHERE file_id IN (SELECT file_id FROM needed_files)
                    GROUP BY file_id, month, day_of_month
                )"""

        conn.execute(
            f"""
            COPY (
                WITH meta AS (
                    SELECT file_id,
                           matched_year AS year,
                           matched_latitude AS lat,
                           matched_longitude AS lon
                    FROM read_parquet('{metadata_glob}')
                    WHERE match_source_session_id = {metadata_session_id}
                      AND matched_latitude IS NOT NULL
                      AND matched_longitude IS NOT NULL
                      AND matched_year IS NOT NULL
                ),
                target_meta AS (
                    -- located stations in the requested file_id range (targets)
                    SELECT m.file_id, m.year, m.lat, m.lon
                    FROM meta m
                    WHERE 1 = 1 {target_filter}
                ),
                tbox AS (
                    -- bounding box of the targets; ``alat`` is the widest latitude
                    -- used for the longitude margin below
                    SELECT min(lat) AS lat_lo, max(lat) AS lat_hi,
                           min(lon) AS lon_lo, max(lon) AS lon_hi,
                           max(abs(lat)) AS alat
                    FROM target_meta
                ),
                neighbour_files AS (
                    -- the only non-target files whose consensus we may need: located
                    -- stations sharing a target year and lying within
                    -- {RADIUS_LARGE_KM} km of the target bounding box
                    SELECT ml.file_id
                    FROM meta ml, tbox b
                    WHERE ml.year IN (SELECT DISTINCT year FROM target_meta)
                      AND ml.lat BETWEEN b.lat_lo - {dlat} AND b.lat_hi + {dlat}
                      AND ml.lon BETWEEN
                            b.lon_lo - ({RADIUS_LARGE_KM} / ({KM_PER_DEG_LAT}
                                * cos(radians(b.alat))))
                         AND b.lon_hi + ({RADIUS_LARGE_KM} / ({KM_PER_DEG_LAT}
                                * cos(radians(b.alat))))
                ),
                needed_files AS (
                    SELECT file_id FROM target_meta
                    UNION
                    SELECT file_id FROM neighbour_files
                ),
                {daily_cte},
                pass_days AS (
                    SELECT file_id, month, day_of_month
                    FROM read_parquet('{status_glob}')
                    WHERE qc_session_id = {qc_session_id}
                      AND final_flag = 'pass'
                ),
                targets AS (
                    SELECT t.file_id, t.year, d.month, d.day_of_month,
                           t.lat, t.lon, d.value
                    FROM target_meta t
                    JOIN daily d USING (file_id)
                ),
                neighbours AS (
                    SELECT m.file_id, m.year, d.month, d.day_of_month,
                           m.lat, m.lon, d.value
                    FROM meta m
                    JOIN daily d USING (file_id)
                    JOIN pass_days p
                      ON p.file_id = d.file_id
                     AND p.month = d.month
                     AND p.day_of_month = d.day_of_month
                ),
                pairs_raw AS (
                    SELECT
                        t.file_id, t.year, t.month, t.day_of_month, t.value AS target_value,
                        n.value AS nb_value,
                        {EARTH_RADIUS_KM} * sqrt(
                            pow(radians(n.lat - t.lat), 2)
                            + pow(radians(n.lon - t.lon)
                                  * cos(radians((t.lat + n.lat) / 2.0)), 2)
                        ) AS dist_km
                    FROM targets t
                    JOIN neighbours n
                      ON n.year = t.year
                     AND n.month = t.month
                     AND n.day_of_month = t.day_of_month
                     AND n.file_id <> t.file_id
                     AND n.lat BETWEEN t.lat - {dlat} AND t.lat + {dlat}
                     AND n.lon BETWEEN
                            t.lon - {RADIUS_LARGE_KM} / ({KM_PER_DEG_LAT}
                                * cos(radians(t.lat)))
                         AND t.lon + {RADIUS_LARGE_KM} / ({KM_PER_DEG_LAT}
                                * cos(radians(t.lat)))
                ),
                pairs AS (
                    SELECT * FROM pairs_raw WHERE dist_km <= {RADIUS_LARGE_KM}
                ),
                agg AS (
                    SELECT
                        file_id, year, month, day_of_month,
                        count(*) FILTER (WHERE dist_km <= {RADIUS_SMALL_KM}) AS n_20km,
                        median(nb_value) FILTER (WHERE dist_km <= {RADIUS_SMALL_KM})
                            AS median_20km,
                        count(*) AS n_50km,
                        median(nb_value) AS median_50km
                    FROM pairs
                    GROUP BY file_id, year, month, day_of_month
                ),
                mad AS (
                    SELECT
                        p.file_id, p.year, p.month, p.day_of_month,
                        median(abs(p.nb_value - a.median_20km))
                            FILTER (WHERE p.dist_km <= {RADIUS_SMALL_KM}) AS mad_20km,
                        median(abs(p.nb_value - a.median_50km)) AS mad_50km
                    FROM pairs p
                    JOIN agg a USING (file_id, year, month, day_of_month)
                    GROUP BY p.file_id, p.year, p.month, p.day_of_month
                )
                SELECT
                    t.file_id,
                    CAST(t.year AS BIGINT) AS matched_year,
                    CAST(t.month AS TINYINT) AS month,
                    CAST(t.day_of_month AS TINYINT) AS day_of_month,
                    t.value AS consensus_value,
                    CAST(COALESCE(a.n_20km, 0) AS BIGINT) AS n_20km,
                    a.median_20km,
                    m.mad_20km,
                    CAST(COALESCE(a.n_50km, 0) AS BIGINT) AS n_50km,
                    a.median_50km,
                    m.mad_50km,
                    CAST({metadata_session_id} AS BIGINT) AS metadata_session_id,
                    CAST({qc_session_id} AS BIGINT) AS qc_session_id,
                    '{now}' AS created_at
                FROM targets t
                LEFT JOIN agg a USING (file_id, year, month, day_of_month)
                LEFT JOIN mad m USING (file_id, year, month, day_of_month)
                ORDER BY t.file_id, t.month, t.day_of_month
            ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        counts = conn.execute(
            f"""
            SELECT COUNT(*),
                   SUM(CASE WHEN n_50km > 0 THEN 1 ELSE 0 END)
            FROM read_parquet('{out_path}')
            """
        ).fetchone()
    finally:
        conn.close()

    target_rows = int(counts[0]) if counts and counts[0] is not None else 0
    with_neighbours = int(counts[1]) if counts and counts[1] is not None else 0

    return RegionalStatsResult(
        metadata_session_id=metadata_session_id,
        qc_session_id=qc_session_id,
        target_rows_written=target_rows,
        targets_with_neighbours=with_neighbours,
        output_path=out_path,
    )


def merge_regional_stats_shards_parquet(
    *,
    regional_root: Path,
    shard_paths: "list[Path]",
    num_shards: Optional[int] = None,
) -> RegionalStatsResult:
    """Consolidate per-shard regional-stats Parquet files into one dataset.

    Each shard produces a disjoint set of target station-day rows (partitioned
    by target ``file_id`` range), so merging is a streaming concatenation -- no
    cross-shard aggregation is needed. The combined rows are written to
    ``regional_root/regional_daily_stats/session_metaNNNNNN_qcNNNNNN.parquet``.
    """
    if not shard_paths:
        raise ValueError("No shard files to merge")
    if num_shards is not None and len(shard_paths) != num_shards:
        raise ValueError(
            f"Expected {num_shards} shard files but got {len(shard_paths)}"
        )

    shard_list_sql = "[" + ", ".join(
        f"'{Path(p).resolve()}'" for p in shard_paths
    ) + "]"

    conn = _connect()
    try:
        sess = conn.execute(
            f"SELECT MAX(metadata_session_id), MAX(qc_session_id) "
            f"FROM read_parquet({shard_list_sql})"
        ).fetchone()
        if sess is None or sess[0] is None or sess[1] is None:
            raise ValueError("Shard files contain no rows; nothing to merge")
        metadata_session_id = int(sess[0])
        qc_session_id = int(sess[1])

        out_dir = regional_root / "regional_daily_stats"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = (
            out_dir
            / f"session_meta{metadata_session_id:06d}_qc{qc_session_id:06d}.parquet"
        ).resolve()

        conn.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet({shard_list_sql})
            ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        counts = conn.execute(
            f"""
            SELECT COUNT(*),
                   SUM(CASE WHEN n_50km > 0 THEN 1 ELSE 0 END)
            FROM read_parquet('{out_path}')
            """
        ).fetchone()
    finally:
        conn.close()

    target_rows = int(counts[0]) if counts and counts[0] is not None else 0
    with_neighbours = int(counts[1]) if counts and counts[1] is not None else 0

    return RegionalStatsResult(
        metadata_session_id=metadata_session_id,
        qc_session_id=qc_session_id,
        target_rows_written=target_rows,
        targets_with_neighbours=with_neighbours,
        output_path=out_path,
    )
