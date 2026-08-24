"""Transcription-source quality control on the ensemble Parquet dataset.

A *source* is one file == one year's sheet == 12 months x 31 = 372 day-cells,
each cell being the consensus (median over the five ensemble members) of that
day's rainfall. Two independent, purely *content-derived* signals are computed
for every source:

1. **Bad transcription sources.** The number of the 372 day-cells that hold a
   real rainfall reading -- ``nonzero_days`` = cells whose consensus is both
   *non-null* (some member recorded a value) and *non-zero* (> 0, i.e. actual
   rainfall rather than a dry ``0.0`` or a blank). A well-transcribed sheet has
   many such days; a mostly-blank or mostly-dry sheet has very few. The cut-off
   is chosen by inspecting the distribution of ``nonzero_days``, so the pipeline
   always stores the raw count and only *flags* ``bad_source`` when it falls
   below a caller-supplied ``min_nonzero_days``.

2. **Duplicate sources.** Most sheets are transcribed more than once, producing
   several files that hold the *same* data apart from transcription errors.
   Duplicates are detected from content alone -- never from the file name,
   descriptor, section id or year range -- by comparing the full **372 daily
   consensus values** of two sources. Candidate pairs are generated cheaply with
   LSH-style banding over rounded monthly sums (robust: a few day-errors barely
   move a month's total), then *confirmed at day level*: a pair is a duplicate
   when at least ``min_agreement`` of their overlapping days agree within
   ``match_tol`` mm, over at least ``min_overlap_days`` shared days.

The heavy per-file aggregation (day-cell consensus + counts) is sharded by
contiguous ``file_id`` range, exactly like the other Parquet pipelines; the
duplicate self-join runs once in the merge step over the compact per-file
metrics table (which carries each file's 372-value vector as a ``LIST``).
Everything is written as a sessioned Parquet dataset under
``transcription_qc_parquet`` so runs are immutable and downstream code can read
the latest session. ``nonzero_days`` is stored per file, so notebook diagnostics
read the small ``file_quality`` table rather than re-scanning the billion-row
daily table.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_ingest import default_ensemble_parquet_root
from .parquet_similarity import _configure_duckdb

MONTHS = tuple(range(1, 13))
DAYS_PER_MONTH = 31            # fixed 31 day-slots per month on every sheet
DAY_CELLS = len(MONTHS) * DAYS_PER_MONTH  # 372 day-cells per source

# Months are split into contiguous bands for LSH-style candidate generation.
# Two files that share ALL rounded monthly sums in ANY one band become a
# candidate pair, so scattered transcription errors (which spoil at most a few
# months' sums) still leave at least one clean band to match on. Confirmation is
# always done at day level over the full 372-value vectors.
BANDS: Tuple[Tuple[int, ...], ...] = ((1, 2, 3), (4, 5, 6), (7, 8, 9), (10, 11, 12))

# --- Duplicate-detection defaults ---------------------------------------
DEFAULT_ROUND_DECIMALS = 1     # rounding of monthly sums for band keys
DEFAULT_MATCH_TOL = 0.2        # |a-b| <= tol counts a day as agreeing (mm)
DEFAULT_MIN_OVERLAP_DAYS = 60  # need this many days present in both files
DEFAULT_MIN_AGREEMENT = 0.9    # matching / overlap must reach this fraction
DEFAULT_MAX_BLOCK = 2000       # skip degenerate blocks (e.g. all-zero vectors)

# --- Bad-source default --------------------------------------------------
DEFAULT_MIN_NONZERO_DAYS = 20  # flag files with fewer than this many rainfall days


@dataclass(frozen=True)
class TranscriptionQCResult:
    qc_root: Path
    session_id: int
    total_files: int
    bad_sources: int
    min_nonzero_days: int
    duplicate_pairs: int
    duplicate_groups: int
    files_with_duplicates: int
    max_group_size: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _glob_sql(dir_path: Path) -> str:
    return str((dir_path / "*.parquet").resolve())


def _connect() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    _configure_duckdb(conn)
    return conn


def default_transcription_qc_parquet_root() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass qc_root explicitly")
    return Path(pdir) / "transcription_qc_parquet"


def default_transcription_qc_shard_dir() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass shard_dir explicitly")
    return Path(pdir) / "transcription_qc_shards"


def file_id_bounds(total_file_ids: int, num_shards: int, shard_index: int) -> Tuple[int, int]:
    """Inclusive [start, end] file_id slice for one contiguous shard.

    The union of all shards covers ``[0, num_shards * per_shard - 1]``, which is
    a superset of the real ``file_id`` range as long as ``total_file_ids`` is at
    least the maximum ``file_id`` in the dataset.
    """
    if num_shards <= 0:
        raise ValueError("num_shards must be positive")
    if not 0 <= shard_index < num_shards:
        raise ValueError(f"shard_index {shard_index} out of range [0, {num_shards})")
    per_shard = (int(total_file_ids) + num_shards - 1) // num_shards
    start = shard_index * per_shard
    end = start + per_shard - 1
    return start, end


def _next_session_id(qc_root: Path) -> int:
    sessions_dir = qc_root / "qc_sessions"
    if not sessions_dir.exists():
        return 1
    ids: List[int] = []
    for path in sessions_dir.glob("session_*.parquet"):
        try:
            ids.append(int(path.stem.split("_")[1]))
        except (IndexError, ValueError):
            continue
    return max(ids) + 1 if ids else 1


FILE_METRICS_SCHEMA = pa.schema(
    [
        ("file_id", pa.int64()),
        ("file_name", pa.string()),
        ("descriptor", pa.string()),
        ("section_id", pa.string()),
        ("year_start", pa.int64()),
        ("year_end", pa.int64()),
        ("nonnull_days", pa.int64()),
        ("nonzero_days", pa.int64()),
        ("months_present", pa.int32()),
    ]
    + [(f"m{month}", pa.float64()) for month in MONTHS]
    + [("day_values", pa.list_(pa.float64()))]
)


def _file_metrics_query(
    ensemble_dataset_root: Path,
    *,
    start_file_id: Optional[int],
    end_file_id: Optional[int],
) -> str:
    """SQL producing one row per file: bad-source counts + 372-day vector.

    Each source is reduced to a fixed 372-element ``day_values`` vector (index
    ``(month-1)*31 + (day-1)``) whose entries are the per-cell consensus (median
    over the members, ``NULL`` when no member recorded a value). From it we
    derive ``nonzero_days`` (the bad-source metric) and per-month sums ``m1..m12``
    used only to block duplicate candidates. A file x calendar-grid cross join
    guarantees exactly 372 aligned entries even if a day-slot row is absent.
    """
    daily_glob = _glob_sql(ensemble_dataset_root / "ensemble_daily_values")
    files_glob = _glob_sql(ensemble_dataset_root / "ensemble_files")

    def _range(alias: str) -> str:
        clauses: List[str] = []
        if start_file_id is not None:
            clauses.append(f"{alias}file_id >= {int(start_file_id)}")
        if end_file_id is not None:
            clauses.append(f"{alias}file_id <= {int(end_file_id)}")
        return (" WHERE " + " AND ".join(clauses)) if clauses else ""

    # Per-month sum of that month's non-null daily consensus values (band keys).
    month_sums = ",\n            ".join(
        f"list_sum(list_filter(list_slice(day_values, {(m - 1) * DAYS_PER_MONTH + 1}, "
        f"{m * DAYS_PER_MONTH}), y -> y IS NOT NULL)) AS m{m}"
        for m in MONTHS
    )
    months_present = " + ".join(
        f"CASE WHEN m{m} IS NOT NULL THEN 1 ELSE 0 END" for m in MONTHS
    )

    return f"""
        WITH files AS (
            SELECT file_id, file_name, descriptor, section_id,
                   CAST(year_start AS BIGINT) AS year_start,
                   CAST(year_end AS BIGINT) AS year_end
            FROM read_parquet('{files_glob}'){_range("")}
        ),
        grid AS (
            SELECT m.month, d.day_of_month
            FROM (SELECT unnest(range(1, {len(MONTHS) + 1})) AS month) m
            CROSS JOIN (SELECT unnest(range(1, {DAYS_PER_MONTH + 1})) AS day_of_month) d
        ),
        cell AS (
            -- One consensus value per (file, calendar day); NULL if no member
            -- recorded a value. median() ignores NULL members.
            SELECT fg.file_id, fg.month, fg.day_of_month,
                   median(dv.rainfall) AS cval
            FROM (SELECT f.file_id, g.month, g.day_of_month
                  FROM files f CROSS JOIN grid g) fg
            LEFT JOIN read_parquet('{daily_glob}') dv
              ON dv.file_id = fg.file_id
             AND dv.month = fg.month
             AND dv.day_of_month = fg.day_of_month
            GROUP BY fg.file_id, fg.month, fg.day_of_month
        ),
        vec AS (
            SELECT file_id,
                   list(cval ORDER BY month, day_of_month) AS day_values,
                   COUNT(*) FILTER (WHERE cval IS NOT NULL) AS nonnull_days,
                   COUNT(*) FILTER (WHERE cval IS NOT NULL AND cval > 0) AS nonzero_days
            FROM cell
            GROUP BY file_id
        ),
        metrics AS (
            SELECT file_id, day_values, nonnull_days, nonzero_days,
                   {month_sums}
            FROM vec
        )
        SELECT
            f.file_id,
            f.file_name,
            f.descriptor,
            f.section_id,
            f.year_start,
            f.year_end,
            COALESCE(mt.nonnull_days, 0) AS nonnull_days,
            COALESCE(mt.nonzero_days, 0) AS nonzero_days,
            CAST(({months_present}) AS INTEGER) AS months_present,
            {", ".join(f"mt.m{m}" for m in MONTHS)},
            mt.day_values
        FROM files f
        LEFT JOIN metrics mt ON mt.file_id = f.file_id
        ORDER BY f.file_id
    """


def build_file_metrics_parquet(
    *,
    ensemble_dataset_root: Path,
    out_path: Path,
    start_file_id: Optional[int] = None,
    end_file_id: Optional[int] = None,
) -> int:
    """Write per-file QC metrics (missing stats + consensus vector) to Parquet.

    Called once per shard (with a contiguous ``file_id`` range) on SLURM, or
    once over the whole dataset for an in-process run. Returns the row count.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect()
    try:
        query = _file_metrics_query(
            ensemble_dataset_root,
            start_file_id=start_file_id,
            end_file_id=end_file_id,
        )
        conn.execute(
            f"COPY ({query}) TO '{out_path.resolve()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        n_rows = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_path.resolve()}')"
        ).fetchone()[0]
        return int(n_rows)
    finally:
        conn.close()


def _band_key_sql(round_decimals: int) -> str:
    """UNION ALL of (file_id, band_key) rows, one block of rows per band."""
    selects: List[str] = []
    for band_index, band in enumerate(BANDS, start=1):
        parts = " || '|' || ".join(
            f"COALESCE(CAST(round(m{m}, {round_decimals}) AS VARCHAR), 'x')"
            for m in band
        )
        not_null = " OR ".join(f"m{m} IS NOT NULL" for m in band)
        selects.append(
            f"SELECT file_id, 'b{band_index}:' || {parts} AS band_key "
            f"FROM fm WHERE {not_null}"
        )
    return "\n            UNION ALL\n            ".join(selects)


def _duplicate_pairs_sql(
    *,
    round_decimals: int,
    match_tol: float,
    min_overlap_days: int,
    min_agreement: float,
    max_block: int,
) -> str:
    """SQL returning confirmed duplicate pairs (fa < fb) with day-level scores.

    Candidates are blocked on rounded monthly sums (cheap, robust to a few
    day-errors) and then confirmed at *day* level over the full 372-value
    ``day_values`` vectors: a day counts to ``overlap_days`` when both files have
    a value, and to ``matching_days`` when those values also agree within
    ``match_tol`` mm. Assumes a view/table ``fm`` (the per-file metrics,
    including ``day_values``) is in scope.
    """
    zipped = "list_zip(A.day_values, B.day_values)"
    both_present = "x[1] IS NOT NULL AND x[2] IS NOT NULL"
    return f"""
        WITH band_keys AS (
            {_band_key_sql(round_decimals)}
        ),
        sized AS (
            SELECT band_key
            FROM band_keys
            GROUP BY band_key
            HAVING COUNT(*) BETWEEN 2 AND {int(max_block)}
        ),
        cand AS (
            SELECT DISTINCT a.file_id AS fa, b.file_id AS fb
            FROM band_keys a
            JOIN band_keys b
              ON a.band_key = b.band_key AND a.file_id < b.file_id
            JOIN sized s ON s.band_key = a.band_key
        ),
        scored AS (
            SELECT cand.fa, cand.fb,
                   len(list_filter({zipped}, x -> {both_present})) AS overlap_days,
                   len(list_filter({zipped},
                       x -> {both_present} AND abs(x[1] - x[2]) <= {match_tol}))
                       AS matching_days
            FROM cand
            JOIN fm A ON A.file_id = cand.fa
            JOIN fm B ON B.file_id = cand.fb
        )
        SELECT fa AS file_id_a, fb AS file_id_b,
               overlap_days, matching_days,
               matching_days::DOUBLE / overlap_days AS agreement
        FROM scored
        WHERE overlap_days >= {int(min_overlap_days)}
          AND matching_days >= {float(min_agreement)} * overlap_days
        ORDER BY file_id_a, file_id_b
    """


def _connected_components(
    edges: List[Tuple[int, int]]
) -> Dict[int, int]:
    """Union-find over duplicate edges; return file_id -> group_id (min member)."""
    parent: Dict[int, int] = {}

    def find(x: int) -> int:
        parent.setdefault(x, x)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            # Attach the larger root under the smaller so group_id == min member.
            hi, lo = (ra, rb) if ra > rb else (rb, ra)
            parent[hi] = lo

    for a, b in edges:
        union(a, b)
    return {node: find(node) for node in parent}


DUP_PAIR_SCHEMA = pa.schema(
    [
        ("file_id_a", pa.int64()),
        ("file_id_b", pa.int64()),
        ("overlap_days", pa.int64()),
        ("matching_days", pa.int64()),
        ("agreement", pa.float64()),
    ]
)

DUP_GROUP_SCHEMA = pa.schema(
    [
        ("file_id", pa.int64()),
        ("group_id", pa.int64()),
        ("group_size", pa.int64()),
        ("duplicate_rank", pa.int64()),
    ]
)


def _write_table(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_qc_session(
    *,
    file_metrics_glob: str,
    qc_root: Path,
    min_nonzero_days: int = DEFAULT_MIN_NONZERO_DAYS,
    round_decimals: int = DEFAULT_ROUND_DECIMALS,
    match_tol: float = DEFAULT_MATCH_TOL,
    min_overlap_days: int = DEFAULT_MIN_OVERLAP_DAYS,
    min_agreement: float = DEFAULT_MIN_AGREEMENT,
    max_block: int = DEFAULT_MAX_BLOCK,
) -> TranscriptionQCResult:
    """Turn a per-file metrics glob into a full QC session on disk.

    Writes ``file_quality/``, ``duplicate_pairs/``, ``duplicate_groups/`` and a
    ``qc_sessions/session_N.parquet`` summary row. Shared by the SLURM merge
    step (metrics = the shard files) and the in-process runner (metrics = a
    single file).
    """
    qc_root.mkdir(parents=True, exist_ok=True)
    session_id = _next_session_id(qc_root)

    conn = _connect()
    try:
        conn.execute(
            f"CREATE TEMP VIEW fm AS SELECT * FROM read_parquet('{file_metrics_glob}')"
        )
        total_files = int(conn.execute("SELECT COUNT(*) FROM fm").fetchone()[0])

        # --- Bad-source flags ------------------------------------------------
        file_quality_path = (
            qc_root / "file_quality" / f"session_{session_id}.parquet"
        )
        file_quality_path.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(
            f"""
            COPY (
                SELECT
                    file_id, file_name, descriptor, section_id,
                    year_start, year_end,
                    nonnull_days, nonzero_days, months_present,
                    (nonzero_days < {int(min_nonzero_days)}) AS bad_source,
                    {int(min_nonzero_days)} AS min_nonzero_days
                FROM fm
                ORDER BY file_id
            ) TO '{file_quality_path.resolve()}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        bad_sources = int(
            conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{file_quality_path.resolve()}') "
                "WHERE bad_source"
            ).fetchone()[0]
        )

        # --- Duplicate pairs (content-only) ----------------------------------
        pairs_df = conn.execute(
            _duplicate_pairs_sql(
                round_decimals=round_decimals,
                match_tol=match_tol,
                min_overlap_days=min_overlap_days,
                min_agreement=min_agreement,
                max_block=max_block,
            )
        ).df()
    finally:
        conn.close()

    pairs_path = qc_root / "duplicate_pairs" / f"session_{session_id}.parquet"
    _write_table(pairs_path, pa.Table.from_pandas(pairs_df, schema=DUP_PAIR_SCHEMA, preserve_index=False))

    # --- Duplicate groups via connected components ---------------------------
    edges = list(zip(pairs_df["file_id_a"].tolist(), pairs_df["file_id_b"].tolist()))
    node_group = _connected_components([(int(a), int(b)) for a, b in edges])

    groups: Dict[int, List[int]] = {}
    for file_id, group_id in node_group.items():
        groups.setdefault(group_id, []).append(file_id)

    group_rows: List[dict] = []
    for group_id, members in groups.items():
        members.sort()
        size = len(members)
        for rank, file_id in enumerate(members, start=1):
            group_rows.append(
                {
                    "file_id": file_id,
                    "group_id": group_id,
                    "group_size": size,
                    "duplicate_rank": rank,
                }
            )
    groups_path = qc_root / "duplicate_groups" / f"session_{session_id}.parquet"
    _write_table(
        groups_path,
        pa.Table.from_pylist(group_rows, schema=DUP_GROUP_SCHEMA),
    )

    files_with_duplicates = len(node_group)
    duplicate_groups = len(groups)
    max_group_size = max((len(m) for m in groups.values()), default=0)

    # --- Session summary -----------------------------------------------------
    session_schema = pa.schema(
        [
            ("session_id", pa.int64()),
            ("created_at", pa.string()),
            ("total_files", pa.int64()),
            ("bad_sources", pa.int64()),
            ("min_nonzero_days", pa.int64()),
            ("duplicate_pairs", pa.int64()),
            ("duplicate_groups", pa.int64()),
            ("files_with_duplicates", pa.int64()),
            ("max_group_size", pa.int64()),
            ("round_decimals", pa.int64()),
            ("match_tol", pa.float64()),
            ("min_overlap_days", pa.int64()),
            ("min_agreement", pa.float64()),
            ("max_block", pa.int64()),
        ]
    )
    session_path = qc_root / "qc_sessions" / f"session_{session_id}.parquet"
    _write_table(
        session_path,
        pa.Table.from_pylist(
            [
                {
                    "session_id": session_id,
                    "created_at": _utc_now(),
                    "total_files": total_files,
                    "bad_sources": bad_sources,
                    "min_nonzero_days": int(min_nonzero_days),
                    "duplicate_pairs": int(len(pairs_df)),
                    "duplicate_groups": duplicate_groups,
                    "files_with_duplicates": files_with_duplicates,
                    "max_group_size": max_group_size,
                    "round_decimals": int(round_decimals),
                    "match_tol": float(match_tol),
                    "min_overlap_days": int(min_overlap_days),
                    "min_agreement": float(min_agreement),
                    "max_block": int(max_block),
                }
            ],
            schema=session_schema,
        ),
    )

    return TranscriptionQCResult(
        qc_root=qc_root,
        session_id=session_id,
        total_files=total_files,
        bad_sources=bad_sources,
        min_nonzero_days=int(min_nonzero_days),
        duplicate_pairs=int(len(pairs_df)),
        duplicate_groups=duplicate_groups,
        files_with_duplicates=files_with_duplicates,
        max_group_size=max_group_size,
    )


def merge_transcription_qc_shards_parquet(
    *,
    shard_dir: Path,
    qc_root: Path,
    expected_shards: Optional[int] = None,
    min_nonzero_days: int = DEFAULT_MIN_NONZERO_DAYS,
    round_decimals: int = DEFAULT_ROUND_DECIMALS,
    match_tol: float = DEFAULT_MATCH_TOL,
    min_overlap_days: int = DEFAULT_MIN_OVERLAP_DAYS,
    min_agreement: float = DEFAULT_MIN_AGREEMENT,
    max_block: int = DEFAULT_MAX_BLOCK,
) -> TranscriptionQCResult:
    """Merge per-file metric shards into a QC session (SLURM merge step)."""
    shard_paths = sorted(Path(shard_dir).glob("tqc_shard_*.parquet"))
    if not shard_paths:
        raise SystemExit(f"No tqc_shard_*.parquet files found in {shard_dir}")
    if expected_shards is not None and len(shard_paths) != expected_shards:
        raise SystemExit(
            f"Expected {expected_shards} shard files, found {len(shard_paths)} in {shard_dir}"
        )
    return write_qc_session(
        file_metrics_glob=str((Path(shard_dir) / "tqc_shard_*.parquet").resolve()),
        qc_root=qc_root,
        min_nonzero_days=min_nonzero_days,
        round_decimals=round_decimals,
        match_tol=match_tol,
        min_overlap_days=min_overlap_days,
        min_agreement=min_agreement,
        max_block=max_block,
    )


def run_transcription_qc(
    *,
    ensemble_dataset_root: Optional[Path] = None,
    qc_root: Optional[Path] = None,
    start_file_id: Optional[int] = None,
    end_file_id: Optional[int] = None,
    min_nonzero_days: int = DEFAULT_MIN_NONZERO_DAYS,
    round_decimals: int = DEFAULT_ROUND_DECIMALS,
    match_tol: float = DEFAULT_MATCH_TOL,
    min_overlap_days: int = DEFAULT_MIN_OVERLAP_DAYS,
    min_agreement: float = DEFAULT_MIN_AGREEMENT,
    max_block: int = DEFAULT_MAX_BLOCK,
) -> TranscriptionQCResult:
    """Run the whole QC step in one process (for the notebook / smoke set).

    Builds the per-file metrics over an optional contiguous ``file_id`` range,
    then writes a QC session. Pass ``start_file_id``/``end_file_id`` to keep the
    holistic per-cell aggregation bounded on a small machine; for the full
    operational dataset use the sharded SLURM pipeline instead
    (submit_transcription_qc.sh).
    """
    ensemble_dataset_root = ensemble_dataset_root or default_ensemble_parquet_root()
    qc_root = qc_root or default_transcription_qc_parquet_root()
    qc_root.mkdir(parents=True, exist_ok=True)

    metrics_path = qc_root / "_work" / "file_metrics.parquet"
    build_file_metrics_parquet(
        ensemble_dataset_root=ensemble_dataset_root,
        out_path=metrics_path,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
    )
    return write_qc_session(
        file_metrics_glob=str(metrics_path.resolve()),
        qc_root=qc_root,
        min_nonzero_days=min_nonzero_days,
        round_decimals=round_decimals,
        match_tol=match_tol,
        min_overlap_days=min_overlap_days,
        min_agreement=min_agreement,
        max_block=max_block,
    )
