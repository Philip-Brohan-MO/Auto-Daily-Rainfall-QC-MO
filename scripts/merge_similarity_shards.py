"""Merge per-shard match files into a single similarity session.

DuckDB/parquet backend: reads ``similarity_shard_*.parquet`` and writes the
combined session into the comparison Parquet root.
SQLite backend (legacy): reads ``shard_*.sqlite`` and copies into the shared
comparison DB.
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

from src.rainfall_rescue_sqlite.comparison_baseline import merge_shard_matches
from src.rainfall_rescue_sqlite.parquet_similarity import (
    default_comparison_parquet_root,
    merge_similarity_shards_parquet,
)
from src.rainfall_rescue_sqlite.sqlite_staging import local_scratch_dir, publish_db


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass the path explicitly")
    return Path(pdir).joinpath(*parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge similarity match shards")
    parser.add_argument(
        "--backend",
        choices=("duckdb", "sqlite"),
        default="duckdb",
        help="Storage backend (default: duckdb/parquet)",
    )
    # DuckDB/parquet paths
    parser.add_argument("--comparison-root", type=Path, default=None)
    parser.add_argument(
        "--shard-dir",
        type=Path,
        default=None,
        help="Directory containing per-shard parquet files (default: $PDIR/similarity_shards_parquet)",
    )
    # SQLite paths (legacy)
    parser.add_argument("--comparison-db-path", type=Path, default=None)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--min-overlap", type=int, default=10)
    parser.add_argument("--uncertainty-weight", type=float, default=0.15)
    parser.add_argument(
        "--expected-shards",
        type=int,
        default=None,
        help="If set, fail unless exactly this many shard files are present",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.backend == "duckdb":
        comparison_root = args.comparison_root or default_comparison_parquet_root()
        shard_dir = args.shard_dir or _pdir_path("similarity_shards_parquet")

        result = merge_similarity_shards_parquet(
            comparison_root=comparison_root,
            shard_dir=Path(shard_dir),
            top_k=args.top_k,
            min_overlap=args.min_overlap,
            uncertainty_weight=args.uncertainty_weight,
            expected_shards=args.expected_shards,
        )
        print(f"Merged {result.shards_merged} shards -> session {result.session_id}")
        print(f"  {result.matches_written} match rows written to {comparison_root}")
        return

    # SQLite (legacy) path.
    comparison_db_path = args.comparison_db_path or _pdir_path("monthly_similarity.sqlite")
    shard_dir = args.shard_dir or _pdir_path("similarity_shards")

    shard_paths = sorted(Path(shard_dir).glob("shard_*.sqlite"))
    if not shard_paths:
        raise SystemExit(f"No shard_*.sqlite files found in {shard_dir}")
    if args.expected_shards is not None and len(shard_paths) != args.expected_shards:
        raise SystemExit(
            f"Expected {args.expected_shards} shards but found {len(shard_paths)} "
            f"in {shard_dir}"
        )

    # Merge on node-local scratch: copy the shared vectors DB down, insert the
    # (immutable-read) shard matches into it there, then publish it back. The
    # shared FS can't do the SQLite write locking this needs.
    local_db = local_scratch_dir() / f"merge_{os.getpid()}_monthly_similarity.sqlite"
    shutil.copy2(comparison_db_path, local_db)

    result = merge_shard_matches(
        comparison_db_path=local_db,
        shard_paths=shard_paths,
        top_k=args.top_k,
        min_overlap=args.min_overlap,
        uncertainty_weight=args.uncertainty_weight,
    )
    publish_db(local_db, comparison_db_path)
    print(f"Merged {len(shard_paths)} shards")
    print(result)
    print(f"Published -> {comparison_db_path}")


if __name__ == "__main__":
    main()
