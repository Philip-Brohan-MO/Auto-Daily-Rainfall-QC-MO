"""Merge per-shard ensemble ingest databases into one ensemble database.

Reads every ``ens_shard_*.sqlite`` in the shard directory and combines them into
a single ``ensemble_transcriptions.sqlite``, offsetting each shard's ``file_id``
values so the child-table foreign keys stay consistent.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.rainfall_rescue_sqlite.ensemble_ingest import (
    default_ensemble_db_path,
    default_ensemble_root,
    merge_ensemble_shards,
)
from src.rainfall_rescue_sqlite.sqlite_staging import local_scratch_dir, publish_db


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass the path explicitly")
    return Path(pdir).joinpath(*parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge ensemble ingest shards")
    parser.add_argument("--ensemble-db-path", type=Path, default=None)
    parser.add_argument("--shard-dir", type=Path, default=None)
    parser.add_argument(
        "--ensemble-root",
        type=Path,
        default=None,
        help="Recorded as the source root in the merged ingestion-run row",
    )
    parser.add_argument(
        "--expected-shards",
        type=int,
        default=None,
        help="If set, fail unless exactly this many shard files are present",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensemble_db_path = args.ensemble_db_path or default_ensemble_db_path()
    shard_dir = args.shard_dir or _pdir_path("ensemble_shards")
    ensemble_root = args.ensemble_root or default_ensemble_root()

    shard_paths = sorted(Path(shard_dir).glob("ens_shard_*.sqlite"))
    if not shard_paths:
        raise SystemExit(f"No ens_shard_*.sqlite files found in {shard_dir}")
    if args.expected_shards is not None and len(shard_paths) != args.expected_shards:
        raise SystemExit(
            f"Expected {args.expected_shards} shards but found {len(shard_paths)} "
            f"in {shard_dir}"
        )

    # Build the merged DB on node-local scratch, then publish it to shared disc.
    local_db = local_scratch_dir() / f"merge_{os.getpid()}_ensemble.sqlite"

    result = merge_ensemble_shards(local_db, shard_paths, ensemble_root)
    publish_db(local_db, ensemble_db_path)
    print(f"Merged {len(shard_paths)} shards")
    print(result)
    print(f"Published -> {ensemble_db_path}")


if __name__ == "__main__":
    main()
