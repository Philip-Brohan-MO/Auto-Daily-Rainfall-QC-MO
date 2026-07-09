"""Build the comparison vectors DB once, ready for sharded SLURM matching."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.rainfall_rescue_sqlite.comparison_baseline import build_comparison_vectors
from src.rainfall_rescue_sqlite.sqlite_staging import local_scratch_dir, publish_db


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass the path explicitly")
    return Path(pdir).joinpath(*parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build comparison vectors DB")
    parser.add_argument("--rr-db-path", type=Path, default=None)
    parser.add_argument("--ensemble-db-path", type=Path, default=None)
    parser.add_argument("--comparison-db-path", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rr_db_path = args.rr_db_path or _pdir_path("Rainfall-Rescue", "rainfall_rescue.sqlite")
    ensemble_db_path = args.ensemble_db_path or _pdir_path("ensemble_transcriptions.sqlite")
    comparison_db_path = args.comparison_db_path or _pdir_path("monthly_similarity.sqlite")

    # Write on node-local scratch (shared FS can't do SQLite write locking),
    # then publish the finished DB to its shared-disc location.
    local_db = local_scratch_dir() / f"build_{os.getpid()}_monthly_similarity.sqlite"

    result = build_comparison_vectors(
        rr_db_path=rr_db_path,
        ensemble_db_path=ensemble_db_path,
        comparison_db_path=local_db,
    )
    publish_db(local_db, comparison_db_path)
    print(result)
    print(f"Published -> {comparison_db_path}")


if __name__ == "__main__":
    main()
