"""Merge per-shard QC result files into a single QC session.

SQLite backend merges ``qc_shard_*.sqlite`` into one ensemble SQLite QC session.
DuckDB backend merges ``qc_shard_*.parquet`` into Parquet QC session outputs.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.parquet_qc_exact_monthly import (
    default_qc_shard_dir,
    default_roots as default_qc_parquet_roots,
    merge_exact_monthly_qc_shards_parquet,
)
from src.rainfall_rescue_sqlite.qc_exact_monthly import merge_exact_monthly_qc_shards


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass paths explicitly")
    return Path(pdir).joinpath(*parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge QC shards into ensemble DB")
    parser.add_argument(
        "--backend",
        choices=("duckdb", "sqlite"),
        default="duckdb",
        help="Storage backend for QC artifacts",
    )
    parser.add_argument("--ensemble-db-path", type=Path, default=None)
    parser.add_argument("--qc-root", type=Path, default=None)
    parser.add_argument("--similarity-session-id", type=int, default=None)
    parser.add_argument(
        "--shard-dir",
        type=Path,
        default=None,
        help="Directory containing shard files",
    )
    parser.add_argument("--tolerance", type=float, default=0.01)
    parser.add_argument(
        "--expected-shards",
        type=int,
        default=None,
        help="If set, fail unless exactly this many shard files are present",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.backend == "sqlite":
        ensemble_db_path = args.ensemble_db_path or _pdir_path("ensemble_transcriptions.sqlite")
        shard_dir = args.shard_dir or _pdir_path("qc_shards")

        shard_paths = sorted(Path(shard_dir).glob("qc_shard_*.sqlite"))
        if not shard_paths:
            raise SystemExit(f"No qc_shard_*.sqlite files found in {shard_dir}")
        if args.expected_shards is not None and len(shard_paths) != args.expected_shards:
            raise SystemExit(
                f"Expected {args.expected_shards} shards but found {len(shard_paths)} "
                f"in {shard_dir}"
            )

        print(
            f"Merging {len(shard_paths)} shards from {shard_dir} "
            f"into {ensemble_db_path}"
        )

        result = merge_exact_monthly_qc_shards(
            ensemble_db_path=ensemble_db_path,
            shard_paths=shard_paths,
            tolerance=args.tolerance,
            num_shards=args.expected_shards,
        )
        print(
            f"QC session {result.qc_session_id} written: "
            f"{result.day_rows_written} rows ({result.pass_rows} pass, {result.fail_rows} fail)"
        )
        print(f"Published -> {ensemble_db_path}")
        return

    _ensemble_root, _comparison_root, _rr_root, default_qc_root = default_qc_parquet_roots()
    qc_root = args.qc_root or default_qc_root
    shard_dir = args.shard_dir or default_qc_shard_dir()

    shard_paths = sorted(Path(shard_dir).glob("qc_shard_*.parquet"))
    if not shard_paths:
        raise SystemExit(f"No qc_shard_*.parquet files found in {shard_dir}")
    if args.expected_shards is not None and len(shard_paths) != args.expected_shards:
        raise SystemExit(
            f"Expected {args.expected_shards} shards but found {len(shard_paths)} "
            f"in {shard_dir}"
        )

    print(f"Merging {len(shard_paths)} shards from {shard_dir} into {qc_root}")

    result = merge_exact_monthly_qc_shards_parquet(
        qc_root=qc_root,
        shard_paths=shard_paths,
        tolerance=args.tolerance,
        similarity_session_id=args.similarity_session_id,
        num_shards=args.expected_shards,
    )

    print(
        f"QC session {result.qc_session_id} written: "
        f"{result.day_rows_written} rows ({result.pass_rows} pass, {result.fail_rows} fail)"
    )
    print(f"Published -> {qc_root}")


if __name__ == "__main__":
    main()
