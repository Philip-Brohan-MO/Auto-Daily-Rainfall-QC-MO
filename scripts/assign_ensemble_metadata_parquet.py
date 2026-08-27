"""Assign Rainfall Rescue metadata to ensemble files and write a parquet session.

This is the DATA-pass parquet equivalent of the older SQLite metadata assignment.
It reads the latest (or requested) similarity session from COMPARISON_PARQUET_ROOT
and writes ensemble_metadata/session_XXXXXX.parquet under OUTPUT_ROOT.
"""

from __future__ import annotations

import argparse
import glob
from pathlib import Path
from typing import Iterable, Optional

from src.rainfall_rescue_sqlite.assign_ensemble_metadata import assign_ensemble_metadata_parquet
from src.rainfall_rescue_sqlite.parquet_ingest import (
    default_ensemble_parquet_root,
    default_rainfall_rescue_parquet_root,
)
from src.rainfall_rescue_sqlite.parquet_similarity import default_comparison_parquet_root


def _has_any(pattern: str) -> bool:
    return any(True for _ in glob.iglob(pattern))


def _require_glob(pattern: str, hint: str) -> None:
    if not _has_any(pattern):
        raise SystemExit(f"Missing required input files: {pattern}\nHINT: {hint}")


def _require_parquet_inputs(
    *,
    comparison_root: Path,
    ensemble_dataset_root: Path,
    rr_dataset_root: Path,
) -> None:
    _require_glob(
        str(comparison_root / "similarity_sessions" / "*.parquet"),
        "Run similarity matching first (build_vectors -> match_array -> merge_shards).",
    )
    _require_glob(
        str(comparison_root / "similarity_matches" / "*.parquet"),
        "Run similarity matching first (merge_shards writes similarity_matches).",
    )
    _require_glob(
        str(comparison_root / "rr_monthly_vectors" / "*.parquet"),
        "Run vector build first (build_vectors writes rr_monthly_vectors).",
    )
    _require_glob(
        str(comparison_root / "ensemble_consensus_vectors" / "*.parquet"),
        "Run vector build first (build_vectors writes ensemble_consensus_vectors).",
    )
    _require_glob(
        str(ensemble_dataset_root / "ensemble_files" / "*.parquet"),
        "Build or point to a valid ensemble parquet dataset root.",
    )
    _require_glob(
        str(rr_dataset_root / "stations" / "*.parquet"),
        "Build or point to a valid Rainfall Rescue parquet dataset root.",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assign RR metadata to ensemble records using parquet similarity outputs"
    )
    parser.add_argument("--comparison-root", type=Path, default=None)
    parser.add_argument("--ensemble-dataset-root", type=Path, default=None)
    parser.add_argument("--rr-dataset-root", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument(
        "--session-id",
        type=int,
        default=None,
        help="Similarity session to assign (default: latest)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    comparison_root = args.comparison_root or default_comparison_parquet_root()
    ensemble_dataset_root = args.ensemble_dataset_root or default_ensemble_parquet_root()
    rr_dataset_root = args.rr_dataset_root or default_rainfall_rescue_parquet_root()
    output_root = args.output_root or comparison_root

    _require_parquet_inputs(
        comparison_root=comparison_root,
        ensemble_dataset_root=ensemble_dataset_root,
        rr_dataset_root=rr_dataset_root,
    )

    print("Assigning DATA metadata parquet session")
    print(f"  Comparison root      : {comparison_root}")
    print(f"  Ensemble dataset root: {ensemble_dataset_root}")
    print(f"  RR dataset root      : {rr_dataset_root}")
    print(f"  Output root          : {output_root}")
    print(f"  Session              : {args.session_id or 'latest'}")

    result = assign_ensemble_metadata_parquet(
        comparison_root=comparison_root,
        ensemble_dataset_root=ensemble_dataset_root,
        rr_dataset_root=rr_dataset_root,
        output_root=output_root,
        session_id=args.session_id,
    )

    print("Metadata assignment complete")
    print(f"  Session ID         : {result.session_id}")
    print(f"  Output parquet     : {result.output_path}")
    print(f"  Total files        : {result.total_ensemble_files}")
    print(f"  Exact matches      : {result.exact_matches}")
    print(f"  Approximate matches: {result.approximate_matches}")
    print(f"  Unmatched          : {result.unmatched}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
