"""Build consensus vectors and run baseline monthly-profile similarity search."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.rainfall_rescue_sqlite.comparison_baseline import (
    build_comparison_vectors,
    run_baseline_matching,
)


def _default_rr_db_path() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass --rr-db-path explicitly")
    return Path(pdir) / "Rainfall-Rescue" / "rainfall_rescue.sqlite"


def _default_ensemble_db_path() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass --ensemble-db-path explicitly")
    return Path(pdir) / "ensemble_transcriptions.sqlite"


def _default_comparison_db_path() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass --comparison-db-path explicitly")
    return Path(pdir) / "monthly_similarity.sqlite"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build and query baseline monthly-profile similarity index"
    )
    parser.add_argument(
        "--rr-db-path",
        type=Path,
        default=None,
        help="Path to rainfall_rescue.sqlite",
    )
    parser.add_argument(
        "--ensemble-db-path",
        type=Path,
        default=None,
        help="Path to ensemble_transcriptions.sqlite",
    )
    parser.add_argument(
        "--comparison-db-path",
        type=Path,
        default=None,
        help="Path to output comparison SQLite",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip rebuilding vectors and only run matching",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Top K RR matches per ensemble query",
    )
    parser.add_argument(
        "--min-overlap",
        type=int,
        default=10,
        help="Minimum overlapping months required for a match",
    )
    parser.add_argument(
        "--uncertainty-weight",
        type=float,
        default=0.15,
        help="Penalty multiplier for ensemble uncertainty in ranking",
    )
    parser.add_argument(
        "--max-ensemble-queries",
        type=int,
        default=None,
        help="Optional query limit for smoke tests",
    )
    parser.add_argument(
        "--max-rr-candidates",
        type=int,
        default=None,
        help="Optional RR candidate limit for smoke tests",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=8192,
        help="RR candidate batch size for NumPy matching",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=0,
        help="Print progress every N ensemble queries (0 disables)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rr_db_path = args.rr_db_path or _default_rr_db_path()
    ensemble_db_path = args.ensemble_db_path or _default_ensemble_db_path()
    comparison_db_path = args.comparison_db_path or _default_comparison_db_path()

    if not args.skip_build:
        build_result = build_comparison_vectors(
            rr_db_path=rr_db_path,
            ensemble_db_path=ensemble_db_path,
            comparison_db_path=comparison_db_path,
        )
        print("Vector build completed")
        print(f"  Comparison DB: {build_result.comparison_db_path}")
        print(f"  RR vectors: {build_result.rr_vectors}")
        print(f"  Ensemble consensus vectors: {build_result.ensemble_vectors}")

    match_result = run_baseline_matching(
        comparison_db_path=comparison_db_path,
        top_k=args.top_k,
        min_overlap=args.min_overlap,
        uncertainty_weight=args.uncertainty_weight,
        max_ensemble_queries=args.max_ensemble_queries,
        max_rr_candidates=args.max_rr_candidates,
        batch_size=args.batch_size,
        progress_interval=args.progress_interval,
    )
    print("Matching completed")
    print(f"  Session ID: {match_result.session_id}")
    print(f"  Ensemble queries: {match_result.ensemble_queries}")
    print(f"  RR candidates: {match_result.rr_candidates}")
    print(f"  Matches written: {match_result.matches_written}")


if __name__ == "__main__":
    main()
