"""CLI for assigning Rainfall Rescue metadata to ensemble records."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from src.rainfall_rescue_sqlite.assign_ensemble_metadata import assign_ensemble_metadata


def default_ensemble_db_path() -> Path:
    """Return default ensemble database path."""
    pdir = os.getenv("PDIR", "/data/scratch/philip.brohan/ADRQ")
    return Path(pdir) / "ensemble_transcriptions.sqlite"


def default_comparison_db_path() -> Path:
    """Return default comparison database path."""
    pdir = os.getenv("PDIR", "/data/scratch/philip.brohan/ADRQ")
    return Path(pdir) / "monthly_similarity.sqlite"


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Assign Rainfall Rescue metadata to ensemble records using similarity matches."
    )
    parser.add_argument(
        "--ensemble-db",
        type=Path,
        default=default_ensemble_db_path(),
        help="Path to ensemble database",
    )
    parser.add_argument(
        "--comparison-db",
        type=Path,
        default=default_comparison_db_path(),
        help="Path to comparison database",
    )
    parser.add_argument(
        "--session-id",
        type=int,
        default=None,
        help="Session ID to use (default: latest)",
    )

    args = parser.parse_args()

    print(f"Assigning metadata...")
    print(f"  Ensemble DB: {args.ensemble_db}")
    print(f"  Comparison DB: {args.comparison_db}")
    print(f"  Session ID: {args.session_id or 'latest'}")
    print()

    result = assign_ensemble_metadata(
        ensemble_db_path=args.ensemble_db,
        comparison_db_path=args.comparison_db,
        session_id=args.session_id,
    )

    print(f"Metadata Assignment Complete")
    print(f"  Session ID: {result.session_id}")
    print(f"  Total ensemble files: {result.total_ensemble_files}")
    print(f"  Exact matches: {result.exact_matches} ({100*result.exact_matches/result.total_ensemble_files:.1f}%)")
    print(f"  Approximate matches: {result.approximate_matches} ({100*result.approximate_matches/result.total_ensemble_files:.1f}%)")
    print(f"  Unmatched: {result.unmatched} ({100*result.unmatched/result.total_ensemble_files:.1f}%)")
    if result.failures > 0:
        print(f"  Failures: {result.failures}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
