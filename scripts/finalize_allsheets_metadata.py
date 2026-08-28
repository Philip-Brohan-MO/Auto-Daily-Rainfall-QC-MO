"""Finalise the ALLSHEETS residual match: assign metadata, then combine.

Runs after the ALLSHEETS match array + merge have written a similarity session
into the ALLSHEETS comparison root. Two steps:

  1. Assign ALLSHEETS metadata for the residual ensemble files
     (``assign_ensemble_metadata_parquet`` against the ALLSHEETS root). ALLSHEETS
     sheets carry no coordinates, so this yields name + year only.
  2. Combine the DATA and ALLSHEETS assignments into one final
     ``ensemble_metadata`` session under the DATA comparison root
     (``combine_metadata_assignments_parquet``): DATA exact matches are kept,
     residual records are filled from ALLSHEETS exact matches (tagged
     ``exact_allsheets``), and the combined session becomes the latest one
     downstream code reads.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from src.rainfall_rescue_sqlite.assign_ensemble_metadata import (
    assign_ensemble_metadata_parquet,
    combine_metadata_assignments_parquet,
)
from src.rainfall_rescue_sqlite.parquet_ingest import (
    default_allsheets_parquet_root,
    default_ensemble_parquet_root,
)
from src.rainfall_rescue_sqlite.parquet_similarity import (
    default_allsheets_comparison_parquet_root,
    default_comparison_parquet_root,
)
from src.rainfall_rescue_sqlite.run_manifest import publish_match_metadata_run_manifest


def _latest_metadata_session(comparison_root: Path) -> Path:
    """Return the highest-numbered ``ensemble_metadata/session_*.parquet``."""
    meta_dir = comparison_root / "ensemble_metadata"
    sessions = list(meta_dir.glob("session_*.parquet"))
    if not sessions:
        raise SystemExit(
            f"No ensemble_metadata/session_*.parquet found under {comparison_root}.\n"
            "Run the DATA metadata assignment first (assign_ensemble_metadata_parquet)."
        )
    return max(sessions, key=lambda p: int(p.stem.split("_")[1]))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assign ALLSHEETS metadata and combine with DATA metadata"
    )
    parser.add_argument("--allsheets-comparison-root", type=Path, default=None)
    parser.add_argument("--source-comparison-root", type=Path, default=None)
    parser.add_argument("--ensemble-dataset-root", type=Path, default=None)
    parser.add_argument("--allsheets-dataset-root", type=Path, default=None)
    parser.add_argument(
        "--data-metadata-path",
        type=Path,
        default=None,
        help="DATA ensemble_metadata parquet (default: latest session in "
        "--source-comparison-root)",
    )
    parser.add_argument("--allsheets-match-type", default="exact_allsheets")
    parser.add_argument(
        "--leftover-sites-csv",
        type=Path,
        default=None,
        help="LeftOverSites.csv used to fill coordinates for ALLSHEETS name "
        "matches (default: LeftOverSites.csv under the Rainfall Rescue root)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    allsheets_comparison_root = (
        args.allsheets_comparison_root or default_allsheets_comparison_parquet_root()
    )
    source_comparison_root = args.source_comparison_root or default_comparison_parquet_root()
    ensemble_dataset_root = args.ensemble_dataset_root or default_ensemble_parquet_root()
    allsheets_dataset_root = args.allsheets_dataset_root or default_allsheets_parquet_root()
    data_metadata_path = args.data_metadata_path or _latest_metadata_session(
        source_comparison_root
    )

    print("Step 1: assign ALLSHEETS metadata for residual records")
    allsheets_result = assign_ensemble_metadata_parquet(
        comparison_root=allsheets_comparison_root,
        ensemble_dataset_root=ensemble_dataset_root,
        rr_dataset_root=allsheets_dataset_root,
        session_id=None,
    )
    print(f"  ALLSHEETS exact matches : {allsheets_result.exact_matches:,}")
    print(f"  Metadata written        : {allsheets_result.output_path}")

    print("Step 2: combine DATA + ALLSHEETS into the final metadata session")
    combined = combine_metadata_assignments_parquet(
        data_metadata_path=data_metadata_path,
        allsheets_metadata_path=allsheets_result.output_path,
        comparison_root=source_comparison_root,
        allsheets_match_type=args.allsheets_match_type,
        leftover_sites_csv=args.leftover_sites_csv,
    )
    print(f"  Combined session {combined.session_id} -> {combined.output_path}")
    print(f"    total ensemble files : {combined.total_ensemble_files:,}")
    print(f"    DATA exact           : {combined.data_exact:,}")
    print(f"    ALLSHEETS filled     : {combined.allsheets_filled:,}")
    print(f"    ALLSHEETS w/ coords  : {combined.allsheets_with_coords:,}")
    print(f"    DATA approximate     : {combined.data_approximate:,}")
    print(f"    unmatched            : {combined.unmatched:,}")

    manifest_path = publish_match_metadata_run_manifest(
        comparison_root=source_comparison_root,
        session_id=combined.session_id,
        ensemble_metadata_path=combined.output_path,
        data_metadata_input_path=data_metadata_path,
        allsheets_metadata_input_path=allsheets_result.output_path,
    )
    print(f"Step 3: published run manifest -> {manifest_path}")


if __name__ == "__main__":
    main()
