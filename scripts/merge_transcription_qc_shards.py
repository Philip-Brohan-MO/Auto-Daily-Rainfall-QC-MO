"""Merge transcription-QC shards into a single QC session.

Reads all per-file metric shards (``tqc_shard_*.parquet``) produced by the
array step, flags bad transcription sources (too few rainfall days), detects
duplicate sources from content alone (day-level agreement), and writes the
sessioned Parquet outputs (``file_quality`` / ``duplicate_pairs`` /
``duplicate_groups`` / ``qc_sessions``) under the transcription-QC root.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.parquet_transcription_qc import (
    DEFAULT_MATCH_TOL,
    DEFAULT_MAX_BLOCK,
    DEFAULT_MIN_AGREEMENT,
    DEFAULT_MIN_NONZERO_DAYS,
    DEFAULT_MIN_OVERLAP_DAYS,
    DEFAULT_ROUND_DECIMALS,
    default_transcription_qc_parquet_root,
    default_transcription_qc_shard_dir,
    export_good_ensemble_parquet,
    merge_transcription_qc_shards_parquet,
)
from src.rainfall_rescue_sqlite.parquet_ingest import default_ensemble_parquet_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge transcription-QC shards")
    parser.add_argument("--shard-dir", type=Path, default=None)
    parser.add_argument("--qc-root", type=Path, default=None)
    parser.add_argument("--ensemble-dataset-root", type=Path, required=True)
    parser.add_argument("--good-dataset-root", type=Path, required=True)
    parser.add_argument("--expected-shards", type=int, default=None)
    parser.add_argument(
        "--min-nonzero-days", type=int, default=DEFAULT_MIN_NONZERO_DAYS
    )
    parser.add_argument("--round-decimals", type=int, default=DEFAULT_ROUND_DECIMALS)
    parser.add_argument("--match-tol", type=float, default=DEFAULT_MATCH_TOL)
    parser.add_argument(
        "--min-overlap-days", type=int, default=DEFAULT_MIN_OVERLAP_DAYS
    )
    parser.add_argument("--min-agreement", type=float, default=DEFAULT_MIN_AGREEMENT)
    parser.add_argument("--max-block", type=int, default=DEFAULT_MAX_BLOCK)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    shard_dir = args.shard_dir or default_transcription_qc_shard_dir()
    qc_root = args.qc_root or default_transcription_qc_parquet_root()

    result = merge_transcription_qc_shards_parquet(
        shard_dir=shard_dir,
        qc_root=qc_root,
        expected_shards=args.expected_shards,
        min_nonzero_days=args.min_nonzero_days,
        round_decimals=args.round_decimals,
        match_tol=args.match_tol,
        min_overlap_days=args.min_overlap_days,
        min_agreement=args.min_agreement,
        max_block=args.max_block,
    )
    good_files, files_rows, daily_rows, totals_rows = export_good_ensemble_parquet(
        ensemble_dataset_root=args.ensemble_dataset_root or default_ensemble_parquet_root(),
        qc_root=qc_root,
        session_id=result.session_id,
        output_root=args.good_dataset_root,
    )

    print("Transcription-QC merge complete")
    print(f"  Session:              {result.session_id}")
    print(f"  QC root:              {result.qc_root}")
    print(f"  Total files:          {result.total_files}")
    print(
        f"  Bad sources:          {result.bad_sources} "
        f"(nonzero_days < {result.min_nonzero_days})"
    )
    print(f"  Duplicate pairs:      {result.duplicate_pairs}")
    print(f"  Duplicate groups:     {result.duplicate_groups}")
    print(f"  Files w/ duplicates:  {result.files_with_duplicates}")
    print(f"  Max group size:       {result.max_group_size}")
    print("Good-only dataset exported")
    print(f"  Root:                 {args.good_dataset_root}")
    print(f"  Good sources:         {good_files}")
    print(f"  Files / daily / totals: {files_rows} / {daily_rows} / {totals_rows}")


if __name__ == "__main__":
    main()
