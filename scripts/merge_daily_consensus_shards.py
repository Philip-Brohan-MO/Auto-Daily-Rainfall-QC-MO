"""Merge per-shard daily-consensus Parquet files into one dataset.

Reads ``consensus_shard_*.parquet`` from the shard directory and writes the
combined ``daily_consensus`` table under the consensus root. The regional-stats
shards read that table instead of recomputing the daily median.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.parquet_regional_stats import (
    default_daily_consensus_parquet_root,
    default_daily_consensus_shard_dir,
    merge_daily_consensus_shards_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge daily-consensus shards into one Parquet dataset"
    )
    parser.add_argument("--consensus-root", type=Path, default=None)
    parser.add_argument("--shard-dir", type=Path, default=None)
    parser.add_argument(
        "--expected-shards",
        type=int,
        default=None,
        help="If set, fail unless exactly this many shard files are present",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    consensus_root = args.consensus_root or default_daily_consensus_parquet_root()
    shard_dir = args.shard_dir or default_daily_consensus_shard_dir()

    shard_paths = sorted(Path(shard_dir).glob("consensus_shard_*.parquet"))
    if not shard_paths:
        raise SystemExit(f"No consensus_shard_*.parquet files found in {shard_dir}")
    if args.expected_shards is not None and len(shard_paths) != args.expected_shards:
        raise SystemExit(
            f"Expected {args.expected_shards} shards but found {len(shard_paths)} "
            f"in {shard_dir}"
        )

    print(f"Merging {len(shard_paths)} consensus shards from {shard_dir}")

    result = merge_daily_consensus_shards_parquet(
        consensus_root=consensus_root,
        shard_paths=shard_paths,
        num_shards=args.expected_shards,
    )

    print(f"Daily-consensus dataset written: {result.rows_written} rows")
    print(f"Published -> {result.output_path}")


if __name__ == "__main__":
    main()
