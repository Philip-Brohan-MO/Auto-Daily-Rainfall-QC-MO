"""Merge per-shard regional-stats Parquet files into one consolidated dataset.

Reads ``regional_shard_*.parquet`` from the shard directory and writes the
combined ``regional_daily_stats`` table under the regional-stats root.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.parquet_regional_stats import (
    default_regional_stats_shard_dir,
    default_roots as default_regional_roots,
    merge_regional_stats_shards_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge regional-stats shards into one Parquet dataset"
    )
    parser.add_argument("--regional-root", type=Path, default=None)
    parser.add_argument(
        "--shard-dir",
        type=Path,
        default=None,
        help="Directory containing regional_shard_*.parquet files",
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

    *_, default_regional_root = default_regional_roots()
    regional_root = args.regional_root or default_regional_root
    shard_dir = args.shard_dir or default_regional_stats_shard_dir()

    shard_paths = sorted(Path(shard_dir).glob("regional_shard_*.parquet"))
    if not shard_paths:
        raise SystemExit(f"No regional_shard_*.parquet files found in {shard_dir}")
    if args.expected_shards is not None and len(shard_paths) != args.expected_shards:
        raise SystemExit(
            f"Expected {args.expected_shards} shards but found {len(shard_paths)} "
            f"in {shard_dir}"
        )

    print(f"Merging {len(shard_paths)} shards from {shard_dir} into {regional_root}")

    result = merge_regional_stats_shards_parquet(
        regional_root=regional_root,
        shard_paths=shard_paths,
        num_shards=args.expected_shards,
    )

    print(
        f"Regional-stats dataset written: {result.target_rows_written} rows "
        f"({result.targets_with_neighbours} with neighbours), "
        f"meta session {result.metadata_session_id}, qc session {result.qc_session_id}"
    )
    print(f"Published -> {result.output_path}")


if __name__ == "__main__":
    main()
