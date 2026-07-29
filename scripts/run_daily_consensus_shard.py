"""Precompute the daily consensus (member median) for one file_id shard.

Each array task computes ``median(rainfall)`` per (file_id, month, day_of_month)
for a *contiguous* slice of file_ids and writes
``<shard-dir>/consensus_shard_<index>.parquet``. Contiguous ranges let DuckDB
prune ``ensemble_daily_values`` row groups so the holistic median only buffers
the slice's rows -- unlike the regional stage, which would otherwise recompute
this median over a nationally-scattered neighbour pool and run out of memory.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.parquet_regional_stats import (
    compute_daily_consensus_shard_parquet,
    default_daily_consensus_shard_dir,
    default_roots as default_regional_roots,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Precompute one daily-consensus shard")
    parser.add_argument("--ensemble-dataset-root", type=Path, default=None)
    parser.add_argument("--shard-dir", type=Path, default=None)
    parser.add_argument("--num-shards", type=int, required=True)
    parser.add_argument(
        "--shard-index",
        type=int,
        default=None,
        help="Shard index; defaults to $SLURM_ARRAY_TASK_ID",
    )
    parser.add_argument("--total-file-ids", type=int, required=True)
    return parser.parse_args()


def _resolve_shard_index(explicit: int | None) -> int:
    if explicit is not None:
        return explicit
    env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if env is None:
        raise SystemExit(
            "No --shard-index and SLURM_ARRAY_TASK_ID not set; cannot pick a shard"
        )
    return int(env)


def main() -> None:
    args = parse_args()

    default_ensemble_root, *_ = default_regional_roots()
    ensemble_dataset_root = args.ensemble_dataset_root or default_ensemble_root
    shard_dir = args.shard_dir or default_daily_consensus_shard_dir()
    shard_index = _resolve_shard_index(args.shard_index)
    num_shards = args.num_shards
    total_file_ids = args.total_file_ids

    ids_per_shard = (total_file_ids + num_shards - 1) // num_shards
    start_file_id = shard_index * ids_per_shard + 1
    end_file_id = min(start_file_id + ids_per_shard - 1, total_file_ids)

    print(
        f"Daily-consensus shard {shard_index}/{num_shards}: "
        f"file_ids {start_file_id}-{end_file_id}"
    )

    shard_output_path = Path(shard_dir) / f"consensus_shard_{shard_index:05d}.parquet"
    shard_output_path.parent.mkdir(parents=True, exist_ok=True)

    result = compute_daily_consensus_shard_parquet(
        ensemble_dataset_root=ensemble_dataset_root,
        output_path=shard_output_path,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
    )

    print(f"Shard {shard_index} done: {result.rows_written} consensus rows")
    print(f"Published -> {result.output_path}")


if __name__ == "__main__":
    main()
