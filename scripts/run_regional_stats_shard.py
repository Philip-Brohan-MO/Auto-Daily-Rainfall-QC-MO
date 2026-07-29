"""Run one shard of the regional neighbour-statistics computation (stage 1 of
the second QC check) as a SLURM array task.

Each shard computes regional statistics for its slice of *target* file_ids and
writes them to ``<shard-dir>/regional_shard_<index>.parquet``. The neighbour
pool for those targets (passing station-days within the search radius) is scoped
automatically inside :func:`compute_regional_daily_stats_parquet`.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.parquet_regional_stats import (
    compute_regional_daily_stats_parquet,
    default_regional_stats_shard_dir,
    default_roots as default_regional_roots,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one regional-stats shard")
    parser.add_argument("--ensemble-dataset-root", type=Path, default=None)
    parser.add_argument("--comparison-root", type=Path, default=None)
    parser.add_argument("--qc-root", type=Path, default=None)
    parser.add_argument(
        "--consensus-root",
        type=Path,
        default=None,
        help="Root of the precomputed daily_consensus dataset (read instead of "
        "recomputing the daily median; required at scale to bound memory)",
    )
    parser.add_argument(
        "--shard-dir",
        type=Path,
        default=None,
        help="Directory for per-shard outputs",
    )
    parser.add_argument("--num-shards", type=int, required=True)
    parser.add_argument(
        "--shard-index",
        type=int,
        default=None,
        help="Shard index; defaults to $SLURM_ARRAY_TASK_ID",
    )
    parser.add_argument(
        "--total-file-ids",
        type=int,
        required=True,
        help="Total number of file_ids (used to compute the target start/end slice)",
    )
    parser.add_argument("--metadata-session-id", type=int, default=None)
    parser.add_argument("--qc-session-id", type=int, default=None)
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

    default_ensemble_root, default_comparison_root, default_qc_root, _ = (
        default_regional_roots()
    )
    ensemble_dataset_root = args.ensemble_dataset_root or default_ensemble_root
    comparison_root = args.comparison_root or default_comparison_root
    qc_root = args.qc_root or default_qc_root

    shard_dir = args.shard_dir or default_regional_stats_shard_dir()
    shard_index = _resolve_shard_index(args.shard_index)
    num_shards = args.num_shards
    total_file_ids = args.total_file_ids

    # Partition target file_ids evenly across shards (1-based).
    ids_per_shard = (total_file_ids + num_shards - 1) // num_shards
    start_file_id = shard_index * ids_per_shard + 1
    end_file_id = min(start_file_id + ids_per_shard - 1, total_file_ids)

    print(
        f"Regional-stats shard {shard_index}/{num_shards}: "
        f"target file_ids {start_file_id}-{end_file_id}"
    )

    shard_output_path = Path(shard_dir) / f"regional_shard_{shard_index:05d}.parquet"
    shard_output_path.parent.mkdir(parents=True, exist_ok=True)

    result = compute_regional_daily_stats_parquet(
        ensemble_dataset_root=ensemble_dataset_root,
        comparison_root=comparison_root,
        qc_root=qc_root,
        output_path=shard_output_path,
        consensus_root=args.consensus_root,
        metadata_session_id=args.metadata_session_id,
        qc_session_id=args.qc_session_id,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
    )

    print(
        f"Shard {shard_index} done: {result.target_rows_written} target rows "
        f"({result.targets_with_neighbours} with neighbours)"
    )
    print(f"Published -> {result.output_path}")


if __name__ == "__main__":
    main()
