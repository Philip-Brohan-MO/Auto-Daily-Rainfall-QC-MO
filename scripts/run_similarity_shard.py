"""Run one shard of the monthly-similarity matching (a SLURM array task).

Each shard matches its slice of ensemble queries against ALL RR candidates and
writes its top-K matches to ``<shard-dir>/shard_<index>.sqlite``. The comparison
vectors DB is read-only (immutable), so many shards can share it concurrently.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.rainfall_rescue_sqlite.comparison_baseline import run_matching_shard
from src.rainfall_rescue_sqlite.sqlite_staging import local_scratch_dir, publish_db


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass the path explicitly")
    return Path(pdir).joinpath(*parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one similarity matching shard")
    parser.add_argument("--comparison-db-path", type=Path, default=None)
    parser.add_argument(
        "--shard-dir",
        type=Path,
        default=None,
        help="Directory for per-shard output DBs (default: $PDIR/similarity_shards)",
    )
    parser.add_argument("--num-shards", type=int, required=True)
    parser.add_argument(
        "--shard-index",
        type=int,
        default=None,
        help="Shard index; defaults to $SLURM_ARRAY_TASK_ID",
    )
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--min-overlap", type=int, default=10)
    parser.add_argument("--uncertainty-weight", type=float, default=0.15)
    parser.add_argument("--max-rr-candidates", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=8192)
    parser.add_argument("--progress-interval", type=int, default=50)
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
    comparison_db_path = args.comparison_db_path or _pdir_path("monthly_similarity.sqlite")
    shard_dir = args.shard_dir or _pdir_path("similarity_shards")
    shard_index = _resolve_shard_index(args.shard_index)

    shard_output_path = Path(shard_dir) / f"shard_{shard_index:05d}.sqlite"

    # Read the (shared) vectors DB immutable, write this shard on node-local
    # scratch, then publish it to the shared shard directory.
    local_shard = local_scratch_dir() / f"shard_{shard_index:05d}_{os.getpid()}.sqlite"

    result = run_matching_shard(
        comparison_db_path=comparison_db_path,
        shard_output_path=local_shard,
        shard_index=shard_index,
        num_shards=args.num_shards,
        top_k=args.top_k,
        min_overlap=args.min_overlap,
        uncertainty_weight=args.uncertainty_weight,
        max_rr_candidates=args.max_rr_candidates,
        batch_size=args.batch_size,
        progress_interval=args.progress_interval,
    )
    publish_db(local_shard, shard_output_path)
    print(result)
    print(f"Published -> {shard_output_path}")


if __name__ == "__main__":
    main()
