"""Run one shard of the exact-monthly QC check (a SLURM array task).

Each shard processes its slice of file_ids and writes daily_qc_results rows to
``<shard-dir>/qc_shard_<index>.sqlite`` (SQLite backend) or
``<shard-dir>/qc_shard_<index>.parquet`` (DuckDB backend).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.ingest import default_db_path
from src.rainfall_rescue_sqlite.parquet_qc_exact_monthly import (
    default_qc_shard_dir,
    default_roots as default_qc_parquet_roots,
    run_exact_monthly_consistency_shard_parquet,
)
from src.rainfall_rescue_sqlite.qc_exact_monthly import (
    run_exact_monthly_consistency_shard,
)
from src.rainfall_rescue_sqlite.sqlite_staging import local_scratch_dir, publish_db


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass paths explicitly")
    return Path(pdir).joinpath(*parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one QC shard")
    parser.add_argument(
        "--backend",
        choices=("duckdb", "sqlite"),
        default="duckdb",
        help="Storage backend for QC artifacts",
    )
    parser.add_argument("--ensemble-db-path", type=Path, default=None)
    parser.add_argument("--comparison-db-path", type=Path, default=None)
    parser.add_argument("--rr-db-path", type=Path, default=None)
    parser.add_argument("--ensemble-dataset-root", type=Path, default=None)
    parser.add_argument("--comparison-root", type=Path, default=None)
    parser.add_argument("--rr-dataset-root", type=Path, default=None)
    parser.add_argument("--similarity-session-id", type=int, default=None)
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
        help="Total number of file_ids in the ensemble DB (used to compute start/end)",
    )
    parser.add_argument("--tolerance", type=float, default=0.01)
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

    shard_dir = args.shard_dir or default_qc_shard_dir()
    shard_index = _resolve_shard_index(args.shard_index)
    num_shards = args.num_shards
    total_file_ids = args.total_file_ids

    # Partition file_ids evenly across shards (1-based).
    ids_per_shard = (total_file_ids + num_shards - 1) // num_shards
    start_file_id = shard_index * ids_per_shard + 1
    end_file_id = min(start_file_id + ids_per_shard - 1, total_file_ids)

    print(f"Shard {shard_index}/{num_shards}: file_ids {start_file_id}-{end_file_id}")

    if args.backend == "sqlite":
        ensemble_db_path = args.ensemble_db_path or _pdir_path("ensemble_transcriptions.sqlite")
        comparison_db_path = args.comparison_db_path or _pdir_path("monthly_similarity.sqlite")
        rr_db_path = args.rr_db_path or default_db_path()

        shard_output_path = Path(shard_dir) / f"qc_shard_{shard_index:05d}.sqlite"
        local_shard = local_scratch_dir() / f"qc_shard_{shard_index:05d}_{os.getpid()}.sqlite"

        result = run_exact_monthly_consistency_shard(
            ensemble_db_path=ensemble_db_path,
            comparison_db_path=comparison_db_path,
            rr_db_path=rr_db_path,
            tolerance=args.tolerance,
            shard_output_path=local_shard,
            start_file_id=start_file_id,
            end_file_id=end_file_id,
        )

        publish_db(local_shard, shard_output_path)
    else:
        default_ensemble_root, default_comparison_root, default_rr_root, _ = default_qc_parquet_roots()
        ensemble_dataset_root = args.ensemble_dataset_root or default_ensemble_root
        comparison_root = args.comparison_root or default_comparison_root
        rr_dataset_root = args.rr_dataset_root or default_rr_root

        shard_output_path = Path(shard_dir) / f"qc_shard_{shard_index:05d}.parquet"
        shard_output_path.parent.mkdir(parents=True, exist_ok=True)

        result = run_exact_monthly_consistency_shard_parquet(
            ensemble_dataset_root=ensemble_dataset_root,
            comparison_root=comparison_root,
            rr_dataset_root=rr_dataset_root,
            tolerance=args.tolerance,
            shard_output_path=shard_output_path,
            similarity_session_id=args.similarity_session_id,
            start_file_id=start_file_id,
            end_file_id=end_file_id,
        )

    print(
        f"Shard {shard_index} done: {result.files_processed} files, "
        f"{result.day_rows_written} rows ({result.pass_rows} pass, {result.fail_rows} fail)"
    )
    print(f"Published -> {shard_output_path}")


if __name__ == "__main__":
    main()
