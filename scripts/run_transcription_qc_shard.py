"""Run one shard of the transcription-source QC step (a SLURM array task).

Each shard computes per-file QC metrics -- the missing-day fraction and the
12-month consensus vector -- for its contiguous ``file_id`` slice and writes
them to ``<shard-dir>/tqc_shard_<index>.parquet``. Duplicate detection happens
later in the merge step, over the combined per-file metrics.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.parquet_ingest import default_ensemble_parquet_root
from src.rainfall_rescue_sqlite.parquet_transcription_qc import (
    build_file_metrics_parquet,
    default_transcription_qc_shard_dir,
    file_id_bounds,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one transcription-QC shard")
    parser.add_argument("--ensemble-dataset-root", type=Path, default=None)
    parser.add_argument("--shard-dir", type=Path, default=None)
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
        help="Upper bound on file_id (used to compute this shard's slice)",
    )
    return parser.parse_args()


def _resolve_shard_index(explicit: "int | None") -> int:
    if explicit is not None:
        return explicit
    env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if env is None:
        raise SystemExit("shard-index not given and SLURM_ARRAY_TASK_ID unset")
    return int(env)


def main() -> None:
    args = parse_args()
    shard_index = _resolve_shard_index(args.shard_index)
    ensemble_dataset_root = args.ensemble_dataset_root or default_ensemble_parquet_root()
    shard_dir = args.shard_dir or default_transcription_qc_shard_dir()
    shard_dir.mkdir(parents=True, exist_ok=True)

    start_file_id, end_file_id = file_id_bounds(
        args.total_file_ids, args.num_shards, shard_index
    )
    out_path = shard_dir / f"tqc_shard_{shard_index:05d}.parquet"

    print(
        f"Transcription-QC shard {shard_index}/{args.num_shards}: "
        f"file_id in [{start_file_id}, {end_file_id}] -> {out_path.name}"
    )
    n_rows = build_file_metrics_parquet(
        ensemble_dataset_root=ensemble_dataset_root,
        out_path=out_path,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
    )
    print(f"Shard {shard_index} wrote {n_rows} file rows.")


if __name__ == "__main__":
    main()
