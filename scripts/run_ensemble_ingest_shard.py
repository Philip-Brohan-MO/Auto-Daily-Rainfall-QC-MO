"""Ingest one shard of the ensemble transcription JSON files (a SLURM array task)."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.rainfall_rescue_sqlite.ensemble_ingest import (
    default_ensemble_root,
    ingest_ensemble_json,
)
from src.rainfall_rescue_sqlite.parquet_ingest import ingest_ensemble_to_parquet
from src.rainfall_rescue_sqlite.sqlite_staging import local_scratch_dir, publish_db


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass the path explicitly")
    return Path(pdir).joinpath(*parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest one ensemble JSON shard")
    parser.add_argument(
        "--backend",
        choices=("duckdb", "sqlite"),
        default="duckdb",
        help="Storage backend for shard artifacts",
    )
    parser.add_argument(
        "--ensemble-root",
        type=Path,
        default=None,
        help="Root of the ensemble transcription JSON tree "
        "(default: ENSEMBLE_TRANSCRIPTIONS_ROOT or the package default)",
    )
    parser.add_argument(
        "--shard-dir",
        type=Path,
        default=None,
        help="Directory for per-shard output DBs (default: $PDIR/ensemble_shards)",
    )
    parser.add_argument("--num-shards", type=int, required=True)
    parser.add_argument(
        "--shard-index",
        type=int,
        default=None,
        help="Shard index; defaults to $SLURM_ARRAY_TASK_ID",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Cap the total JSON files considered before sharding (for testing)",
    )
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
    ensemble_root = args.ensemble_root or default_ensemble_root()
    shard_dir = args.shard_dir or _pdir_path("ensemble_shards")
    shard_index = _resolve_shard_index(args.shard_index)

    if args.backend == "sqlite":
        shard_output_path = Path(shard_dir) / f"ens_shard_{shard_index:05d}.sqlite"

        # Write this shard on node-local scratch, then publish it to the shared
        # shard directory.
        local_shard = (
            local_scratch_dir() / f"ens_shard_{shard_index:05d}_{os.getpid()}.sqlite"
        )

        result = ingest_ensemble_json(
            ensemble_root,
            local_shard,
            max_files=args.max_files,
            shard_index=shard_index,
            num_shards=args.num_shards,
        )
        publish_db(local_shard, shard_output_path)
        print(result)
        print(f"Published -> {shard_output_path}")
        return

    shard_output_path = Path(shard_dir) / f"ens_shard_{shard_index:05d}"
    result = ingest_ensemble_to_parquet(
        ensemble_root=ensemble_root,
        dataset_root=shard_output_path,
        max_files=args.max_files,
        shard_index=shard_index,
        num_shards=args.num_shards,
        overwrite=True,
    )
    print(result)
    print(f"Published -> {shard_output_path}")


if __name__ == "__main__":
    main()
