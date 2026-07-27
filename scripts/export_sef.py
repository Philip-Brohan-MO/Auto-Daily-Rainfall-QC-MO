"""Export located, quality-controlled daily rainfall to Station Exchange Format.

Runs one SEF export slice. The file_id slice can be given directly
(``--start-file-id`` / ``--end-file-id``) or derived from a SLURM-array-style
partition (``--num-shards`` / ``--shard-index`` / ``--total-file-ids``), mirroring
the regional-stats and secondary-QC shard runners. Each shard writes disjoint
station-year ``.tsv`` files into ``<output_root>/tsv/<year>/`` so no merge stage
is needed.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/ (conda run overwrites
# PYTHONPATH, so inject the repo root explicitly).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.sef_export import (  # noqa: E402
    DEFAULT_LINK,
    DEFAULT_OBS_HOUR,
    DEFAULT_SOURCE,
    export_sef,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export located, QC'd daily rainfall to Station Exchange Format"
    )
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--comparison-root", type=Path, default=None)
    parser.add_argument("--consensus-root", type=Path, default=None)
    parser.add_argument("--qc-root", type=Path, default=None)
    parser.add_argument("--secondary-qc-root", type=Path, default=None)
    parser.add_argument("--qc-session-id", type=int, default=None)
    parser.add_argument("--source", type=str, default=DEFAULT_SOURCE)
    parser.add_argument("--link", type=str, default=DEFAULT_LINK)
    parser.add_argument("--obs-hour", type=int, default=DEFAULT_OBS_HOUR)
    parser.add_argument("--batch-rows", type=int, default=200_000)

    # Slice selection: either an explicit range ...
    parser.add_argument("--start-file-id", type=int, default=None)
    parser.add_argument("--end-file-id", type=int, default=None)
    # ... or a SLURM-array-style partition.
    parser.add_argument("--num-shards", type=int, default=None)
    parser.add_argument(
        "--shard-index",
        type=int,
        default=None,
        help="Shard index; defaults to $SLURM_ARRAY_TASK_ID",
    )
    parser.add_argument(
        "--total-file-ids",
        type=int,
        default=None,
        help="Total number of file_ids (used to compute the shard's start/end slice)",
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


def _resolve_slice(args: argparse.Namespace) -> tuple[int | None, int | None]:
    if args.num_shards is not None:
        if args.total_file_ids is None:
            raise SystemExit("--num-shards requires --total-file-ids")
        shard_index = _resolve_shard_index(args.shard_index)
        ids_per_shard = (args.total_file_ids + args.num_shards - 1) // args.num_shards
        start_file_id = shard_index * ids_per_shard + 1
        end_file_id = min(start_file_id + ids_per_shard - 1, args.total_file_ids)
        print(
            f"SEF export shard {shard_index}/{args.num_shards}: "
            f"file_ids {start_file_id}-{end_file_id}"
        )
        return start_file_id, end_file_id
    return args.start_file_id, args.end_file_id


def main() -> None:
    args = parse_args()
    start_file_id, end_file_id = _resolve_slice(args)

    result = export_sef(
        output_root=args.output_root,
        comparison_root=args.comparison_root,
        consensus_root=args.consensus_root,
        qc_root=args.qc_root,
        secondary_qc_root=args.secondary_qc_root,
        qc_session_id=args.qc_session_id,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
        source=args.source,
        link=args.link,
        obs_hour=args.obs_hour,
        batch_rows=args.batch_rows,
    )

    print(
        f"SEF export done (qc_session={result.qc_session_id}): "
        f"{result.files_written} files, {result.obs_rows} observations"
    )
    print(f"Output root -> {result.output_root}")


if __name__ == "__main__":
    main()
