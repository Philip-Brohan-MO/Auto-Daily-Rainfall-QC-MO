"""Export located, quality-controlled daily rainfall to Station Exchange Format.

Runs one SEF export slice. The slice is a range of matched *years*, given either
directly (``--start-year`` / ``--end-year``) or derived from a SLURM-array-style
partition (``--num-shards`` / ``--shard-index`` / ``--min-year`` / ``--max-year``).
Sharding by year (rather than by ``file_id``) keeps every duplicate transcription
of a station-year in the same shard, so they can be merged into a single SEF file.
Each shard writes disjoint ``<output_root>/tsv/<year>/`` directories, so no merge
stage is needed.
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
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    # ... or a SLURM-array-style partition over a min/max year range.
    parser.add_argument("--num-shards", type=int, default=None)
    parser.add_argument(
        "--shard-index",
        type=int,
        default=None,
        help="Shard index; defaults to $SLURM_ARRAY_TASK_ID",
    )
    parser.add_argument(
        "--min-year",
        type=int,
        default=None,
        help="Earliest matched year in the dataset (start of the partition range)",
    )
    parser.add_argument(
        "--max-year",
        type=int,
        default=None,
        help="Latest matched year in the dataset (end of the partition range)",
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
        if args.min_year is None or args.max_year is None:
            raise SystemExit("--num-shards requires --min-year and --max-year")
        if args.max_year < args.min_year:
            raise SystemExit("--max-year must be >= --min-year")
        shard_index = _resolve_shard_index(args.shard_index)
        total_years = args.max_year - args.min_year + 1
        years_per_shard = (total_years + args.num_shards - 1) // args.num_shards
        start_year = args.min_year + shard_index * years_per_shard
        end_year = min(start_year + years_per_shard - 1, args.max_year)
        if start_year > args.max_year:
            print(
                f"SEF export shard {shard_index}/{args.num_shards}: "
                f"no years in range (empty shard)"
            )
            # Signal an empty slice by an impossible range so nothing is written.
            return args.max_year + 1, args.max_year
        print(
            f"SEF export shard {shard_index}/{args.num_shards}: "
            f"years {start_year}-{end_year}"
        )
        return start_year, end_year
    return args.start_year, args.end_year


def main() -> None:
    args = parse_args()
    start_year, end_year = _resolve_slice(args)

    result = export_sef(
        output_root=args.output_root,
        comparison_root=args.comparison_root,
        consensus_root=args.consensus_root,
        qc_root=args.qc_root,
        secondary_qc_root=args.secondary_qc_root,
        qc_session_id=args.qc_session_id,
        start_year=start_year,
        end_year=end_year,
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
