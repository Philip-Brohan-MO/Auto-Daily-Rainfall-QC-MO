"""Build a Parquet dataset from Rainfall Rescue combined CSV files."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.rainfall_rescue_sqlite.parquet_ingest import (
    default_rainfall_rescue_parquet_root,
    ingest_rainfall_rescue_to_parquet,
)
from src.rainfall_rescue_sqlite.ingest import default_rainfall_rescue_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild Rainfall Rescue Parquet dataset from combined CSV files"
    )
    parser.add_argument(
        "--rainfall-rescue-root",
        type=Path,
        default=None,
        help="Path to Rainfall-Rescue root (contains DATA/)",
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=None,
        help="Destination Parquet dataset root",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional limit for smoke testing",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remove an existing dataset root before rebuilding",
    )
    parser.add_argument(
        "--flush-every-files",
        type=int,
        default=500,
        help="Flush buffered rows to Parquet every N source files",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rainfall_rescue_root = args.rainfall_rescue_root or default_rainfall_rescue_root()
    dataset_root = args.dataset_root or default_rainfall_rescue_parquet_root()

    result = ingest_rainfall_rescue_to_parquet(
        rainfall_rescue_root=rainfall_rescue_root,
        dataset_root=dataset_root,
        max_files=args.max_files,
        overwrite=args.overwrite,
        flush_every_files=args.flush_every_files,
    )

    print("Parquet ingestion completed")
    print(f"  Dataset root:    {result.dataset_root}")
    print(f"  Source root:     {result.source_root}")
    print(f"  Files discovered:{result.files_discovered}")
    print(f"  Files ingested:  {result.files_ingested}")
    print(f"  Monthly rows:    {result.daily_rows}")
    print(f"  Annual rows:     {result.total_rows}")
    print(f"  Errors:          {result.errors}")


if __name__ == "__main__":
    main()
