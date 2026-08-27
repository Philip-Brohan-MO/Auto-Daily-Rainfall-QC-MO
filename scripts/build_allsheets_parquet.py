#!/usr/bin/env python
"""Build the ALLSHEETS Parquet dataset from Rainfall-Rescue source-sheet CSVs."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.rainfall_rescue_sqlite.parquet_ingest import (
    default_allsheets_parquet_root,
    default_rainfall_rescue_root,
    ingest_allsheets_to_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Rainfall-Rescue ALLSHEETS CSVs into a Parquet dataset"
    )
    parser.add_argument(
        "--rainfall-rescue-root",
        type=Path,
        default=None,
        help="Root containing DATA/ and ALLSHEETS/ (default: package PDIR-derived root)",
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=None,
        help="Destination Parquet root (default: package PDIR-derived root)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    rainfall_rescue_root = args.rainfall_rescue_root or default_rainfall_rescue_root()
    dataset_root = args.dataset_root or default_allsheets_parquet_root()

    result = ingest_allsheets_to_parquet(
        rainfall_rescue_root=rainfall_rescue_root,
        dataset_root=dataset_root,
    )
    print(result)


if __name__ == "__main__":
    main()
