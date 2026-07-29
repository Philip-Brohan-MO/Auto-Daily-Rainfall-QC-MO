"""Build a SQLite database from Rainfall Rescue combined CSV files."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.rainfall_rescue_sqlite.ingest import (
    default_db_path,
    default_rainfall_rescue_root,
    ingest_combined_csvs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild rainfall_rescue.sqlite from combined station CSV files"
    )
    parser.add_argument(
        "--rainfall-rescue-root",
        type=Path,
        default=None,
        help="Path to cloned Rainfall-Rescue repository",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Destination SQLite file path",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional limit for smoke testing",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rainfall_rescue_root = args.rainfall_rescue_root or default_rainfall_rescue_root()
    db_path = args.db_path or default_db_path()

    result = ingest_combined_csvs(
        rainfall_rescue_root=rainfall_rescue_root,
        db_path=db_path,
        max_files=args.max_files,
    )

    print("Ingestion completed")
    print(f"  DB path: {result.db_path}")
    print(f"  Source root: {result.source_root}")
    print(f"  Files discovered: {result.files_discovered}")
    print(f"  Files ingested: {result.files_ingested}")
    print(f"  Station rows: {result.station_rows}")
    print(f"  Monthly rows: {result.monthly_rows}")
    print(f"  Annual rows: {result.annual_rows}")
    print(f"  Errors: {result.errors}")


if __name__ == "__main__":
    main()
