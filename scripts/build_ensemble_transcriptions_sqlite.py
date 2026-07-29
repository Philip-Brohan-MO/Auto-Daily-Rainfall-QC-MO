"""Build a SQLite database from ensemble transcription JSON files."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.rainfall_rescue_sqlite.ensemble_ingest import (
    default_ensemble_db_path,
    default_ensemble_root,
    ingest_ensemble_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild ensemble_transcriptions.sqlite from ensemble JSON files"
    )
    parser.add_argument(
        "--ensemble-root",
        type=Path,
        default=None,
        help="Path to the ensemble_transcriptions directory",
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
    ensemble_root = args.ensemble_root or default_ensemble_root()
    db_path = args.db_path or default_ensemble_db_path()

    result = ingest_ensemble_json(
        ensemble_root=ensemble_root,
        db_path=db_path,
        max_files=args.max_files,
    )

    print("Ingestion completed")
    print(f"  DB path: {result.db_path}")
    print(f"  Source root: {result.source_root}")
    print(f"  Files discovered: {result.files_discovered}")
    print(f"  Files ingested: {result.files_ingested}")
    print(f"  Daily rows: {result.daily_rows}")
    print(f"  Total rows: {result.total_rows}")
    print(f"  Errors: {result.errors}")


if __name__ == "__main__":
    main()
