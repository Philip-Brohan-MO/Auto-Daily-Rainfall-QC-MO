#!/usr/bin/env python
"""Run QC check #1: exact-match monthly consistency.

Writes check rows and consolidated day-level status using either the SQLite or
Parquet/DuckDB backend.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.rainfall_rescue_sqlite.ingest import default_db_path
from src.rainfall_rescue_sqlite.parquet_qc_exact_monthly import (
    default_roots as default_qc_parquet_roots,
    run_exact_monthly_consistency_check_parquet,
)
from src.rainfall_rescue_sqlite.qc_exact_monthly import run_exact_monthly_consistency_check


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass paths explicitly")
    return Path(pdir).joinpath(*parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run exact-monthly QC check")
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
    parser.add_argument("--qc-root", type=Path, default=None)
    parser.add_argument("--similarity-session-id", type=int, default=None)
    parser.add_argument("--tolerance", type=float, default=0.01)
    parser.add_argument("--qc-session-id", type=int, default=None)
    parser.add_argument("--start-file-id", type=int, default=None)
    parser.add_argument("--end-file-id", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.backend == "sqlite":
        ensemble_db_path = args.ensemble_db_path or _pdir_path("ensemble_transcriptions.sqlite")
        comparison_db_path = args.comparison_db_path or _pdir_path("monthly_similarity.sqlite")
        rr_db_path = args.rr_db_path or default_db_path()

        result = run_exact_monthly_consistency_check(
            ensemble_db_path=ensemble_db_path,
            comparison_db_path=comparison_db_path,
            rr_db_path=rr_db_path,
            tolerance=args.tolerance,
            qc_session_id=args.qc_session_id,
            start_file_id=args.start_file_id,
            end_file_id=args.end_file_id,
        )
        print(result)
        return

    default_ensemble_root, default_comparison_root, default_rr_root, default_qc_root = default_qc_parquet_roots()
    ensemble_dataset_root = args.ensemble_dataset_root or default_ensemble_root
    comparison_root = args.comparison_root or default_comparison_root
    rr_dataset_root = args.rr_dataset_root or default_rr_root
    qc_root = args.qc_root or default_qc_root

    result = run_exact_monthly_consistency_check_parquet(
        ensemble_dataset_root=ensemble_dataset_root,
        comparison_root=comparison_root,
        rr_dataset_root=rr_dataset_root,
        qc_root=qc_root,
        tolerance=args.tolerance,
        qc_session_id=args.qc_session_id,
        similarity_session_id=args.similarity_session_id,
        start_file_id=args.start_file_id,
        end_file_id=args.end_file_id,
    )
    print(result)


if __name__ == "__main__":
    main()
