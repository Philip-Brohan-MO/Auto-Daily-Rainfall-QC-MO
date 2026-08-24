"""Build the ALLSHEETS residual comparison vectors, ready for sharded matching.

This is the second-pass counterpart to ``build_similarity_vectors.py``. The
candidate side is rebuilt from the ALLSHEETS Parquet dataset, while the query
side reuses the DATA comparison root restricted to the *residual* ensemble
files (those the DATA pass could not match exactly, per its
``ensemble_metadata`` assignment).
"""

from __future__ import annotations

import argparse
from pathlib import Path

from src.rainfall_rescue_sqlite.parquet_ingest import default_allsheets_parquet_root
from src.rainfall_rescue_sqlite.parquet_similarity import (
    build_allsheets_comparison_vectors_parquet,
    default_allsheets_comparison_parquet_root,
    default_comparison_parquet_root,
)


def _latest_metadata_session(comparison_root: Path) -> Path:
    """Return the highest-numbered ``ensemble_metadata/session_*.parquet``."""
    meta_dir = comparison_root / "ensemble_metadata"
    sessions = list(meta_dir.glob("session_*.parquet"))
    if not sessions:
        raise SystemExit(
            f"No ensemble_metadata/session_*.parquet found under {comparison_root}.\n"
            "Run the DATA metadata assignment first (assign_ensemble_metadata_parquet)."
        )
    return max(sessions, key=lambda p: int(p.stem.split("_")[1]))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build ALLSHEETS residual comparison vectors"
    )
    parser.add_argument("--allsheets-dataset-root", type=Path, default=None)
    parser.add_argument("--source-comparison-root", type=Path, default=None)
    parser.add_argument("--allsheets-comparison-root", type=Path, default=None)
    parser.add_argument(
        "--data-metadata-path",
        type=Path,
        default=None,
        help="DATA ensemble_metadata parquet (default: latest session in "
        "--source-comparison-root)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    allsheets_dataset_root = args.allsheets_dataset_root or default_allsheets_parquet_root()
    source_comparison_root = args.source_comparison_root or default_comparison_parquet_root()
    allsheets_comparison_root = (
        args.allsheets_comparison_root or default_allsheets_comparison_parquet_root()
    )
    data_metadata_path = args.data_metadata_path or _latest_metadata_session(
        source_comparison_root
    )

    print("Building ALLSHEETS residual comparison vectors")
    print(f"  ALLSHEETS dataset root : {allsheets_dataset_root}")
    print(f"  Source (DATA) root     : {source_comparison_root}")
    print(f"  DATA metadata parquet  : {data_metadata_path}")
    print(f"  Output root            : {allsheets_comparison_root}")

    result = build_allsheets_comparison_vectors_parquet(
        allsheets_dataset_root=allsheets_dataset_root,
        source_comparison_root=source_comparison_root,
        allsheets_comparison_root=allsheets_comparison_root,
        data_metadata_path=data_metadata_path,
        overwrite=True,
    )
    print(result)
    print(f"  ALLSHEETS candidate vectors : {result.rr_vectors:,}")
    print(f"  Residual ensemble queries   : {result.ensemble_vectors:,}")
    print(f"Vectors written -> {allsheets_comparison_root}")


if __name__ == "__main__":
    main()
