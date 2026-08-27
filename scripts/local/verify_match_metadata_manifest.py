#!/usr/bin/env python3
"""Validate the canonical current run manifest for the metadata pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.rainfall_rescue_sqlite.parquet_similarity import default_comparison_parquet_root
from src.rainfall_rescue_sqlite.run_manifest import load_current_run_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate run_manifest/current.json for the metadata comparison dataset"
    )
    parser.add_argument(
        "--comparison-root",
        type=Path,
        default=None,
        help="Comparison dataset root (default: package main comparison root)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    comparison_root = args.comparison_root or default_comparison_parquet_root()

    manifest = load_current_run_manifest(
        comparison_root,
        expected_pipeline="match_metadata",
        require_root_match=True,
    )
    session_id = int(manifest["session_id"])
    metadata_path = Path(str(manifest["ensemble_metadata_path"]))

    if not metadata_path.exists():
        raise SystemExit(f"Manifest metadata file does not exist: {metadata_path}")

    expected_name = f"session_{session_id:06d}.parquet"
    if metadata_path.name != expected_name:
        raise SystemExit(
            f"Manifest session_id={session_id} does not match metadata file name {metadata_path.name}"
        )

    print("Manifest validation passed")
    print(f"  comparison_root: {Path(comparison_root).resolve()}")
    print(f"  session_id:      {session_id}")
    print(f"  metadata_file:   {metadata_path}")
    print(f"  published_at:    {manifest.get('published_at_utc')}")
    print(f"  git_commit:      {manifest.get('git_commit')}")


if __name__ == "__main__":
    main()
