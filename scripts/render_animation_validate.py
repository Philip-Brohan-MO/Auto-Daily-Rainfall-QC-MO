"""Validate stage: check all expected frames were rendered (single SLURM job).

Reads the manifest and confirms every frame index has a file on disc. Missing
frames are reported both as raw indices and grouped into the shards that own
them, so a rerun can resubmit exactly the failed array tasks. Exits non-zero on
any gap so the dependent encode stage does not run against an incomplete set.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from src.rainfall_rescue_sqlite.rainfall_animation import frame_filename


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass paths explicitly")
    return Path(pdir).joinpath(*parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate rendered animation frames")
    parser.add_argument("--manifest-path", type=Path, default=None)
    return parser.parse_args()


def _shard_for_index(manifest: dict, global_index: int) -> int:
    for shard in manifest["shards"]:
        if shard["first_index"] <= global_index <= shard["last_index"]:
            return int(shard["shard_index"])
    return -1


def main() -> None:
    args = parse_args()
    manifest_path = args.manifest_path or _pdir_path("animation", "manifest.json")
    manifest = json.loads(Path(manifest_path).read_text())

    frame_dir = Path(manifest["frame_dir"])
    total = int(manifest["total_frames"])

    missing_indices = [
        i for i in range(total) if not (frame_dir / frame_filename(i)).exists()
    ]

    if not missing_indices:
        print(f"All {total} frames present in {frame_dir}.")
        return

    failed_shards = sorted({_shard_for_index(manifest, i) for i in missing_indices})
    print(f"MISSING {len(missing_indices)} of {total} frames in {frame_dir}.")
    preview = ", ".join(str(i) for i in missing_indices[:20])
    print(f"  first missing indices: {preview}")
    print(f"  affected shards: {failed_shards}")
    array_spec = ",".join(str(s) for s in failed_shards if s >= 0)
    print(f"  rerun with: --array={array_spec}")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
