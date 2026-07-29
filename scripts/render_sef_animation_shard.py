"""Render stage: render one shard of SEF-animation frames (a SLURM array task).

Mirrors ``render_animation_shard.py`` but reads values from the exported SEF
``.tsv`` files (via :func:`sef_animation.render_frame_range`) instead of the
ensemble SQLite database. Frames are written to node-local scratch first, then
published to the shared frame directory.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import date
from pathlib import Path

from src.rainfall_rescue_sqlite.sef_animation import (
    frame_filename,
    render_frame_range,
)
from src.rainfall_rescue_sqlite.sqlite_staging import local_scratch_dir


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass paths explicitly")
    return Path(pdir).joinpath(*parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render one SEF-animation frame shard")
    parser.add_argument("--manifest-path", type=Path, default=None)
    parser.add_argument(
        "--shard-index",
        type=int,
        default=None,
        help="Shard index; defaults to $SLURM_ARRAY_TASK_ID",
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


def main() -> None:
    args = parse_args()
    manifest_path = args.manifest_path or _pdir_path("sef_animation", "manifest.json")

    manifest = json.loads(Path(manifest_path).read_text())
    shard_index = _resolve_shard_index(args.shard_index)

    shard = manifest["shards"][shard_index]
    first_index = int(shard["first_index"])
    last_index = int(shard["last_index"])

    frame_dir = Path(manifest["frame_dir"])
    frame_dir.mkdir(parents=True, exist_ok=True)

    if last_index < first_index:
        print(f"Shard {shard_index} has no frames; nothing to do.")
        return

    sef_root = Path(manifest["sef_root"])
    start_date = date.fromisoformat(manifest["date_start"])
    end_date = date.fromisoformat(manifest["date_end"])
    frames_per_day = int(manifest["frames_per_day"])

    # Render to node-local scratch, then publish PNGs to the shared frame dir.
    local_dir = local_scratch_dir() / f"sef_anim_shard_{shard_index:05d}_{os.getpid()}"
    local_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"Shard {shard_index}/{manifest['num_shards']}: "
        f"frames {first_index}-{last_index} "
        f"({last_index - first_index + 1} frames) from {sef_root}"
    )

    render_frame_range(
        sef_root=sef_root,
        start_date=start_date,
        end_date=end_date,
        frames_per_day=frames_per_day,
        first_index=first_index,
        last_index=last_index,
        output_dir=local_dir,
        cmap=manifest["cmap"],
        vmax=float(manifest["vmax"]),
        marker_size=float(manifest["marker_size"]),
        error_color=manifest.get("error_color", "#d62728"),
    )

    for global_index in range(first_index, last_index + 1):
        name = frame_filename(global_index)
        shutil.copy2(local_dir / name, frame_dir / name)
    shutil.rmtree(local_dir, ignore_errors=True)

    print(f"Shard {shard_index} complete: published to {frame_dir}")


if __name__ == "__main__":
    main()
