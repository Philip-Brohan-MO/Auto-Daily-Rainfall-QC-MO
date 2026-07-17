"""Precompute stage: write the animation run manifest (single SLURM job).

Enumerates the full frame plan for a date range and interpolation density and
writes a small JSON manifest that every later stage reads. Keeping the plan in
one place makes the render array, validation and encode stages deterministic and
lets a rerun target exactly the frames that are missing.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from src.rainfall_rescue_sqlite.rainfall_animation import (
    shard_bounds,
    total_frames,
)


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass paths explicitly")
    return Path(pdir).joinpath(*parts)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"'{value}' is not a valid date; expected YYYY-MM-DD"
        ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the animation run manifest")
    parser.add_argument("--date-start", type=_parse_date, required=True)
    parser.add_argument("--date-end", type=_parse_date, required=True)
    parser.add_argument("--frames-per-day", type=int, default=6)
    parser.add_argument("--num-shards", type=int, required=True)
    parser.add_argument("--fps", type=int, default=30)
    parser.add_argument("--cmap", default="YlGnBu")
    parser.add_argument("--vmax", type=float, default=2.0)
    parser.add_argument("--marker-size", type=float, default=9.0)
    parser.add_argument("--manifest-path", type=Path, default=None)
    parser.add_argument("--frame-dir", type=Path, default=None)
    parser.add_argument("--output-path", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    manifest_path = args.manifest_path or _pdir_path("animation", "manifest.json")
    frame_dir = args.frame_dir or _pdir_path("animation", "frames")
    output_path = args.output_path or _pdir_path(
        "animation",
        f"rainfall_{args.date_start.isoformat()}_{args.date_end.isoformat()}.mp4",
    )

    n_frames = total_frames(args.date_start, args.date_end, args.frames_per_day)
    shards = [
        {
            "shard_index": i,
            "first_index": shard_bounds(n_frames, args.num_shards, i)[0],
            "last_index": shard_bounds(n_frames, args.num_shards, i)[1],
        }
        for i in range(args.num_shards)
    ]

    manifest = {
        "date_start": args.date_start.isoformat(),
        "date_end": args.date_end.isoformat(),
        "frames_per_day": args.frames_per_day,
        "num_shards": args.num_shards,
        "fps": args.fps,
        "cmap": args.cmap,
        "vmax": args.vmax,
        "marker_size": args.marker_size,
        "total_frames": n_frames,
        "frame_dir": str(frame_dir),
        "output_path": str(output_path),
        "shards": shards,
    }

    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    frame_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2))

    print(f"Wrote manifest: {manifest_path}")
    print(
        f"  {n_frames} frames across {args.num_shards} shards "
        f"({args.date_start} -> {args.date_end}, {args.frames_per_day}/day)"
    )
    print(f"  frame dir: {frame_dir}")
    print(f"  output:    {output_path}")


if __name__ == "__main__":
    main()
