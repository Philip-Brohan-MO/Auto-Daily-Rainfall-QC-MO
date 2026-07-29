"""Encode stage: assemble rendered frames into an MP4 (single SLURM job).

Runs ffmpeg once over the numerically-ordered frame sequence to produce an
H.264 MP4 at the manifest's frame rate. Frames are named ``frame_NNNNNNN.png``
so ffmpeg's numeric input pattern reads them in the correct order.

The video is encoded onto node-local scratch first (fast, and the ``+faststart``
second pass rewrites the whole file, which is slow on the shared parallel
filesystem) and only the finished file is published to the shared output path
via an atomic rename. That way a killed or timed-out job never leaves a
truncated, unplayable MP4 at the destination. A metadata sidecar records the run
parameters next to the video.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

from src.rainfall_rescue_sqlite.sqlite_staging import local_scratch_dir


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass paths explicitly")
    return Path(pdir).joinpath(*parts)


def _thread_count() -> int:
    slurm = [
        int(v)
        for var in ("SLURM_CPUS_PER_TASK", "SLURM_NTASKS", "SLURM_CPUS_ON_NODE")
        if (v := os.environ.get(var)) and v.isdigit() and int(v) > 0
    ]
    if slurm:
        return max(slurm)
    return os.cpu_count() or 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Encode animation frames to MP4")
    parser.add_argument("--manifest-path", type=Path, default=None)
    parser.add_argument(
        "--preset",
        default=os.environ.get("RENDER_PRESET", "fast"),
        help="x264 preset (faster presets encode quicker at a small size cost)",
    )
    parser.add_argument(
        "--crf",
        type=int,
        default=int(os.environ.get("RENDER_CRF", "18")),
        help="x264 constant-rate-factor (lower = higher quality, larger file)",
    )
    parser.add_argument(
        "--keep-frames",
        action="store_true",
        help="Keep the intermediate PNG frames after encoding (default: delete)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest_path = args.manifest_path or _pdir_path("animation", "manifest.json")
    manifest = json.loads(Path(manifest_path).read_text())

    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg is None:
        raise SystemExit(
            "ffmpeg not found on PATH. Install ffmpeg or run the encode step "
            "manually against the frame directory."
        )

    frame_dir = Path(manifest["frame_dir"])
    output_path = Path(manifest["output_path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fps = int(manifest["fps"])
    threads = _thread_count()

    # Encode onto node-local scratch, then publish the finished file.
    local_dir = local_scratch_dir() / f"anim_encode_{os.getpid()}"
    local_dir.mkdir(parents=True, exist_ok=True)
    local_output = local_dir / output_path.name

    cmd = [
        ffmpeg,
        "-y",
        "-framerate", str(fps),
        "-i", str(frame_dir / "frame_%07d.png"),
        "-c:v", "libx264",
        "-preset", args.preset,
        "-crf", str(args.crf),
        "-threads", str(threads),
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        str(local_output),
    ]
    print("Running:", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)

        # Publish atomically: copy to a temp name on the shared FS, then rename
        # into place so no partial file is ever visible at output_path.
        tmp_output = output_path.with_name(output_path.name + ".partial")
        shutil.copy2(local_output, tmp_output)
        os.replace(tmp_output, output_path)
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)

    sidecar = output_path.with_suffix(output_path.suffix + ".json")
    sidecar.write_text(json.dumps(manifest, indent=2))
    print(f"Wrote video: {output_path}")
    print(f"Wrote metadata: {sidecar}")

    if not args.keep_frames:
        shutil.rmtree(frame_dir, ignore_errors=True)
        print(f"Removed intermediate frames: {frame_dir}")


if __name__ == "__main__":
    main()
