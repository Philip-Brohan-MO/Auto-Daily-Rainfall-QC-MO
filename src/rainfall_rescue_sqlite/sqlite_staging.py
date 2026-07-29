"""Helpers for writing SQLite databases on clusters with shared filesystems.

Shared parallel filesystems (Lustre/GPFS/NFS) often do not support the POSIX
locking / shared-memory that SQLite needs to *write* (and sometimes to read).
The pattern here is: do all SQLite writes on node-local scratch, then copy the
finished file to its shared-disc destination.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

_SIDECAR_SUFFIXES = ("", "-wal", "-shm", "-journal")


def local_scratch_dir() -> Path:
    """Return a node-local scratch directory for temporary SQLite writes.

    Prefers SLURM's per-job ``TMPDIR`` (node-local, cleaned up automatically),
    falling back to ``/var/tmp``.
    """
    base = os.environ.get("TMPDIR") or "/var/tmp"
    path = Path(base)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _remove_db(path: Path) -> None:
    for suffix in _SIDECAR_SUFFIXES:
        sidecar = Path(str(path) + suffix)
        if sidecar.exists():
            sidecar.unlink()


def publish_db(local_path: Path, final_path: Path) -> None:
    """Copy a locally-written SQLite DB to its shared destination, then clean up.

    The main ``.sqlite`` file is copied (assumed checkpointed / cleanly closed);
    any stale destination sidecar files are removed first, and the local copy
    (with sidecars) is deleted afterwards.
    """
    local_path = Path(local_path)
    final_path = Path(final_path)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    _remove_db(final_path)
    shutil.copy2(local_path, final_path)
    _remove_db(local_path)
