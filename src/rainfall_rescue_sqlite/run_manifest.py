"""Publish and load canonical run manifests for comparison datasets."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
from typing import Any, Dict, Optional


REQUIRED_KEYS = {
    "schema_version",
    "pipeline",
    "published_at_utc",
    "comparison_root",
    "session_id",
    "ensemble_metadata_path",
}


def _manifest_dir(comparison_root: Path) -> Path:
    return comparison_root / "run_manifest"


def current_manifest_path(comparison_root: Path) -> Path:
    return _manifest_dir(comparison_root) / "current.json"


def session_manifest_path(comparison_root: Path, session_id: int) -> Path:
    return _manifest_dir(comparison_root) / f"session_{int(session_id):06d}.json"


def _git_head(repo_root: Path) -> Optional[str]:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    value = proc.stdout.strip()
    return value or None


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def publish_match_metadata_run_manifest(
    *,
    comparison_root: Path,
    session_id: int,
    ensemble_metadata_path: Path,
    data_metadata_input_path: Optional[Path] = None,
    allsheets_metadata_input_path: Optional[Path] = None,
) -> Path:
    """Write per-session and current manifests for the metadata match pipeline."""
    comparison_root = Path(comparison_root).resolve()
    ensemble_metadata_path = Path(ensemble_metadata_path).resolve()
    repo_root = Path(__file__).resolve().parents[2]

    payload: Dict[str, Any] = {
        "schema_version": 1,
        "pipeline": "match_metadata",
        "published_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "comparison_root": str(comparison_root),
        "session_id": int(session_id),
        "ensemble_metadata_path": str(ensemble_metadata_path),
        "data_metadata_input_path": (
            str(Path(data_metadata_input_path).resolve())
            if data_metadata_input_path is not None
            else None
        ),
        "allsheets_metadata_input_path": (
            str(Path(allsheets_metadata_input_path).resolve())
            if allsheets_metadata_input_path is not None
            else None
        ),
        "git_commit": _git_head(repo_root),
    }

    session_path = session_manifest_path(comparison_root, int(session_id))
    current_path = current_manifest_path(comparison_root)

    _write_json_atomic(session_path, payload)
    _write_json_atomic(current_path, payload)
    return current_path


def load_current_run_manifest(
    comparison_root: Path,
    *,
    expected_pipeline: Optional[str] = None,
    require_root_match: bool = False,
) -> Dict[str, Any]:
    """Load and validate the canonical current run manifest for a dataset."""
    resolved_root = Path(comparison_root).resolve()
    path = current_manifest_path(resolved_root)
    if not path.exists():
        raise FileNotFoundError(
            f"Run manifest not found: {path}. Run scripts/local/submit_local.sh match_metadata first."
        )
    payload: Dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))

    missing = sorted(REQUIRED_KEYS - set(payload.keys()))
    if missing:
        raise ValueError(f"Manifest {path} is missing required keys: {', '.join(missing)}")

    if int(payload["schema_version"]) != 1:
        raise ValueError(
            f"Unsupported manifest schema_version={payload['schema_version']} in {path}"
        )

    session_id = int(payload["session_id"])
    if session_id <= 0:
        raise ValueError(f"Invalid session_id={session_id} in {path}")

    if expected_pipeline is not None and str(payload["pipeline"]) != expected_pipeline:
        raise ValueError(
            f"Manifest pipeline={payload['pipeline']!r} does not match expected "
            f"{expected_pipeline!r}"
        )

    manifest_root = Path(str(payload["comparison_root"])).resolve()
    if require_root_match and manifest_root != resolved_root:
        raise ValueError(
            f"Manifest comparison_root={manifest_root} does not match requested root "
            f"{resolved_root}"
        )

    metadata_path = Path(str(payload["ensemble_metadata_path"])).resolve()
    if metadata_path.parent.name != "ensemble_metadata":
        raise ValueError(
            f"Manifest ensemble_metadata_path is not under an ensemble_metadata directory: "
            f"{metadata_path}"
        )
    try:
        metadata_path.relative_to(manifest_root)
    except ValueError as exc:
        raise ValueError(
            f"Manifest ensemble_metadata_path={metadata_path} is outside comparison_root "
            f"{manifest_root}"
        ) from exc

    return payload


def publish_main_run_manifest(
    *,
    comparison_root: Path,
    session_id: int,
    ensemble_metadata_path: Path,
    data_metadata_input_path: Optional[Path] = None,
    allsheets_metadata_input_path: Optional[Path] = None,
) -> Path:
    """Backward-compatible alias for older callers."""
    return publish_match_metadata_run_manifest(
        comparison_root=comparison_root,
        session_id=session_id,
        ensemble_metadata_path=ensemble_metadata_path,
        data_metadata_input_path=data_metadata_input_path,
        allsheets_metadata_input_path=allsheets_metadata_input_path,
    )
