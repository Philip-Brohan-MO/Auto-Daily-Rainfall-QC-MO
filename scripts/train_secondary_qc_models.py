"""Train the two secondary-QC XGBoost models (stage 2 of the second QC check).

Loads the reliable (QC1-pass) station-days, fits model 1 (predicts a station's
consensus rainfall from its regional neighbour statistics) and model 2 (predicts
model 1's absolute error), calibrates the expectation-range multiplier ``k`` and
persists both models plus a training-session record.

Run as a single job (XGBoost is multithreaded); size the node with enough cores
and memory for the requested ``--max-rows`` sample.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.parquet_secondary_qc import (
    build_training_frame,
    default_secondary_qc_parquet_root,
    train_models,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the secondary-QC models")
    parser.add_argument("--regional-root", type=Path, default=None)
    parser.add_argument("--qc-root", type=Path, default=None)
    parser.add_argument("--secondary-qc-root", type=Path, default=None)
    parser.add_argument(
        "--qc-session-id",
        type=int,
        default=None,
        help="QC1 session to draw the pass/fail flags from (default: latest)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=5_000_000,
        help="Cap on training rows (month-stratified sample); 0 or negative = all",
    )
    parser.add_argument("--coverage-target", type=float, default=0.99)
    parser.add_argument("--n-folds", type=int, default=5)
    parser.add_argument("--calib-fraction", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--n-jobs",
        type=int,
        default=None,
        help="XGBoost threads; defaults to $SLURM_CPUS_PER_TASK or all cores",
    )
    return parser.parse_args()


def _resolve_n_jobs(explicit: int | None) -> int:
    if explicit is not None:
        return explicit
    env = os.environ.get("SLURM_CPUS_PER_TASK")
    return int(env) if env else -1


def main() -> None:
    args = parse_args()

    secondary_qc_root = args.secondary_qc_root or default_secondary_qc_parquet_root()
    max_rows = args.max_rows if args.max_rows and args.max_rows > 0 else None
    n_jobs = _resolve_n_jobs(args.n_jobs)

    print(
        f"Building training frame (max_rows={max_rows}, seed={args.seed}) ...",
        flush=True,
    )
    frame = build_training_frame(
        regional_root=args.regional_root,
        qc_root=args.qc_root,
        qc_session_id=args.qc_session_id,
        max_rows=max_rows,
        seed=args.seed,
    )
    print(f"Training on {len(frame)} reliable station-days.", flush=True)
    if frame.empty:
        raise SystemExit("No reliable (QC1-pass) rows found; cannot train.")

    # Resolve the qc_session_id actually used so it is recorded with the models.
    from src.rainfall_rescue_sqlite.parquet_secondary_qc import _resolve_qc_session_id
    from src.rainfall_rescue_sqlite.parquet_qc_exact_monthly import (
        default_qc_parquet_root,
    )

    qc_root = args.qc_root or default_qc_parquet_root()
    qc_session_id = _resolve_qc_session_id(qc_root, args.qc_session_id)

    result = train_models(
        frame=frame,
        secondary_qc_root=secondary_qc_root,
        qc_session_id=qc_session_id,
        coverage_target=args.coverage_target,
        n_folds=args.n_folds,
        calib_fraction=args.calib_fraction,
        seed=args.seed,
        n_jobs=n_jobs,
    )

    print(
        f"Trained secondary-QC session {result.train_session_id}: "
        f"n_train={result.n_train} n_calib={result.n_calib} k={result.k:.3f} "
        f"coverage={result.coverage_achieved:.4f} "
        f"MAE={result.mae_inches:.4f} in  R2(z)={result.r2_transformed:.4f}"
    )
    print(f"Models -> {result.models_dir}")


if __name__ == "__main__":
    main()
