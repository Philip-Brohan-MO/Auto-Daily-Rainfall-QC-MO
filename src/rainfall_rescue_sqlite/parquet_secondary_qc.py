"""Secondary QC check: XGBoost expectation models on Parquet datasets.

Stage two of the second QC check. Two gradient-boosted models are trained on the
*reliable* station-days (those that passed the first QC check) and then used to
re-examine the *unreliable* station-days (those that failed it):

* **Model 1** predicts a station's own consensus daily rainfall from its regional
  neighbour statistics (the ``regional_daily_stats`` table).
* **Model 2** predicts the *absolute error* of model 1 from the same statistics,
  giving a per-row uncertainty.

For a failed station-day the two models yield an expectation range

    predicted_consensus  ±  k * predicted_error

(computed in a variance-stabilising ``log1p`` space and inverted to inches). A
failed day whose actual consensus falls inside the range is plausible (secondary
flag ``pass``); one that falls outside is genuinely suspect (``fail``). Days with
no neighbours, or no consensus value, cannot be tested (``indeterminate``).

Design notes
------------
* Features are the neighbour statistics plus calendar ``month`` (seasonality).
  ``median`` / ``mad`` columns are NULL when a ring has no neighbours; these are
  passed to XGBoost as ``NaN`` and handled by its native missing-value support.
  Rows with no neighbours at all (``n_50km = 0``) carry no signal and are
  excluded from training and reported ``indeterminate`` at scoring time.
* Model 2 is trained on model 1's **out-of-fold** residuals (k-fold), so its
  error estimate is not biased optimistically by model 1's in-sample fit.
* The range multiplier ``k`` is **calibrated** on a held-out slice of the pass
  set so that a target fraction (default 0.99) of reliable days fall inside the
  range.

Artifacts live under ``$PDIR/secondary_qc_parquet``:
``models/train_<NNNNNN>/`` (``model1.joblib``, ``model2.joblib``,
``metadata.json``), ``secondary_qc_sessions/session_<NNNNNN>.parquet`` and
``secondary_qc_status/`` (the scored failed days).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_qc_exact_monthly import default_qc_parquet_root
from .parquet_regional_stats import default_regional_stats_parquet_root
from .parquet_similarity import _configure_duckdb

# Feature columns fed to both models, in a fixed order. ``month`` adds
# seasonality; the neighbour statistics carry the spatial signal.
FEATURE_COLUMNS = [
    "n_20km",
    "median_20km",
    "mad_20km",
    "n_50km",
    "median_50km",
    "mad_50km",
    "month",
]

# The quantity model 1 predicts (a station's own consensus daily rainfall).
TARGET_COLUMN = "consensus_value"

# Variance-stabilising transform for the (zero-inflated, right-skewed) rainfall
# target. Modelling is done in this space; ranges are inverted back to inches.
TRANSFORM_NAME = "log1p"

# Default XGBoost hyperparameters for both regressors. Deliberately modest so a
# single node trains in minutes on a few-million-row sample.
DEFAULT_XGB_PARAMS: Dict[str, object] = {
    "n_estimators": 400,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
    "tree_method": "hist",
    "objective": "reg:squarederror",
}


@dataclass(frozen=True)
class SecondaryQCTrainResult:
    """Summary of a secondary-QC model-training run."""

    train_session_id: int
    qc_session_id: int
    n_train: int
    n_calib: int
    k: float
    coverage_target: float
    coverage_achieved: float
    mae_inches: float
    r2_transformed: float
    models_dir: Path


@dataclass(frozen=True)
class SecondaryQCScoreResult:
    """Summary of a secondary-QC scoring run over the failed station-days."""

    train_session_id: int
    qc_session_id: int
    rows_written: int
    pass_rows: int
    fail_rows: int
    indeterminate_rows: int
    output_path: Path


# --------------------------------------------------------------------------- #
# Small helpers (mirror the sibling parquet modules)
# --------------------------------------------------------------------------- #
def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _glob_sql(dir_path: Path) -> str:
    return str((dir_path / "*.parquet").resolve())


def _connect() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection honouring env-based memory/temp limits."""
    conn = duckdb.connect()
    _configure_duckdb(conn)
    return conn


def default_secondary_qc_parquet_root() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass secondary_qc_root explicitly")
    return Path(pdir) / "secondary_qc_parquet"


def _models_root(secondary_qc_root: Path) -> Path:
    return secondary_qc_root / "models"


def _next_train_session_id(secondary_qc_root: Path) -> int:
    models_dir = _models_root(secondary_qc_root)
    if not models_dir.exists():
        return 1
    ids: List[int] = []
    for path in models_dir.glob("train_*"):
        try:
            ids.append(int(path.name.split("_")[1]))
        except (IndexError, ValueError):
            continue
    return max(ids) + 1 if ids else 1


def _latest_train_session_id(secondary_qc_root: Path) -> int:
    models_dir = _models_root(secondary_qc_root)
    ids: List[int] = []
    if models_dir.exists():
        for path in models_dir.glob("train_*"):
            meta = path / "metadata.json"
            if meta.exists():
                try:
                    ids.append(int(path.name.split("_")[1]))
                except (IndexError, ValueError):
                    continue
    if not ids:
        raise ValueError(
            "No trained secondary-QC models found; run the training stage first"
        )
    return max(ids)


def _train_session_dir(secondary_qc_root: Path, train_session_id: int) -> Path:
    return _models_root(secondary_qc_root) / f"train_{train_session_id:06d}"


def _resolve_qc_session_id(qc_root: Path, explicit: Optional[int]) -> int:
    if explicit is not None:
        return int(explicit)
    conn = _connect()
    try:
        value = conn.execute(
            f"SELECT MAX(qc_session_id) FROM read_parquet("
            f"'{_glob_sql(qc_root / 'daily_qc_status')}')"
        ).fetchone()[0]
    finally:
        conn.close()
    if value is None:
        raise ValueError("No QC sessions found in qc_root; run the first QC check first")
    return int(value)


def _regional_table_glob(regional_root: Path) -> str:
    """Glob for the merged regional_daily_stats parquet (excludes shard slices).

    Prefers the merged ``session_meta<NNNNNN>_qc<NNNNNN>.parquet`` files; if none
    are present, falls back to every parquet in the directory.
    """
    import re

    merged_dir = regional_root / "regional_daily_stats"
    merged = [
        p
        for p in sorted(merged_dir.glob("session_meta*_qc*.parquet"))
        if re.fullmatch(r"session_meta\d+_qc\d+\.parquet", p.name)
    ]
    if merged:
        return str(merged[-1].resolve())
    return _glob_sql(merged_dir)


def _feature_matrix(df) -> np.ndarray:
    """Return the feature matrix (float64, NaN-preserving) in FEATURE_COLUMNS order."""
    return df[FEATURE_COLUMNS].to_numpy(dtype=np.float64)


# --------------------------------------------------------------------------- #
# Training-frame construction
# --------------------------------------------------------------------------- #
def build_training_frame(
    *,
    regional_root: Optional[Path] = None,
    qc_root: Optional[Path] = None,
    qc_session_id: Optional[int] = None,
    max_rows: Optional[int] = None,
    seed: int = 0,
):
    """Load the reliable (QC1-pass) station-days as a training DataFrame.

    Joins ``regional_daily_stats`` to ``daily_qc_status`` on
    ``(file_id, month, day_of_month)`` for the resolved ``qc_session_id``, keeping
    only rows that **passed** QC1, have at least one 50 km neighbour and a
    non-NULL consensus value. When ``max_rows`` is set and the pass set is larger,
    a reproducible Bernoulli sample of the right fraction is taken -- uniform
    sampling preserves each month's share, so the result is month-stratified.

    Returns a pandas DataFrame with the ``FEATURE_COLUMNS`` and ``TARGET_COLUMN``.
    """
    regional_root = regional_root or default_regional_stats_parquet_root()
    qc_root = qc_root or default_qc_parquet_root()
    qsid = _resolve_qc_session_id(qc_root, qc_session_id)

    regional_glob = _regional_table_glob(regional_root)
    status_glob = _glob_sql(qc_root / "daily_qc_status")

    where_pass = (
        f"s.qc_session_id = {qsid} AND s.final_flag = 'pass' "
        f"AND r.n_50km > 0 AND r.{TARGET_COLUMN} IS NOT NULL"
    )
    base_from = (
        f"FROM read_parquet('{regional_glob}') r "
        f"JOIN read_parquet('{status_glob}') s "
        f"  ON s.file_id = r.file_id AND s.month = r.month "
        f"  AND s.day_of_month = r.day_of_month "
        f"WHERE {where_pass}"
    )
    select_cols = ", ".join(f"r.{c}" for c in FEATURE_COLUMNS) + f", r.{TARGET_COLUMN}"

    conn = _connect()
    try:
        sample_clause = ""
        if max_rows is not None:
            total = conn.execute(f"SELECT COUNT(*) {base_from}").fetchone()[0]
            if total and total > max_rows:
                fraction = float(max_rows) / float(total)
                # setseed takes a value in [-1, 1]; map the integer seed into it.
                conn.execute(f"SELECT setseed({(seed % 1000) / 1000.0})")
                sample_clause = f" AND random() < {fraction}"
        frame = conn.execute(
            f"SELECT {select_cols} {base_from}{sample_clause}"
        ).df()
    finally:
        conn.close()
    return frame


# --------------------------------------------------------------------------- #
# Model training
# --------------------------------------------------------------------------- #
def _make_regressor(params: Optional[Dict[str, object]], seed: int, n_jobs: int):
    from xgboost import XGBRegressor

    merged = dict(DEFAULT_XGB_PARAMS)
    if params:
        merged.update(params)
    merged["random_state"] = seed
    merged["n_jobs"] = n_jobs
    return XGBRegressor(**merged)


def train_models(
    *,
    frame,
    secondary_qc_root: Optional[Path] = None,
    qc_session_id: int,
    coverage_target: float = 0.99,
    n_folds: int = 5,
    calib_fraction: float = 0.2,
    xgb_params: Optional[Dict[str, object]] = None,
    seed: int = 0,
    n_jobs: int = -1,
) -> SecondaryQCTrainResult:
    """Fit models 1 & 2 on ``frame``, calibrate ``k`` and persist the artifacts.

    ``frame`` is the output of :func:`build_training_frame`. Model 1 predicts
    ``log1p(consensus)``; model 2 predicts the absolute out-of-fold residual of
    model 1. ``k`` is chosen on a held-out calibration split so that
    ``coverage_target`` of those rows fall within ``pred ± k * predicted_error``.
    """
    from sklearn.metrics import r2_score
    from sklearn.model_selection import KFold, train_test_split

    if secondary_qc_root is None:
        secondary_qc_root = default_secondary_qc_parquet_root()

    started_at = _utc_now()

    X = _feature_matrix(frame)
    y = np.log1p(frame[TARGET_COLUMN].to_numpy(dtype=np.float64))

    X_train, X_calib, y_train, y_calib = train_test_split(
        X, y, test_size=calib_fraction, random_state=seed
    )

    # Out-of-fold predictions of model 1 on the training split -> honest
    # residuals for model 2's target.
    oof_pred = np.empty_like(y_train)
    kfold = KFold(n_splits=n_folds, shuffle=True, random_state=seed)
    for fold_train, fold_val in kfold.split(X_train):
        fold_model = _make_regressor(xgb_params, seed, n_jobs)
        fold_model.fit(X_train[fold_train], y_train[fold_train])
        oof_pred[fold_val] = fold_model.predict(X_train[fold_val])
    oof_abs_resid = np.abs(y_train - oof_pred)

    # Final models fit on the whole training split.
    model1 = _make_regressor(xgb_params, seed, n_jobs)
    model1.fit(X_train, y_train)
    model2 = _make_regressor(xgb_params, seed, n_jobs)
    model2.fit(X_train, oof_abs_resid)

    # Calibrate k on the held-out split: for each row the k needed to cover it is
    # resid / predicted_error; the coverage_target quantile of those is our k.
    z_pred_calib = model1.predict(X_calib)
    err_pred_calib = np.clip(model2.predict(X_calib), 1e-6, None)
    resid_calib = np.abs(y_calib - z_pred_calib)
    k_needed = resid_calib / err_pred_calib
    k = float(np.quantile(k_needed, coverage_target))
    coverage_achieved = float(np.mean(resid_calib <= k * err_pred_calib))

    # Metrics on the calibration split, reported in inches.
    pred_inches = np.expm1(z_pred_calib)
    actual_inches = np.expm1(y_calib)
    mae_inches = float(np.mean(np.abs(pred_inches - actual_inches)))
    r2_transformed = float(r2_score(y_calib, z_pred_calib))

    # Persist artifacts.
    train_session_id = _next_train_session_id(secondary_qc_root)
    models_dir = _train_session_dir(secondary_qc_root, train_session_id)
    models_dir.mkdir(parents=True, exist_ok=True)

    import joblib

    joblib.dump(model1, models_dir / "model1.joblib")
    joblib.dump(model2, models_dir / "model2.joblib")

    metadata = {
        "train_session_id": train_session_id,
        "qc_session_id": int(qc_session_id),
        "feature_columns": FEATURE_COLUMNS,
        "target_column": TARGET_COLUMN,
        "transform": TRANSFORM_NAME,
        "k": k,
        "coverage_target": float(coverage_target),
        "coverage_achieved": coverage_achieved,
        "n_train": int(X_train.shape[0]),
        "n_calib": int(X_calib.shape[0]),
        "n_folds": int(n_folds),
        "calib_fraction": float(calib_fraction),
        "seed": int(seed),
        "xgb_params": {**DEFAULT_XGB_PARAMS, **(xgb_params or {})},
        "mae_inches": mae_inches,
        "r2_transformed": r2_transformed,
        "created_at": _utc_now(),
    }
    (models_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True))

    _write_train_session_row(
        secondary_qc_root=secondary_qc_root,
        train_session_id=train_session_id,
        qc_session_id=int(qc_session_id),
        metadata=metadata,
        started_at=started_at,
    )

    return SecondaryQCTrainResult(
        train_session_id=train_session_id,
        qc_session_id=int(qc_session_id),
        n_train=int(X_train.shape[0]),
        n_calib=int(X_calib.shape[0]),
        k=k,
        coverage_target=float(coverage_target),
        coverage_achieved=coverage_achieved,
        mae_inches=mae_inches,
        r2_transformed=r2_transformed,
        models_dir=models_dir,
    )


def _write_train_session_row(
    *,
    secondary_qc_root: Path,
    train_session_id: int,
    qc_session_id: int,
    metadata: dict,
    started_at: str,
) -> None:
    sessions_dir = secondary_qc_root / "secondary_qc_sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [
            {
                "train_session_id": train_session_id,
                "qc_session_id": qc_session_id,
                "started_at": started_at,
                "completed_at": _utc_now(),
                "status": "success",
                "k": float(metadata["k"]),
                "coverage_target": float(metadata["coverage_target"]),
                "coverage_achieved": float(metadata["coverage_achieved"]),
                "n_train": int(metadata["n_train"]),
                "n_calib": int(metadata["n_calib"]),
                "mae_inches": float(metadata["mae_inches"]),
                "r2_transformed": float(metadata["r2_transformed"]),
                "config_json": json.dumps(metadata, sort_keys=True),
            }
        ]
    )
    pq.write_table(
        table,
        sessions_dir / f"session_{train_session_id:06d}.parquet",
        compression="zstd",
    )


# --------------------------------------------------------------------------- #
# Model loading & scoring
# --------------------------------------------------------------------------- #
def load_models(secondary_qc_root: Path, train_session_id: int):
    """Load ``(model1, model2, metadata)`` for a training session."""
    import joblib

    models_dir = _train_session_dir(secondary_qc_root, train_session_id)
    metadata = json.loads((models_dir / "metadata.json").read_text())
    model1 = joblib.load(models_dir / "model1.joblib")
    model2 = joblib.load(models_dir / "model2.joblib")
    return model1, model2, metadata


_SCORE_SCHEMA = pa.schema(
    [
        ("file_id", pa.int64()),
        ("matched_year", pa.int64()),
        ("month", pa.int8()),
        ("day_of_month", pa.int8()),
        ("consensus_value", pa.float64()),
        ("predicted_consensus", pa.float64()),
        ("predicted_abs_error", pa.float64()),
        ("expectation_lower", pa.float64()),
        ("expectation_upper", pa.float64()),
        ("secondary_flag", pa.string()),
        ("train_session_id", pa.int64()),
        ("qc_session_id", pa.int64()),
        ("created_at", pa.string()),
    ]
)


def score_secondary_qc(
    *,
    output_path: Path,
    regional_root: Optional[Path] = None,
    qc_root: Optional[Path] = None,
    secondary_qc_root: Optional[Path] = None,
    train_session_id: Optional[int] = None,
    qc_session_id: Optional[int] = None,
    start_file_id: Optional[int] = None,
    end_file_id: Optional[int] = None,
    batch_rows: int = 200_000,
) -> SecondaryQCScoreResult:
    """Score the QC1-failed station-days and write ``secondary_qc_status`` parquet.

    Streams the failed rows (bounded memory), applies the expectation range from
    the trained models, and flags each row ``pass`` / ``fail`` / ``indeterminate``.
    Rows with no neighbours (``n_50km = 0``) or no consensus value are
    ``indeterminate``.
    """
    regional_root = regional_root or default_regional_stats_parquet_root()
    qc_root = qc_root or default_qc_parquet_root()
    secondary_qc_root = secondary_qc_root or default_secondary_qc_parquet_root()
    if train_session_id is None:
        train_session_id = _latest_train_session_id(secondary_qc_root)
    qsid = _resolve_qc_session_id(qc_root, qc_session_id)

    model1, model2, metadata = load_models(secondary_qc_root, train_session_id)
    k = float(metadata["k"])

    regional_glob = _regional_table_glob(regional_root)
    status_glob = _glob_sql(qc_root / "daily_qc_status")

    clauses = [f"s.qc_session_id = {qsid}", "s.final_flag = 'fail'"]
    if start_file_id is not None:
        clauses.append(f"r.file_id >= {int(start_file_id)}")
    if end_file_id is not None:
        clauses.append(f"r.file_id <= {int(end_file_id)}")
    where = " AND ".join(clauses)
    feature_select = ", ".join(f"r.{c}" for c in FEATURE_COLUMNS)
    query = (
        f"SELECT r.file_id, r.matched_year, r.day_of_month, "
        f"r.{TARGET_COLUMN}, {feature_select} "
        f"FROM read_parquet('{regional_glob}') r "
        f"JOIN read_parquet('{status_glob}') s "
        f"  ON s.file_id = r.file_id AND s.month = r.month "
        f"  AND s.day_of_month = r.day_of_month "
        f"WHERE {where}"
    )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    now = _utc_now()
    rows_written = pass_rows = fail_rows = indeterminate_rows = 0

    conn = _connect()
    writer = pq.ParquetWriter(str(output_path), _SCORE_SCHEMA, compression="zstd")
    try:
        reader = conn.execute(query).fetch_record_batch(batch_rows)
        for batch in reader:
            df = batch.to_pandas()
            n = len(df)
            if n == 0:
                continue
            X = _feature_matrix(df)
            z_pred = model1.predict(X)
            err_pred = np.clip(model2.predict(X), 1e-6, None)
            predicted_consensus = np.expm1(z_pred)
            lower = np.expm1(z_pred - k * err_pred)
            upper = np.expm1(z_pred + k * err_pred)

            consensus = df[TARGET_COLUMN].to_numpy(dtype=np.float64)
            n_50km = df["n_50km"].to_numpy(dtype=np.float64)
            testable = (n_50km > 0) & np.isfinite(consensus)
            inside = testable & (consensus >= lower) & (consensus <= upper)

            flags = np.where(
                ~testable, "indeterminate", np.where(inside, "pass", "fail")
            )
            # Blank out predictions/ranges for untestable rows. predicted_abs_error
            # is model 2's output: the expected absolute error in log1p space (the
            # interpretable inches range is expectation_lower / expectation_upper).
            predicted_consensus = np.where(testable, predicted_consensus, np.nan)
            err_z = np.where(testable, err_pred, np.nan)
            lower = np.where(testable, lower, np.nan)
            upper = np.where(testable, upper, np.nan)

            out = pa.table(
                {
                    "file_id": df["file_id"].to_numpy(dtype=np.int64),
                    "matched_year": df["matched_year"].to_numpy(dtype=np.int64),
                    "month": df["month"].to_numpy(dtype=np.int8),
                    "day_of_month": df["day_of_month"].to_numpy(dtype=np.int8),
                    "consensus_value": consensus,
                    "predicted_consensus": predicted_consensus,
                    "predicted_abs_error": err_z,
                    "expectation_lower": lower,
                    "expectation_upper": upper,
                    "secondary_flag": pa.array(flags, type=pa.string()),
                    "train_session_id": np.full(n, train_session_id, dtype=np.int64),
                    "qc_session_id": np.full(n, qsid, dtype=np.int64),
                    "created_at": pa.array([now] * n, type=pa.string()),
                },
                schema=_SCORE_SCHEMA,
            )
            writer.write_table(out)

            rows_written += n
            pass_rows += int(np.sum(flags == "pass"))
            fail_rows += int(np.sum(flags == "fail"))
            indeterminate_rows += int(np.sum(flags == "indeterminate"))
    finally:
        writer.close()
        conn.close()

    return SecondaryQCScoreResult(
        train_session_id=int(train_session_id),
        qc_session_id=qsid,
        rows_written=rows_written,
        pass_rows=pass_rows,
        fail_rows=fail_rows,
        indeterminate_rows=indeterminate_rows,
        output_path=output_path,
    )
