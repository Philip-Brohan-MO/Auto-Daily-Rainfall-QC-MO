"""Score the QC1-failed station-days with the secondary-QC models.

Applies the trained expectation models to every station-day that *failed* the
first QC check, flagging each ``pass`` (plausible), ``fail`` (genuinely suspect)
or ``indeterminate`` (no neighbours / no consensus value). The failed rows are
streamed, so this runs comfortably as a single job; an optional ``--start-file-id``
/ ``--end-file-id`` range lets it be sharded like the regional-stats stage.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.parquet_secondary_qc import (
    default_secondary_qc_parquet_root,
    score_secondary_qc,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score failed days with the secondary-QC models")
    parser.add_argument("--regional-root", type=Path, default=None)
    parser.add_argument("--qc-root", type=Path, default=None)
    parser.add_argument("--secondary-qc-root", type=Path, default=None)
    parser.add_argument(
        "--train-session-id",
        type=int,
        default=None,
        help="Trained model session to use (default: latest)",
    )
    parser.add_argument(
        "--qc-session-id",
        type=int,
        default=None,
        help="QC1 session whose 'fail' flags are re-tested (default: latest)",
    )
    parser.add_argument("--output", type=Path, default=None, help="Output parquet path")
    parser.add_argument("--start-file-id", type=int, default=None)
    parser.add_argument("--end-file-id", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    secondary_qc_root = args.secondary_qc_root or default_secondary_qc_parquet_root()
    output_path = args.output or (
        secondary_qc_root / "secondary_qc_status" / "secondary_qc_status.parquet"
    )

    result = score_secondary_qc(
        output_path=output_path,
        regional_root=args.regional_root,
        qc_root=args.qc_root,
        secondary_qc_root=secondary_qc_root,
        train_session_id=args.train_session_id,
        qc_session_id=args.qc_session_id,
        start_file_id=args.start_file_id,
        end_file_id=args.end_file_id,
    )

    print(
        f"Secondary QC scored (train session {result.train_session_id}, "
        f"qc session {result.qc_session_id}):"
    )
    print(
        f"  rows={result.rows_written}  pass={result.pass_rows}  "
        f"fail={result.fail_rows}  indeterminate={result.indeterminate_rows}"
    )
    print(f"Published -> {result.output_path}")


if __name__ == "__main__":
    main()
