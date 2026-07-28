"""Build the SEF analysis Parquet dataset for one SEF year.

Parses ``<sef-root>/tsv/<year>/*.tsv`` into ``observations`` and
``daily_national`` Parquet tables under ``--out-root`` (see
:mod:`rainfall_rescue_sqlite.sef_analysis`). The year can be given directly with
``--year`` or, for a SLURM array where each task handles one year, by index into
the sorted list of discovered years with ``--year-index`` (defaulting to
``$SLURM_ARRAY_TASK_ID``). Because every year's output is disjoint, there is no
merge stage.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/ (conda run overwrites
# PYTHONPATH, so inject the repo root explicitly).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rainfall_rescue_sqlite.sef_analysis import (  # noqa: E402
    build_year_parquet,
    default_analysis_root,
    default_sef_root,
    discover_years,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the SEF analysis Parquet dataset for one SEF year"
    )
    parser.add_argument(
        "--sef-root",
        type=Path,
        default=None,
        help="SEF export root (contains tsv/<year>/). Default: $PDIR/sef_export",
    )
    parser.add_argument(
        "--out-root",
        type=Path,
        default=None,
        help="Output analysis root. Default: $PDIR/sef_analysis",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--year", type=int, default=None, help="Calendar year to build")
    group.add_argument(
        "--year-index",
        type=int,
        default=None,
        help="Index into the sorted list of SEF years; defaults to $SLURM_ARRAY_TASK_ID",
    )
    return parser.parse_args()


def _resolve_year_index(explicit: int | None) -> int:
    if explicit is not None:
        return explicit
    env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if env is None:
        raise SystemExit(
            "No --year / --year-index and SLURM_ARRAY_TASK_ID not set; cannot pick a year"
        )
    return int(env)


def main() -> None:
    args = parse_args()
    sef_root = args.sef_root or default_sef_root()
    out_root = args.out_root or default_analysis_root()

    if args.year is not None:
        year = args.year
    else:
        years = discover_years(sef_root)
        if not years:
            raise SystemExit(f"No SEF years found under {sef_root}/tsv")
        index = _resolve_year_index(args.year_index)
        if not 0 <= index < len(years):
            raise SystemExit(
                f"year-index {index} out of range 0..{len(years) - 1} "
                f"({len(years)} years under {sef_root}/tsv)"
            )
        year = years[index]

    print(f"Building SEF analysis parquet for {year}")
    print(f"  sef-root:  {sef_root}")
    print(f"  out-root:  {out_root}")
    summary = build_year_parquet(sef_root, year, out_root)
    passed = summary["n_passed"]
    total = summary["n_observations"]
    pct = (100.0 * passed / total) if total else 0.0
    print(
        f"  {year}: {total} observations from {summary['n_stations']} stations "
        f"over {summary['n_days']} days; {passed} passed QC ({pct:.1f}%)"
    )


if __name__ == "__main__":
    main()
