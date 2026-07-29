#!/usr/bin/env python
"""Interactive map of secondary-QC status for one date.

This is the QC-check-2 stage-2 companion to ``plot_daily_qc_interactive.py``. For
a given calendar date ``YYYY-MM-DD`` it draws every scored (QC1-fail) target
station-day on a map of the UK, coloured by its ``secondary_flag``:

* ``pass``          -- the transcribed consensus falls inside the expectation
  range predicted from the station's regional neighbour statistics,
* ``fail``          -- the consensus falls outside that range (a QC suspect),
* ``indeterminate`` -- the row could not be tested (no neighbours within 50 km,
  or no transcribed consensus value).

The scored flags come from the ``secondary_qc_status`` table written by
``score_secondary_qc``. Locations are joined from the comparison
``ensemble_metadata`` table on ``file_id`` + ``matched_year`` (latest metadata
session). Hovering or clicking a point reveals the **specifier** (the ensemble
transcription's source file name), the station name, the flag, the transcribed
consensus and the predicted expectation range; clicking copies the specifier to
the clipboard.

Example
-------
    python scripts/diagnostics/plot_secondary_qc_interactive.py 1911-06-01 \
        --output /var/tmp/secondary_qc_1911-06-01.html
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path
from typing import List, NamedTuple, Optional

import duckdb

# Allow importing the sibling interactive module whether run as a script or
# loaded by path (reuses the click-to-copy JavaScript helper).
sys.path.insert(0, str(Path(__file__).resolve().parent))

from plot_daily_rainfall_interactive import _CLICK_TO_COPY_JS  # noqa: E402

# Draw order is bottom-to-top: later traces render above earlier ones, so listing
# "fail" last puts QC suspects on top where they are easiest to spot.
FLAG_ORDER = ["indeterminate", "pass", "fail"]
FLAG_COLORS = {
    "pass": "#7a9ac4",
    "fail": "#cc8585",
    "indeterminate": "#9e9e9e",
}


def _pdir() -> Path:
    pdir = os.getenv("PDIR")
    if not pdir:
        raise SystemExit("PDIR environment variable is not set.")
    return Path(pdir)


def _table_glob(root, table: str) -> str:
    """Glob pattern for every parquet shard of ``table`` under ``root``."""
    return f"{Path(root)}/{table}/*.parquet"


def _resolve_status_source(secondary_qc_root, secondary_qc_path) -> str:
    """Return a DuckDB ``read_parquet`` source for the secondary-QC status.

    If ``secondary_qc_path`` is given it is used verbatim (a single parquet
    file). Otherwise the canonical full-run file
    ``secondary_qc_status/secondary_qc_status.parquet`` under
    ``secondary_qc_root`` is preferred; failing that, every parquet shard in that
    directory is read.
    """
    if secondary_qc_path is not None:
        return str(Path(secondary_qc_path))
    status_dir = Path(secondary_qc_root) / "secondary_qc_status"
    canonical = status_dir / "secondary_qc_status.parquet"
    if canonical.is_file():
        return str(canonical)
    return _table_glob(secondary_qc_root, "secondary_qc_status")


class FlagRecord(NamedTuple):
    """One located target station-day's secondary-QC verdict."""

    file_name: str
    location_name: Optional[str]
    latitude: float
    longitude: float
    secondary_flag: str
    consensus_value: Optional[float]
    expectation_lower: Optional[float]
    expectation_upper: Optional[float]
    predicted_consensus: Optional[float]


def load_secondary_qc_for_date(
    *,
    target_date: date,
    comparison_root,
    secondary_qc_root=None,
    secondary_qc_path=None,
    train_session_id: Optional[int] = None,
) -> List[FlagRecord]:
    """Return a ``FlagRecord`` per scored station for ``target_date``.

    Only rows whose ``matched_year`` equals ``target_date.year`` and whose
    ``month`` / ``day_of_month`` match the date are returned. Locations are joined
    from ``ensemble_metadata`` on ``file_id`` + ``matched_year`` (using the latest
    metadata session so each station appears once).
    """
    if secondary_qc_root is None and secondary_qc_path is None:
        secondary_qc_root = _pdir() / "secondary_qc_parquet"

    metadata_glob = _table_glob(comparison_root, "ensemble_metadata")
    status_source = _resolve_status_source(secondary_qc_root, secondary_qc_path)

    train_filter = (
        f"AND train_session_id = {int(train_session_id)}"
        if train_session_id is not None
        else ""
    )

    conn = duckdb.connect()
    try:
        rows = conn.execute(
            f"""
            WITH status AS (
                SELECT file_id, matched_year, secondary_flag,
                       consensus_value, expectation_lower, expectation_upper,
                       predicted_consensus
                FROM read_parquet('{status_source}')
                WHERE matched_year = ? AND month = ? AND day_of_month = ?
                {train_filter}
            ),
            meta AS (
                SELECT file_id, file_name, matched_location_name,
                       matched_latitude, matched_longitude, matched_year
                FROM read_parquet('{metadata_glob}')
                WHERE matched_latitude IS NOT NULL
                  AND matched_longitude IS NOT NULL
                  AND match_source_session_id = (
                      SELECT MAX(match_source_session_id)
                      FROM read_parquet('{metadata_glob}')
                  )
            )
            SELECT m.file_name, m.matched_location_name,
                   m.matched_latitude, m.matched_longitude,
                   s.secondary_flag, s.consensus_value,
                   s.expectation_lower, s.expectation_upper, s.predicted_consensus
            FROM status s
            JOIN meta m
              ON m.file_id = s.file_id
             AND m.matched_year = s.matched_year
            ORDER BY s.file_id
            """,
            [target_date.year, target_date.month, target_date.day],
        ).fetchall()
    finally:
        conn.close()

    def _f(value) -> Optional[float]:
        return float(value) if value is not None else None

    records: List[FlagRecord] = []
    for (
        file_name,
        location_name,
        lat,
        lon,
        flag,
        consensus,
        lower,
        upper,
        predicted,
    ) in rows:
        records.append(
            FlagRecord(
                file_name=str(file_name),
                location_name=location_name,
                latitude=float(lat),
                longitude=float(lon),
                secondary_flag=str(flag),
                consensus_value=_f(consensus),
                expectation_lower=_f(lower),
                expectation_upper=_f(upper),
                predicted_consensus=_f(predicted),
            )
        )
    return records


def build_figure(
    *,
    target_date: date,
    comparison_root,
    secondary_qc_root=None,
    secondary_qc_path=None,
    train_session_id: Optional[int] = None,
    output_path: Optional[Path] = None,
    marker_size: float = 9.0,
):
    """Build the interactive UK secondary-QC status map for ``target_date``.

    Returns the Plotly ``Figure``. If ``output_path`` is given, a self-contained
    HTML file (with the click-to-copy specifier bar) is also written there.
    """
    import plotly.graph_objects as go

    def _specifier(file_name: str) -> str:
        return file_name[:-5] if file_name.endswith(".json") else file_name

    def _fmt(v: Optional[float]) -> str:
        return f"{v:.3f} in" if v is not None else "n/a"

    def _range(lo: Optional[float], hi: Optional[float]) -> str:
        if lo is None or hi is None:
            return "n/a"
        return f"[{lo:.3f}, {hi:.3f}] in"

    records = load_secondary_qc_for_date(
        target_date=target_date,
        comparison_root=comparison_root,
        secondary_qc_root=secondary_qc_root,
        secondary_qc_path=secondary_qc_path,
        train_session_id=train_session_id,
    )
    if not records:
        raise SystemExit(
            f"No secondary-QC status found for {target_date.isoformat()}."
        )

    groups: dict[str, list[FlagRecord]] = {flag: [] for flag in FLAG_ORDER}
    for record in records:
        groups.setdefault(record.secondary_flag, []).append(record)

    fig = go.Figure()

    for flag in FLAG_ORDER:
        group = groups.get(flag, [])
        if not group:
            continue
        fig.add_trace(
            go.Scattergeo(
                lon=[r.longitude for r in group],
                lat=[r.latitude for r in group],
                mode="markers",
                name=f"{flag} ({len(group)})",
                marker=dict(
                    size=marker_size + 1,
                    color=FLAG_COLORS.get(flag, "#4f81bd"),
                    line=dict(width=0.5, color="black"),
                ),
                customdata=[
                    [
                        _specifier(r.file_name),
                        r.location_name or "",
                        r.secondary_flag,
                        _fmt(r.consensus_value),
                        _range(r.expectation_lower, r.expectation_upper),
                    ]
                    for r in group
                ],
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "%{customdata[1]}<br>"
                    "flag=%{customdata[2]}<br>"
                    "consensus=%{customdata[3]}<br>"
                    "expected=%{customdata[4]}"
                    "<extra></extra>"
                ),
            )
        )

    fig.update_geos(
        resolution=50,
        scope="europe",
        showcountries=True,
        countrycolor="black",
        showland=True,
        landcolor="rgb(243, 243, 243)",
        showocean=True,
        oceancolor="rgb(230, 240, 250)",
        lataxis_range=[49, 61],
        lonaxis_range=[-11, 4],
    )
    fig.update_layout(
        title=(
            f"Secondary-QC status  {target_date.isoformat()}<br>"
            f"<sup>{len(records)} scored QC1-fail stations "
            f"(click a point to copy its source specifier)</sup>"
        ),
        width=800,
        height=1000,
        margin=dict(l=10, r=10, t=70, b=10),
    )

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.write_html(
            str(output_path),
            include_plotlyjs="cdn",
            post_script=_CLICK_TO_COPY_JS,
        )
    return fig


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - argparse surfaces the message
        raise argparse.ArgumentTypeError(
            f"'{value}' is not a valid date; expected YYYY-MM-DD"
        ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("date", type=_parse_date, help="Date to plot, as YYYY-MM-DD")
    parser.add_argument(
        "--comparison-root",
        type=Path,
        default=None,
        help="Comparison/similarity parquet root (default: $PDIR/...)",
    )
    parser.add_argument(
        "--secondary-qc-root",
        type=Path,
        default=None,
        help="Secondary-QC parquet root (default: $PDIR/secondary_qc_parquet)",
    )
    parser.add_argument(
        "--secondary-qc-file",
        type=Path,
        default=None,
        help="A specific secondary_qc_status parquet file to read",
    )
    parser.add_argument(
        "--train-session-id",
        type=int,
        default=None,
        help="Filter to a single training session (default: all sessions present)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output HTML path (default: $PDIR/diagnostics/secondary_qc_<date>.html)",
    )
    return parser.parse_args()


def _default_comparison_root():
    """Default comparison_root parquet path."""
    src = Path(__file__).resolve().parents[2] / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from rainfall_rescue_sqlite.parquet_similarity import (
        default_comparison_parquet_root,
    )

    return default_comparison_parquet_root()


def main() -> None:
    args = parse_args()
    comparison_root = args.comparison_root or _default_comparison_root()
    secondary_qc_root = args.secondary_qc_root
    if secondary_qc_root is None and args.secondary_qc_file is None:
        secondary_qc_root = _pdir() / "secondary_qc_parquet"
    output_path = args.output or (
        _pdir() / "diagnostics" / f"secondary_qc_{args.date.isoformat()}.html"
    )

    build_figure(
        target_date=args.date,
        comparison_root=comparison_root,
        secondary_qc_root=secondary_qc_root,
        secondary_qc_path=args.secondary_qc_file,
        train_session_id=args.train_session_id,
        output_path=output_path,
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
