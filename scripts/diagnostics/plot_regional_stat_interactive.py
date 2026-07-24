#!/usr/bin/env python
"""Interactive map of one regional neighbour statistic for one date.

This is the QC-check-2 companion to ``plot_daily_rainfall_interactive.py``. For a
given calendar date ``YYYY-MM-DD`` and one of the regional statistics computed by
``compute_regional_daily_stats_parquet`` it draws every located target
station-day on a map of the UK, coloured by the chosen statistic.

The plottable statistics (the numeric columns of ``regional_daily_stats``) are:

* ``consensus_value`` -- the station's own consensus daily rainfall (inches),
* ``n_20km`` / ``n_50km`` -- number of neighbours within 20 km / 50 km,
* ``median_20km`` / ``median_50km`` -- neighbour median rainfall (inches),
* ``mad_20km`` / ``mad_50km`` -- neighbour median absolute deviation (inches).

Locations come from the comparison ``ensemble_metadata`` table (joined on
``file_id`` + ``matched_year`` + the row's ``metadata_session_id``). Hovering or
clicking a point reveals the **specifier** (the ensemble transcription's source
file name), the station name, and the statistic's value; clicking copies the
specifier to the clipboard.

Example
-------
    python scripts/diagnostics/plot_regional_stat_interactive.py 1891-11-13 median_20km \
        --output /var/tmp/regional_median_20km.html
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
# loaded by path (reuses the click-to-copy JavaScript / inline-HTML helpers).
sys.path.insert(0, str(Path(__file__).resolve().parent))

from plot_daily_rainfall_interactive import (  # noqa: E402
    _CLICK_TO_COPY_JS,
    inline_html,
)

# Numeric columns of ``regional_daily_stats`` that may be plotted, mapped to a
# human-readable colourbar / title label. Also serves as an allow-list so the
# stat name can be interpolated into SQL safely.
PLOTTABLE_STATS = {
    "consensus_value": "Consensus rainfall (in)",
    "n_20km": "Neighbours &le; 20 km",
    "median_20km": "Neighbour median &le; 20 km (in)",
    "mad_20km": "Neighbour MAD &le; 20 km (in)",
    "n_50km": "Neighbours &le; 50 km",
    "median_50km": "Neighbour median &le; 50 km (in)",
    "mad_50km": "Neighbour MAD &le; 50 km (in)",
}


def _pdir() -> Path:
    pdir = os.getenv("PDIR")
    if not pdir:
        raise SystemExit("PDIR environment variable is not set.")
    return Path(pdir)


def _table_glob(root, table: str) -> str:
    """Glob pattern for every parquet shard of ``table`` under ``root``."""
    return f"{Path(root)}/{table}/*.parquet"


def _resolve_regional_source(regional_root, regional_stats_path) -> str:
    """Return a DuckDB ``read_parquet`` source for the regional stats.

    If ``regional_stats_path`` is given it is used verbatim (a single parquet
    file). Otherwise the merged full-dataset file
    ``regional_daily_stats/session_meta<NNNNNN>_qc<NNNNNN>.parquet`` under
    ``regional_root`` is preferred; failing that, every parquet shard in that
    directory is read.
    """
    import re

    if regional_stats_path is not None:
        return str(Path(regional_stats_path))
    merged_dir = Path(regional_root) / "regional_daily_stats"
    merged = [
        p
        for p in sorted(merged_dir.glob("session_meta*_qc*.parquet"))
        if re.fullmatch(r"session_meta\d+_qc\d+\.parquet", p.name)
    ]
    if merged:
        return str(merged[-1])
    return _table_glob(regional_root, "regional_daily_stats")


class StatRecord(NamedTuple):
    """One located target station-day's value of the chosen statistic."""

    file_name: str
    location_name: Optional[str]
    latitude: float
    longitude: float
    value: Optional[float]


def load_regional_stat_for_date(
    *,
    stat: str,
    target_date: date,
    comparison_root,
    regional_root=None,
    regional_stats_path=None,
) -> List[StatRecord]:
    """Return a ``StatRecord`` per located station for ``stat`` on ``target_date``.

    Only rows whose ``matched_year`` equals ``target_date.year`` and whose
    ``month`` / ``day_of_month`` match the date are returned. Locations are joined
    from ``ensemble_metadata`` on ``file_id`` + ``matched_year`` + the row's own
    ``metadata_session_id``.
    """
    if stat not in PLOTTABLE_STATS:
        raise SystemExit(
            f"Unknown stat '{stat}'. Choose one of: {', '.join(PLOTTABLE_STATS)}"
        )

    metadata_glob = _table_glob(comparison_root, "ensemble_metadata")
    regional_source = _resolve_regional_source(regional_root, regional_stats_path)

    conn = duckdb.connect()
    try:
        rows = conn.execute(
            f"""
            WITH stats AS (
                SELECT file_id, matched_year, metadata_session_id,
                       {stat} AS value
                FROM read_parquet('{regional_source}')
                WHERE matched_year = ? AND month = ? AND day_of_month = ?
            ),
            meta AS (
                SELECT file_id, file_name, matched_location_name,
                       matched_latitude, matched_longitude,
                       matched_year, match_source_session_id
                FROM read_parquet('{metadata_glob}')
                WHERE matched_latitude IS NOT NULL
                  AND matched_longitude IS NOT NULL
            )
            SELECT m.file_name, m.matched_location_name,
                   m.matched_latitude, m.matched_longitude, s.value
            FROM stats s
            JOIN meta m
              ON m.file_id = s.file_id
             AND m.matched_year = s.matched_year
             AND m.match_source_session_id = s.metadata_session_id
            ORDER BY s.file_id
            """,
            [target_date.year, target_date.month, target_date.day],
        ).fetchall()
    finally:
        conn.close()

    records: List[StatRecord] = []
    for file_name, location_name, lat, lon, value in rows:
        records.append(
            StatRecord(
                file_name=str(file_name),
                location_name=location_name,
                latitude=float(lat),
                longitude=float(lon),
                value=float(value) if value is not None else None,
            )
        )
    return records


def build_figure(
    *,
    stat: str,
    target_date: date,
    comparison_root,
    regional_root=None,
    regional_stats_path=None,
    output_path: Optional[Path] = None,
    cmap: str = "Viridis",
    vmax: Optional[float] = None,
    marker_size: float = 9.0,
):
    """Build the interactive UK map of ``stat`` for ``target_date``.

    Returns the Plotly ``Figure``. If ``output_path`` is given, a self-contained
    HTML file is also written there.

    ``vmax`` sets the upper end of the (linear) colour scale; if ``None`` it is
    the 98th percentile of the day's non-null values (with a small floor).
    Stations with no value for the statistic are drawn as pale grey circles
    underneath the coloured points.
    """
    import plotly.graph_objects as go

    def _specifier(file_name: str) -> str:
        return file_name[:-5] if file_name.endswith(".json") else file_name

    records = load_regional_stat_for_date(
        stat=stat,
        target_date=target_date,
        comparison_root=comparison_root,
        regional_root=regional_root,
        regional_stats_path=regional_stats_path,
    )
    if not records:
        raise SystemExit(
            f"No regional stats found for {stat} on {target_date.isoformat()}."
        )

    valued = [r for r in records if r.value is not None]
    nulls = [r for r in records if r.value is None]

    label = PLOTTABLE_STATS[stat]

    if vmax is None:
        vals = sorted(r.value for r in valued)
        if vals:
            idx = min(len(vals) - 1, int(round(0.98 * (len(vals) - 1))))
            vmax = max(vals[idx], 1e-6)
        else:
            vmax = 1.0
    vmax = max(float(vmax), 1e-6)

    def _fmt(v: float) -> str:
        return f"{v:g}" if stat.startswith("n_") else f"{v:.3f} in"

    fig = go.Figure()

    # Null stations first, so coloured points draw on top of them.
    if nulls:
        fig.add_trace(
            go.Scattergeo(
                lon=[r.longitude for r in nulls],
                lat=[r.latitude for r in nulls],
                mode="markers",
                name="no value",
                marker=dict(
                    size=marker_size - 2,
                    color="rgb(200, 200, 200)",
                    line=dict(width=0.3, color="rgb(150,150,150)"),
                ),
                customdata=[
                    [_specifier(r.file_name), r.location_name or "", "no value"]
                    for r in nulls
                ],
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "%{customdata[1]}<br>"
                    "%{customdata[2]}"
                    "<extra></extra>"
                ),
                showlegend=False,
            )
        )

    if valued:
        fig.add_trace(
            go.Scattergeo(
                lon=[r.longitude for r in valued],
                lat=[r.latitude for r in valued],
                mode="markers",
                name=stat,
                marker=dict(
                    size=marker_size + 1,
                    color=[r.value for r in valued],
                    colorscale=cmap,
                    cmin=0.0,
                    cmax=vmax,
                    line=dict(width=0.5, color="black"),
                    colorbar=dict(title=f"{label}"),
                ),
                customdata=[
                    [_specifier(r.file_name), r.location_name or "", _fmt(r.value)]
                    for r in valued
                ],
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "%{customdata[1]}<br>"
                    f"{stat} = %{{customdata[2]}}"
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
            f"Regional stat  {stat}  {target_date.isoformat()}<br>"
            f"<sup>{len(records)} located stations "
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
        "stat",
        choices=sorted(PLOTTABLE_STATS),
        help="Regional statistic column to plot",
    )
    parser.add_argument(
        "--comparison-root",
        type=Path,
        default=None,
        help="Comparison/similarity parquet root (default: $PDIR/...)",
    )
    parser.add_argument(
        "--regional-root",
        type=Path,
        default=None,
        help="Regional stats parquet root (default: $PDIR/...)",
    )
    parser.add_argument(
        "--regional-file",
        type=Path,
        default=None,
        help="A specific regional_daily_stats parquet file to read",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output HTML path (default: $PDIR/diagnostics/regional_<stat>_<date>.html)",
    )
    parser.add_argument("--cmap", default="Viridis", help="Plotly colorscale name")
    parser.add_argument(
        "--vmax",
        type=float,
        default=None,
        help="Upper colour limit (default: 98th percentile of the day's values)",
    )
    return parser.parse_args()


def _default_roots() -> tuple:
    """Default (comparison_root, regional_root) parquet paths."""
    src = Path(__file__).resolve().parents[2] / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from rainfall_rescue_sqlite.parquet_similarity import (
        default_comparison_parquet_root,
    )
    from rainfall_rescue_sqlite.parquet_regional_stats import (
        default_regional_stats_parquet_root,
    )

    return default_comparison_parquet_root(), default_regional_stats_parquet_root()


def main() -> None:
    args = parse_args()
    default_comparison_root, default_regional_root = _default_roots()
    comparison_root = args.comparison_root or default_comparison_root
    regional_root = args.regional_root or default_regional_root
    output_path = args.output or (
        _pdir() / "diagnostics" / f"regional_{args.stat}_{args.date.isoformat()}.html"
    )

    build_figure(
        stat=args.stat,
        target_date=args.date,
        comparison_root=comparison_root,
        regional_root=regional_root,
        regional_stats_path=args.regional_file,
        output_path=output_path,
        cmap=args.cmap,
        vmax=args.vmax,
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
