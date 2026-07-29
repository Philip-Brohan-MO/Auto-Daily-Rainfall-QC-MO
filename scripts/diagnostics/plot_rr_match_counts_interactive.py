#!/usr/bin/env python
"""Interactive map of how many ensemble files match each RR reference station.

For a single Rainfall-Rescue reference *year*, this draws every RR station-year
on a map of the UK, coloured by the number of ensemble transcription files that
are an **exact match** to that station-year. It is a diagnostic for the
metadata-matching step: a healthy year has the matches spread thinly across many
stations, whereas an "attractor" year shows a few very bright stations that have
absorbed a disproportionate share of the ensemble files.

An *exact match* is whatever the metadata-assignment step
(``assign_ensemble_metadata_parquet``) recorded as ``match_type = 'exact'`` in
the ``ensemble_metadata`` table. This diagnostic reads that decision directly
rather than re-deriving it, so it can never disagree with the assignment. Files
assigned an approximate/centroid match or no match are not counted.

Clicking a station reveals its RR label (location name, station number, year and
coordinates) and the full list of ensemble specifiers exactly matched to that
station, in a read-only box with a *Copy* button.

The exact-match decision comes from the ``ensemble_metadata`` table
(``match_type = 'exact'``); the ``similarity_matches`` table of the latest
session supplies the rank-1 ``rr_vector_id`` used to attribute each matched file
to a station, joined to ``ensemble_consensus_vectors`` for the source file name
and to ``rr_monthly_vectors`` for the station location.

The figure is written to a self-contained HTML file that can be opened in any
browser (no running kernel needed) and is also returned for inline display in a
notebook.

Example
-------
    python scripts/diagnostics/plot_rr_match_counts_interactive.py 1881 \
        --output /var/tmp/rr_match_counts_1881.html
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path
from typing import List, NamedTuple, Optional, Tuple

import duckdb

# The set of "exact" matches is taken directly from the ensemble_metadata table
# produced by assign_ensemble_metadata_parquet (cell 9 of match_metadata.ipynb),
# so the exact-match rule lives in exactly one place and this diagnostic can
# never drift from it. similarity_matches is used only to attribute each exactly
# matched ensemble file to the RR station-year it matched at rank 1.


# --------------------------------------------------------------------------- #
# Data loading
# --------------------------------------------------------------------------- #
class RRMatchCount(NamedTuple):
    """One RR reference station-year and the ensemble files that match it."""

    rr_vector_id: str
    location_name: str
    station_number: str
    year: int
    latitude: float
    longitude: float
    match_count: int
    specifiers: Tuple[str, ...] = ()


def _pdir() -> Path:
    pdir = os.getenv("PDIR")
    if not pdir:
        raise SystemExit("PDIR environment variable is not set.")
    return Path(pdir)


def _glob(root: Path, table: str) -> str:
    return str((Path(root) / table / "*.parquet").resolve())


def _specifier(file_name: str) -> str:
    """The specifier is the source file name without its .json suffix."""
    return file_name[:-5] if file_name.endswith(".json") else file_name


def load_rr_match_counts(
    *,
    comparison_root,
    year: int,
    session_id: Optional[int] = None,
) -> List[RRMatchCount]:
    """Return every located RR station-year for ``year`` with its match count.

    The count is the number of ensemble files that are an *exact match* to that
    RR station-year in the chosen session (latest by default). The exact-match
    decision is read straight from the ``ensemble_metadata`` table written by
    ``assign_ensemble_metadata_parquet`` (``match_type = 'exact'``); this
    diagnostic does not re-derive it. ``similarity_matches`` is used only to map
    each exactly matched ensemble file back to the RR station-year it matched at
    rank 1. Stations with no exact match are included with ``match_count == 0``.

    Raises ``SystemExit`` if no ``ensemble_metadata`` has been written for the
    session yet (i.e. cell 9 has not been run).
    """
    matches_glob = _glob(comparison_root, "similarity_matches")
    ensemble_glob = _glob(comparison_root, "ensemble_consensus_vectors")
    rr_glob = _glob(comparison_root, "rr_monthly_vectors")
    metadata_glob = _glob(comparison_root, "ensemble_metadata")

    if not sorted((Path(comparison_root) / "ensemble_metadata").glob("*.parquet")):
        raise SystemExit(
            "No ensemble_metadata parquet found in "
            f"{Path(comparison_root) / 'ensemble_metadata'}.\n"
            "Run the metadata-assignment step first (cell 9 of "
            "match_metadata.ipynb, assign_ensemble_metadata_parquet)."
        )

    session_clause = (
        f"{int(session_id)}"
        if session_id is not None
        else f"(SELECT MAX(session_id) FROM read_parquet('{matches_glob}'))"
    )

    query = f"""
        WITH rr AS (
            SELECT rr_vector_id, location_name, station_number, year,
                   latitude, longitude
            FROM read_parquet('{rr_glob}')
            WHERE year = {int(year)}
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
        ),
        rank1 AS (
            SELECT m.rr_vector_id, e.file_name
            FROM read_parquet('{matches_glob}') m
            JOIN read_parquet('{ensemble_glob}') e
              ON e.ensemble_vector_id = m.ensemble_vector_id
            JOIN read_parquet('{metadata_glob}') md
              ON md.file_id = e.file_id
            WHERE m.session_id = {session_clause}
              AND m.query_rank = 1
              AND md.match_type = 'exact'
              AND md.match_source_session_id = {session_clause}
        ),
        agg AS (
            SELECT rr_vector_id,
                   count(*) AS match_count,
                   list(file_name) AS file_names
            FROM rank1
            GROUP BY rr_vector_id
        )
        SELECT rr.rr_vector_id, rr.location_name, rr.station_number, rr.year,
               rr.latitude, rr.longitude,
               COALESCE(a.match_count, 0) AS match_count,
               a.file_names
        FROM rr
        LEFT JOIN agg a ON a.rr_vector_id = rr.rr_vector_id
        ORDER BY match_count ASC
    """

    conn = duckdb.connect()
    try:
        rows = conn.execute(query).fetchall()
    finally:
        conn.close()

    records: List[RRMatchCount] = []
    for rr_vector_id, name, number, yr, lat, lon, count, file_names in rows:
        specifiers = tuple(sorted(_specifier(fn) for fn in (file_names or [])))
        records.append(
            RRMatchCount(
                rr_vector_id=rr_vector_id,
                location_name=name or "",
                station_number=number or "",
                year=int(yr),
                latitude=float(lat),
                longitude=float(lon),
                match_count=int(count),
                specifiers=specifiers,
            )
        )
    return records


# JavaScript injected into the exported HTML: clicking a station shows its RR
# label and the full list of matching ensemble specifiers in a read-only box,
# with a Copy button (and a manual-select fallback for restricted clipboards).
_CLICK_TO_SHOW_JS = """
var gd = document.getElementById('{plot_id}');
var panel = document.createElement('div');
panel.style.cssText = 'font-family: sans-serif; font-size: 13px; margin: 6px 0;';
var head = document.createElement('div');
head.style.cssText = 'display: flex; align-items: center; gap: 6px; margin-bottom: 4px;';
var label = document.createElement('span');
label.style.cssText = 'flex: 1; font-weight: bold;';
label.textContent = 'Click a station to list the ensemble files matching it';
var btn = document.createElement('button');
btn.type = 'button';
btn.textContent = 'Copy list';
btn.style.cssText = 'padding: 3px 10px; cursor: pointer;';
var status = document.createElement('span');
status.style.cssText = 'color: #2a7; min-width: 70px;';
head.appendChild(label);
head.appendChild(btn);
head.appendChild(status);
var box = document.createElement('textarea');
box.readOnly = true;
box.rows = 8;
box.style.cssText = 'width: 100%; box-sizing: border-box; font-family: monospace; font-size: 12px;';
panel.appendChild(head);
panel.appendChild(box);
gd.parentNode.insertBefore(panel, gd);
function copyList() {
    if (!box.value) { return; }
    box.focus();
    box.select();
    function fallback() {
        try { document.execCommand('copy'); status.textContent = 'Copied!'; }
        catch (e) { status.textContent = 'Press Ctrl+C'; }
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(box.value)
            .then(function () { status.textContent = 'Copied!'; })
            .catch(fallback);
    } else { fallback(); }
}
btn.addEventListener('click', copyList);
gd.on('plotly_click', function (data) {
    if (!data || !data.points || !data.points.length) { return; }
    var cd = data.points[0].customdata;
    label.textContent = cd[0];
    box.value = cd[1];
    status.textContent = '';
});
"""


# --------------------------------------------------------------------------- #
# Plotting
# --------------------------------------------------------------------------- #
def build_figure(
    *,
    year: int,
    comparison_root,
    output_path: Optional[Path] = None,
    session_id: Optional[int] = None,
    cmap: str = "YlOrRd",
    marker_size: float = 9.0,
):
    """Build the interactive RR-match-count map for ``year``.

    Returns the Plotly ``Figure``. If ``output_path`` is given, a self-contained
    HTML file is also written there. Only *exact* matches are counted (see
    :func:`load_rr_match_counts`). Colour varies with sqrt(match count) so the
    many low-count stations stay distinguishable next to a few high-count ones.
    """
    import plotly.graph_objects as go

    records = load_rr_match_counts(
        comparison_root=comparison_root, year=year, session_id=session_id
    )
    if not records:
        raise SystemExit(f"No located RR reference stations found for year {year}.")

    matched = [r for r in records if r.match_count > 0]
    unmatched = [r for r in records if r.match_count == 0]
    total_matches = sum(r.match_count for r in matched)
    max_count = max((r.match_count for r in matched), default=0)

    def _label(r: RRMatchCount) -> str:
        name = r.location_name or "(unnamed)"
        number = f" [{r.station_number}]" if r.station_number else ""
        return (
            f"{name}{number}  {r.year}  "
            f"({r.latitude:.3f}, {r.longitude:.3f})  "
            f"\u2014 {r.match_count} exact match"
            f"{'' if r.match_count == 1 else 'es'}"
        )

    def _list_text(r: RRMatchCount) -> str:
        if not r.specifiers:
            return "(no ensemble files exactly match this station)"
        return "\n".join(r.specifiers)

    fig = go.Figure()

    # Zero-match RR stations: small pale-grey markers underneath.
    if unmatched:
        fig.add_trace(
            go.Scattergeo(
                lon=[r.longitude for r in unmatched],
                lat=[r.latitude for r in unmatched],
                mode="markers",
                name="no matches",
                marker=dict(
                    size=marker_size - 3,
                    color="rgb(200, 200, 200)",
                    line=dict(width=0.3, color="rgb(120,120,120)"),
                ),
                customdata=[[_label(r), _list_text(r)] for r in unmatched],
                hovertemplate="%{customdata[0]}<extra></extra>",
            )
        )

    # Matched RR stations: coloured by sqrt(match count) so low counts stay
    # visible next to a few very high-count attractor stations.
    if matched:
        sqrt_counts = [math.sqrt(r.match_count) for r in matched]
        sqrt_max = math.sqrt(max_count)

        raw_ticks = [t for t in [1, 2, 5, 10, 25, 50] if t <= max_count]
        if not raw_ticks or raw_ticks[-1] < max_count:
            raw_ticks.append(max_count)
        tickvals = [math.sqrt(t) for t in raw_ticks]
        ticktext = [f"{t:g}" for t in raw_ticks]

        fig.add_trace(
            go.Scattergeo(
                lon=[r.longitude for r in matched],
                lat=[r.latitude for r in matched],
                mode="markers",
                name="matched stations",
                marker=dict(
                    size=marker_size + 1,
                    color=sqrt_counts,
                    colorscale=cmap,
                    cmin=0.0,
                    cmax=sqrt_max,
                    line=dict(width=0.5, color="black"),
                    colorbar=dict(
                        title="Ensemble files<br>exactly matched",
                        tickvals=tickvals,
                        ticktext=ticktext,
                    ),
                ),
                customdata=[[_label(r), _list_text(r)] for r in matched],
                hovertemplate="%{customdata[0]}<extra></extra>",
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
            f"Ensemble files exactly matched per RR reference station  \u2014  {year}<br>"
            f"<sup>{len(records)} RR stations, {len(matched)} with exact matches, "
            f"{total_matches} exact matches total "
            f"(click a station to list its matches)</sup>"
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
            post_script=_CLICK_TO_SHOW_JS,
        )
    return fig


def inline_html(fig) -> str:
    """Return an HTML fragment for the figure with the click-to-list behaviour.

    Use this (via ``IPython.display.HTML``) to render the map inline in a
    notebook *with* the click-a-station-to-list-its-matches UI. The plain
    ``fig.show()`` path renders the figure but cannot run the custom JavaScript.
    """
    return fig.to_html(
        include_plotlyjs="cdn",
        full_html=False,
        post_script=_CLICK_TO_SHOW_JS,
    )


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("year", type=int, help="RR reference year to plot")
    parser.add_argument(
        "--comparison-root",
        type=Path,
        default=None,
        help="Comparison/similarity parquet root (default: $PDIR/...)",
    )
    parser.add_argument(
        "--session-id",
        type=int,
        default=None,
        help="Similarity session id to use (default: latest)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output HTML path (default: $PDIR/diagnostics/rr_match_counts_<year>.html)",
    )
    parser.add_argument("--cmap", default="YlOrRd", help="Plotly colorscale name")
    return parser.parse_args()


def _default_comparison_root() -> Path:
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
    output_path = args.output or (
        _pdir() / "diagnostics" / f"rr_match_counts_{args.year}.html"
    )

    build_figure(
        year=args.year,
        comparison_root=comparison_root,
        session_id=args.session_id,
        output_path=output_path,
        cmap=args.cmap,
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
