#!/usr/bin/env python
"""Interactive map of consensus daily rainfall for one date.

This is the interactive companion to ``plot_daily_rainfall_map.py``. It draws the
same picture -- every located station's consensus daily rainfall on a map of the
UK -- but as a Plotly figure. Hovering or clicking on a point reveals the
**specifier** (the ensemble transcription's source file name) the data comes
from, along with the station name and rainfall value.

Located stations with no value for the day are drawn as pale grey circles
underneath the coloured (valued) points, so a coloured point can obscure a null
one but not the other way around.

The figure is written to a self-contained HTML file that can be opened in any
browser (no running kernel needed) and is also returned for inline display in a
notebook.

Example
-------
    python scripts/diagnostics/plot_daily_rainfall_interactive.py 1931-10-15 \
        --output /var/tmp/daily_map_interactive.html
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional

# Allow importing the sibling module whether run as a script or loaded by path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from plot_daily_rainfall_map import (  # noqa: E402
    DailyRecord,
    load_daily_records_for_date,
)


def _pdir() -> Path:
    pdir = os.getenv("PDIR")
    if not pdir:
        raise SystemExit("PDIR environment variable is not set.")
    return Path(pdir)

# JavaScript injected into the exported HTML: clicking a station copies its
# specifier to the clipboard and shows it in a read-only box (with a Copy button
# and a manual-select fallback for browsers/iframes that block clipboard access).
_CLICK_TO_COPY_JS = """
var gd = document.getElementById('{plot_id}');
var bar = document.createElement('div');
bar.style.cssText = 'font-family: sans-serif; font-size: 13px; margin: 6px 0; display: flex; align-items: center; gap: 6px;';
var label = document.createElement('span');
label.textContent = 'Specifier:';
var input = document.createElement('input');
input.type = 'text';
input.readOnly = true;
input.style.cssText = 'flex: 1; min-width: 300px; padding: 3px 6px; font-family: monospace;';
input.placeholder = 'Click a station to copy its specifier';
var btn = document.createElement('button');
btn.type = 'button';
btn.textContent = 'Copy';
btn.style.cssText = 'padding: 3px 10px; cursor: pointer;';
var status = document.createElement('span');
status.style.cssText = 'color: #2a7; min-width: 90px;';
bar.appendChild(label);
bar.appendChild(input);
bar.appendChild(btn);
bar.appendChild(status);
gd.parentNode.insertBefore(bar, gd);
function copySpecifier() {
    if (!input.value) { return; }
    input.focus();
    input.select();
    function fallback() {
        try {
            document.execCommand('copy');
            status.textContent = 'Copied!';
        } catch (e) {
            status.textContent = 'Press Ctrl+C';
        }
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(input.value)
            .then(function () { status.textContent = 'Copied!'; })
            .catch(fallback);
    } else {
        fallback();
    }
}
btn.addEventListener('click', copySpecifier);
gd.on('plotly_click', function (data) {
    if (!data || !data.points || !data.points.length) { return; }
    var specifier = data.points[0].customdata[0];
    input.value = specifier;
    status.textContent = '';
    copySpecifier();
});
"""# --------------------------------------------------------------------------- #
# Plotting
# --------------------------------------------------------------------------- #
def build_figure(
    *,
    target_date: date,
    ensemble_db: Path,
    output_path: Optional[Path] = None,
    cmap: str = "YlGnBu",
    vmax: float = 2.0,
    marker_size: float = 9.0,
):
    """Build the interactive UK daily-rainfall map for ``target_date``.

    Returns the Plotly ``Figure``. If ``output_path`` is given, a self-contained
    HTML file is also written there.

    ``vmax`` sets the upper end of the colour scale in mm (default 2.0). Values
    above ``vmax`` are clipped to ``vmax`` so they show as the darkest colour.
    Colour varies with sqrt(rainfall) for better discrimination at low values.
    """
    import math

    import plotly.graph_objects as go

    def _specifier(file_name: str) -> str:
        """The specifier is the source file name without its .json suffix."""
        return file_name[:-5] if file_name.endswith(".json") else file_name

    records: List[DailyRecord] = load_daily_records_for_date(
        ensemble_db, target_date.year, target_date.month, target_date.day
    )
    if not records:
        raise SystemExit(
            f"No located ensemble records found for {target_date.isoformat()}."
        )

    # Treat null values as zero: all located stations are plotted on the colour
    # scale, with null stations shown at the zero (dry) end.
    all_records = records
    values = [r.value if r.value is not None else 0.0 for r in all_records]
    null_mask = [r.value is None for r in all_records]

    vmax = max(float(vmax), 1e-6)

    # Apply square-root transform for perceptually uniform colour spacing.
    # Values above vmax are clipped before the transform so they saturate at
    # the top of the colour scale rather than going off-scale.
    sqrt_values = [math.sqrt(min(max(v, 0.0), vmax)) for v in values]
    sqrt_vmax = math.sqrt(vmax)

    # Fixed tick positions in original mm space, capped at vmax.
    _raw_ticks = [t for t in [0, 0.25, 0.5, 1.0, 1.5, 2.0] if t <= vmax]
    if not _raw_ticks or _raw_ticks[-1] < vmax:
        _raw_ticks.append(vmax)
    colorbar_tickvals = [math.sqrt(t) for t in _raw_ticks]
    colorbar_ticktext = [f"{t:g}" for t in _raw_ticks]

    fig = go.Figure()

    if all_records:
        fig.add_trace(
            go.Scattergeo(
                lon=[r.longitude for r in all_records],
                lat=[r.latitude for r in all_records],
                mode="markers",
                name="consensus rainfall",
                marker=dict(
                    size=marker_size + 1,
                    color=sqrt_values,
                    colorscale=cmap,
                    cmin=0.0,
                    cmax=sqrt_vmax,
                    line=dict(width=0.5, color="black"),
                    colorbar=dict(
                        title="Consensus<br>daily rainfall (in)",
                        tickvals=colorbar_tickvals,
                        ticktext=colorbar_ticktext,
                    ),
                ),
                customdata=[
                    [
                        _specifier(r.file_name),
                        r.location_name or "",
                        "no value for this date" if is_null else f"{r.value:.2f} in",
                    ]
                    for r, is_null in zip(all_records, null_mask)
                ],
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "%{customdata[1]}<br>"
                    "%{customdata[2]}"
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
            f"Consensus daily rainfall  {target_date.isoformat()}<br>"
            f"<sup>{len(all_records)} located stations "
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


def inline_html(fig) -> str:
    """Return an HTML fragment for the figure with the click-to-copy behaviour.

    Use this (via ``IPython.display.HTML``) to render the map inline in a
    notebook *with* the click-a-station-to-copy-its-specifier UI. The plain
    ``fig.show()`` path renders the figure but cannot run the custom
    click-to-copy JavaScript.
    """
    return fig.to_html(
        include_plotlyjs="cdn",
        full_html=False,
        post_script=_CLICK_TO_COPY_JS,
    )


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
        "--ensemble-db",
        type=Path,
        default=None,
        help="Path to ensemble_transcriptions.sqlite (default: $PDIR/...)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output HTML path (default: $PDIR/diagnostics/daily_map_<date>.html)",
    )
    parser.add_argument("--cmap", default="YlGnBu", help="Plotly colorscale name")
    parser.add_argument(
        "--vmax",
        type=float,
        default=None,
        help="Upper colour limit (default: 98th percentile of the day's values)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensemble_db = args.ensemble_db or (_pdir() / "ensemble_transcriptions.sqlite")
    output_path = args.output or (
        _pdir() / "diagnostics" / f"daily_map_{args.date.isoformat()}.html"
    )

    build_figure(
        target_date=args.date,
        ensemble_db=ensemble_db,
        output_path=output_path,
        cmap=args.cmap,
        vmax=args.vmax,
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
