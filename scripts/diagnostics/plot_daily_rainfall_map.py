#!/usr/bin/env python
"""Map of consensus daily rainfall across all located stations for one date.

Given a calendar date ``YYYY-MM-DD`` this finds every ensemble transcription that

* has been assigned a Rainfall-Rescue year matching ``YYYY`` (``matched_year``),
  and
* has an assigned location (``matched_latitude`` / ``matched_longitude``),

computes the per-station **consensus** daily rainfall for that day (the median
over the 5 ensemble members) and plots each station on a map of the UK, coloured
by its rainfall value.

All data comes from ``ensemble_transcriptions.sqlite`` (built under ``$PDIR``),
using the metadata written by ``assign_ensemble_metadata``.

Example
-------
    python scripts/diagnostics/plot_daily_rainfall_map.py 1903-10-15 \
        --output /var/tmp/daily_map.webp
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import date
from pathlib import Path
from statistics import median
from typing import List, NamedTuple, Optional, Tuple


# --------------------------------------------------------------------------- #
# Data access
# --------------------------------------------------------------------------- #
def _pdir() -> Path:
    pdir = os.getenv("PDIR")
    if not pdir:
        raise SystemExit("PDIR environment variable is not set.")
    return Path(pdir)


def _connect_immutable(path: Path) -> sqlite3.Connection:
    """Open a SQLite DB read-only (works on shared cluster filesystems)."""
    conn = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


class DailyRecord(NamedTuple):
    """One located station's consensus rainfall for a date.

    ``value`` is ``None`` when the station has no rainfall value for the day.
    ``file_name`` is the ensemble transcription's specifier (its source file).
    """

    file_name: str
    location_name: Optional[str]
    latitude: float
    longitude: float
    value: Optional[float]


def load_daily_records_for_date(
    ensemble_db: Path, year: int, month: int, day: int
) -> List[DailyRecord]:
    """Return a ``DailyRecord`` for every located station on one date.

    Only ensemble files whose ``matched_year`` equals ``year`` and which carry an
    assigned latitude/longitude are considered. The consensus is the median over
    the ensemble members present for that day-of-month / month cell; a station
    with no value for the day has ``value=None``.

    The lookup is done in two steps so it can drive from the (small) set of
    located files for the year and then seek the daily values by the
    ``(file_id, day_of_month, month)`` primary-key prefix. A single JOIN lets the
    planner scan every file's values for the day/month instead, which is far
    slower on the 80M-row daily table.
    """
    with _connect_immutable(ensemble_db) as conn:
        located = conn.execute(
            """
            SELECT file_id, file_name, matched_location_name,
                   matched_latitude, matched_longitude
            FROM ensemble_files
            WHERE matched_year = ?
              AND matched_latitude IS NOT NULL
              AND matched_longitude IS NOT NULL
            """,
            (year,),
        ).fetchall()
        if not located:
            return []

        records: List[DailyRecord] = []
        for r in located:
            file_id = int(r["file_id"])
            lat = float(r["matched_latitude"])
            lon = float(r["matched_longitude"])
            # A single-file equality lets the planner use the daily-values
            # primary key (file_id, day_of_month, month, ...) directly.
            values = [
                float(v[0])
                for v in conn.execute(
                    """
                    SELECT rainfall
                    FROM ensemble_daily_values
                    WHERE file_id = ?
                      AND day_of_month = ?
                      AND month = ?
                      AND rainfall IS NOT NULL
                    """,
                    (file_id, day, month),
                )
            ]
            records.append(
                DailyRecord(
                    file_name=str(r["file_name"]),
                    location_name=r["matched_location_name"],
                    latitude=lat,
                    longitude=lon,
                    value=float(median(values)) if values else None,
                )
            )
    return records


def load_daily_rainfall_for_date(
    ensemble_db: Path, year: int, month: int, day: int
) -> Tuple[List[Tuple[float, float, float]], List[Tuple[float, float]]]:
    """Return located stations for one date, split into valued and null.

    The result is ``(points, null_points)`` where

    * ``points`` is ``[(latitude, longitude, consensus_rainfall), ...]`` for
      stations that have at least one ensemble member value for the day, and
    * ``null_points`` is ``[(latitude, longitude), ...]`` for located stations
      that carry no rainfall value for the day.
    """
    points: List[Tuple[float, float, float]] = []
    null_points: List[Tuple[float, float]] = []
    for rec in load_daily_records_for_date(ensemble_db, year, month, day):
        if rec.value is None:
            null_points.append((rec.latitude, rec.longitude))
        else:
            points.append((rec.latitude, rec.longitude, rec.value))
    return points, null_points


# --------------------------------------------------------------------------- #
# Plotting
# --------------------------------------------------------------------------- #
def build_figure(
    *,
    target_date: date,
    ensemble_db: Path,
    output_path: Path,
    cmap: str = "YlGnBu",
    vmax: float = 2.0,
    marker_size: float = 9.0,
) -> Path:
    """Render the UK daily-rainfall map for ``target_date`` and save it."""
    import math

    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure
    from cartopy import crs as ccrs
    from cartopy import feature as cfeature

    records = load_daily_records_for_date(
        ensemble_db, target_date.year, target_date.month, target_date.day
    )
    if not records:
        raise SystemExit(
            f"No located ensemble records found for {target_date.isoformat()}."
        )

    lats = [r.latitude for r in records]
    lons = [r.longitude for r in records]
    # Match interactive map behaviour: null values are shown at the dry (zero) end.
    values = [r.value if r.value is not None else 0.0 for r in records]

    vmax = max(float(vmax), 1e-6)
    sqrt_values = [math.sqrt(min(max(v, 0.0), vmax)) for v in values]
    sqrt_vmax = math.sqrt(vmax)

    raw_ticks = [t for t in [0, 0.25, 0.5, 1.0, 1.5, 2.0] if t <= vmax]
    if not raw_ticks or raw_ticks[-1] < vmax:
        raw_ticks.append(vmax)
    colorbar_tickvals = [math.sqrt(t) for t in raw_ticks]
    colorbar_ticktext = [f"{t:g}" for t in raw_ticks]

    fig = Figure(figsize=(8, 10), dpi=100, facecolor="white")
    FigureCanvas(fig)

    ax = fig.add_axes([0.02, 0.06, 0.82, 0.88], projection=ccrs.PlateCarree())
    ax.set_extent([-11, 4, 49, 61], crs=ccrs.PlateCarree())
    ax.set_facecolor((230 / 255, 240 / 255, 250 / 255))

    ax.add_feature(
        cfeature.LAND.with_scale("50m"),
        facecolor=(243 / 255, 243 / 255, 243 / 255),
        edgecolor="none",
        zorder=1,
    )
    ax.coastlines(resolution="50m", linewidth=0.9, color="black", zorder=3)
    ax.add_feature(
        cfeature.BORDERS.with_scale("50m"), edgecolor="black", linewidth=0.6, zorder=3
    )

    ax.set_xticks([])
    ax.set_yticks([])
    # Match the interactive map's tall visual framing.
    ax.set_aspect("auto")

    # Matplotlib scatter expects marker area (points^2), while Plotly uses an
    # approximate marker diameter in pixels. Convert so visual sizes match.
    marker_area = (marker_size + 1) ** 2

    scatter = ax.scatter(
        lons, lats, c=sqrt_values, cmap=cmap, vmin=0.0, vmax=sqrt_vmax,
        s=marker_area, edgecolor="black", linewidth=0.5,
        transform=ccrs.PlateCarree(), zorder=20,
    )

    ax.set_title(
        f"Consensus daily rainfall  {target_date.isoformat()}\n"
        f"{len(records)} located stations",
        fontsize=14,
    )

    cax = fig.add_axes([0.87, 0.10, 0.03, 0.80])
    cbar = fig.colorbar(scatter, cax=cax, extend="max")
    cbar.set_label("Consensus daily rainfall (in)")
    cbar.set_ticks(colorbar_tickvals)
    cbar.set_ticklabels(colorbar_ticktext)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path))
    return output_path


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
        help="Output image path (default: $PDIR/diagnostics/daily_map_<date>.webp)",
    )
    parser.add_argument("--cmap", default="YlGnBu", help="Matplotlib colormap name")
    parser.add_argument(
        "--vmax",
        type=float,
        default=None,
        help="Upper colour limit in inches (default: 2.0)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensemble_db = args.ensemble_db or (_pdir() / "ensemble_transcriptions.sqlite")
    output_path = args.output or (
        _pdir() / "diagnostics" / f"daily_map_{args.date.isoformat()}.webp"
    )

    saved = build_figure(
        target_date=args.date,
        ensemble_db=ensemble_db,
        output_path=output_path,
        cmap=args.cmap,
        vmax=args.vmax,
    )
    print(f"Wrote {saved}")


if __name__ == "__main__":
    main()
