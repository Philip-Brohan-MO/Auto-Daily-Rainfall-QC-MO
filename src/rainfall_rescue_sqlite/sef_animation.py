"""Interpolated daily-rainfall animation frames built from SEF files.

This is the Station-Exchange-Format (SEF) counterpart to
:mod:`rainfall_rescue_sqlite.rainfall_animation`. Instead of reading consensus
values from the ensemble SQLite database, every value (and its quality-control
verdict) is read back from the exported SEF ``.tsv`` files -- i.e. the animation
is built from *exactly the data that is shared with others*, and nothing else.

Each SEF file is one station-year of daily rainfall (see
:mod:`rainfall_rescue_sqlite.sef_export`): 12 ``name<TAB>value`` header lines,
then a ``Year Month Day Hour Minute Period Value Meta`` data table. ``Value`` is
already in millimetres and each observation's ``Meta`` carries the QC verdicts
``qc1=<pass|review|fail>`` and ``qc2=<pass|fail|indeterminate|NA>``.

QC policy for the animation (as requested)
------------------------------------------
* An observation **passes** if ``qc1 == pass`` *or* the secondary check did not
  definitively reject it (``qc2 == pass`` or ``qc2 == indeterminate``). Its value
  is drawn in the interpolated rainfall field, styled like the SQLite animation
  but with a millimetre colour scale.
* An observation that **fails both** checks (``qc1 != pass`` and ``qc2 == fail``)
  is not trusted as a value: it is drawn instead as an **error marker** (a red
  cross) at the station location, and its value is excluded from the rainfall
  field.

Frame indexing, interpolation density and shard maths are shared verbatim with
:mod:`rainfall_rescue_sqlite.rainfall_animation` so the two pipelines stay in
lock-step and a SEF frame lines up with the same calendar day as an ensemble
frame.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Tuple

# Frame indexing / sharding are data-source agnostic: reuse them unchanged so the
# SEF animation and the ensemble animation share one deterministic frame plan.
from .rainfall_animation import (
    FrameSpec,
    InterpolatedPoint,
    frame_filename,
    frame_spec_for_index,
    shard_bounds,
    total_frames,
)

__all__ = [
    "FrameSpec",
    "InterpolatedPoint",
    "frame_filename",
    "frame_spec_for_index",
    "shard_bounds",
    "total_frames",
    "SEFStationDay",
    "SEFYearData",
    "parse_sef_file",
    "load_year",
    "load_day_values",
    "interpolate_frame",
    "render_points",
    "render_frame",
    "render_frame_range",
]


# --------------------------------------------------------------------------- #
# SEF parsing / data access
# --------------------------------------------------------------------------- #
_N_HEADER_LINES = 12  # SEF v1.0.0 fixed header block


class SEFStationDay(NamedTuple):
    """One station's SEF observation on a single day.

    ``value`` is the millimetre daily total (``None`` when the SEF ``Value`` is
    ``NA``). ``is_error`` is ``True`` when the observation failed both QC checks
    (``qc1 != pass`` and ``qc2 == fail``) and so must be shown as an error marker
    rather than trusted as a value. A ``qc2 == indeterminate`` verdict counts as a
    pass, so it is not an error.
    """

    station_id: str
    location_name: Optional[str]
    latitude: float
    longitude: float
    value: Optional[float]
    is_error: bool


def _parse_qc_meta(meta: str) -> Tuple[str, str]:
    """Return ``(qc1, qc2)`` verdicts from a per-observation ``Meta`` field."""
    qc1 = qc2 = "NA"
    for entry in meta.split("|"):
        key, _, val = entry.partition("=")
        if key == "qc1":
            qc1 = val
        elif key == "qc2":
            qc2 = val
    return qc1, qc2


# qc2 verdicts that are not a definitive failure. An "indeterminate" secondary
# check could not conclusively reject the value, so it is treated as a pass.
_QC2_NON_FAIL = {"pass", "indeterminate"}


def _passed(qc1: str, qc2: str) -> bool:
    """An observation passes if it passed either QC check.

    ``qc1 == pass`` is a pass. For the secondary check, both ``qc2 == pass`` and
    ``qc2 == indeterminate`` count as a pass -- only a definitive ``qc2 == fail``
    (on a day that also failed ``qc1``) marks the observation as failed-both.
    """
    return qc1 == "pass" or qc2 in _QC2_NON_FAIL


def _parse_value(text: str) -> Optional[float]:
    text = text.strip()
    if not text or text.upper() == "NA":
        return None
    try:
        return float(text)
    except ValueError:
        return None


@dataclass
class SEFYearData:
    """All SEF observations for one calendar year, indexed for animation.

    ``stations`` maps station ID to its fixed ``(name, lat, lon)``. ``by_day``
    maps ``(month, day)`` to the per-station observations on that day.
    """

    year: int
    stations: Dict[str, Tuple[Optional[str], float, float]]
    by_day: Dict[Tuple[int, int], Dict[str, SEFStationDay]]

    def day_values(self, target_day: date) -> Dict[str, SEFStationDay]:
        return self.by_day.get((target_day.month, target_day.day), {})


def parse_sef_file(
    path: Path,
) -> Tuple[Optional[Tuple[str, Optional[str], float, float]], List[Tuple[int, int, Optional[float], bool]]]:
    """Parse one SEF ``.tsv``.

    Returns ``(station, observations)`` where ``station`` is
    ``(id, name, lat, lon)`` (or ``None`` when the file has no usable position)
    and each observation is ``(month, day, value_mm, is_error)``.
    """
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    if len(lines) <= _N_HEADER_LINES:
        return None, []

    header: Dict[str, str] = {}
    for line in lines[:_N_HEADER_LINES]:
        name, _, value = line.partition("\t")
        header[name] = value

    lat = _parse_value(header.get("Lat", ""))
    lon = _parse_value(header.get("Lon", ""))
    if lat is None or lon is None:
        return None, []

    station_id = header.get("ID", path.stem)
    name = header.get("Name") or None
    if name == "NA":
        name = None

    observations: List[Tuple[int, int, Optional[float], bool]] = []
    for line in lines[_N_HEADER_LINES + 1 :]:  # skip the data-column header row
        fields = line.split("\t")
        if len(fields) < 8:
            continue
        month = int(fields[1])
        day = int(fields[2])
        value = _parse_value(fields[6])
        qc1, qc2 = _parse_qc_meta(fields[7])
        is_error = not _passed(qc1, qc2)
        observations.append((month, day, value, is_error))

    return (station_id, name, lat, lon), observations


def load_year(sef_root: Path, year: int) -> SEFYearData:
    """Load every SEF file under ``<sef_root>/tsv/<year>/`` into memory.

    A whole year is read at once so a render shard covering many days of the same
    year touches each small ``.tsv`` only once.
    """
    year_dir = Path(sef_root) / "tsv" / str(year)
    stations: Dict[str, Tuple[Optional[str], float, float]] = {}
    by_day: Dict[Tuple[int, int], Dict[str, SEFStationDay]] = {}

    if not year_dir.is_dir():
        return SEFYearData(year=year, stations=stations, by_day=by_day)

    for path in sorted(year_dir.glob("*.tsv")):
        station, observations = parse_sef_file(path)
        if station is None:
            continue
        station_id, name, lat, lon = station
        stations[station_id] = (name, lat, lon)
        for month, day, value, is_error in observations:
            by_day.setdefault((month, day), {})[station_id] = SEFStationDay(
                station_id=station_id,
                location_name=name,
                latitude=lat,
                longitude=lon,
                value=value,
                is_error=is_error,
            )

    return SEFYearData(year=year, stations=stations, by_day=by_day)


class _YearCache:
    """Cache parsed SEF years so a shard reading contiguous frames re-reads once."""

    def __init__(self, sef_root: Path, max_years: int = 2) -> None:
        self._sef_root = Path(sef_root)
        self._max_years = max_years
        self._cache: Dict[int, SEFYearData] = {}

    def year(self, year: int) -> SEFYearData:
        cached = self._cache.get(year)
        if cached is not None:
            return cached
        data = load_year(self._sef_root, year)
        if len(self._cache) >= self._max_years:
            del self._cache[next(iter(self._cache))]
        self._cache[year] = data
        return data

    def day(self, target_day: date) -> Dict[str, SEFStationDay]:
        return self.year(target_day.year).day_values(target_day)


def load_day_values(sef_root: Path, target_day: date) -> Dict[str, SEFStationDay]:
    """Return every SEF station's observation for ``target_day``.

    Convenience wrapper that loads the whole containing year; use
    :class:`_YearCache` (via :func:`render_frame_range`) to avoid re-reading a
    year for each day.
    """
    return load_year(sef_root, target_day.year).day_values(target_day)


# --------------------------------------------------------------------------- #
# Interpolation
# --------------------------------------------------------------------------- #
def interpolate_frame(
    day_a_values: Dict[str, SEFStationDay],
    day_b_values: Dict[str, SEFStationDay],
    step: float,
) -> Tuple[List[InterpolatedPoint], List[Tuple[float, float]]]:
    """Split two days' SEF observations into a value field and error markers.

    Returns ``(good_points, error_points)``:

    * ``good_points`` -- interpolated ``(lat, lon, value_mm)`` for stations whose
      value is trusted. A day's value only contributes when that day's
      observation passed QC; a missing, absent or failed-both value is treated as
      ``0.0`` so stations fade in and out rather than popping.
    * ``error_points`` -- ``(lat, lon)`` for stations whose observation on
      ``day_a`` (the labelled day) failed both QC checks. These are shown as error
      markers and excluded from the value field.
    """
    keys = set(day_a_values) | set(day_b_values)
    good: List[InterpolatedPoint] = []
    errors: List[Tuple[float, float]] = []

    for key in keys:
        a = day_a_values.get(key)
        b = day_b_values.get(key)
        anchor = a or b
        if anchor is None:  # pragma: no cover - key came from one of the dicts
            continue

        if a is not None and a.is_error:
            errors.append((a.latitude, a.longitude))
            continue

        va = a.value if (a is not None and not a.is_error and a.value is not None) else 0.0
        vb = b.value if (b is not None and not b.is_error and b.value is not None) else 0.0
        value = va + (vb - va) * step
        good.append(InterpolatedPoint(anchor.latitude, anchor.longitude, value))

    return good, errors


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #
def render_points(
    *,
    good_points: List[InterpolatedPoint],
    error_points: List[Tuple[float, float]],
    label_date: date,
    output_path: Path,
    cmap: str = "YlGnBu",
    vmax: float = 50.0,
    marker_size: float = 9.0,
    error_color: str = "#d62728",
    subtitle: Optional[str] = None,
) -> Path:
    """Draw one SEF animation frame: value field plus QC error markers.

    Styling matches ``rainfall_animation.render_points`` (tall UK framing,
    square-root colour scale, coastlines and borders) but the colour scale is in
    **millimetres** and stations that failed both QC checks are overplotted as red
    crosses.
    """
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure
    from cartopy import crs as ccrs
    from cartopy import feature as cfeature

    vmax = max(float(vmax), 1e-6)
    sqrt_vmax = math.sqrt(vmax)
    lats = [p.latitude for p in good_points]
    lons = [p.longitude for p in good_points]
    sqrt_values = [math.sqrt(min(max(p.value, 0.0), vmax)) for p in good_points]

    raw_ticks = [t for t in [0, 1, 2, 5, 10, 25, 50] if t <= vmax]
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
    ax.set_aspect("auto")

    marker_area = (marker_size + 1) ** 2
    scatter = ax.scatter(
        lons, lats, c=sqrt_values, cmap=cmap, vmin=0.0, vmax=sqrt_vmax,
        s=marker_area, edgecolor="black", linewidth=0.5,
        transform=ccrs.PlateCarree(), zorder=20,
    )

    if error_points:
        err_lats = [lat for lat, _ in error_points]
        err_lons = [lon for _, lon in error_points]
        ax.scatter(
            err_lons, err_lats, marker="x", c=error_color,
            s=((marker_size + 3) ** 2) / 2, linewidth=1.6,
            transform=ccrs.PlateCarree(), zorder=30,
            label="failed both QC",
        )

    if subtitle is None:
        subtitle = f"{len(good_points)} stations passed QC · {len(error_points)} failed both"
    ax.set_title(
        f"Shared (SEF) daily rainfall  {label_date.isoformat()}\n{subtitle}",
        fontsize=14,
    )

    cax = fig.add_axes([0.87, 0.10, 0.03, 0.80])
    cbar = fig.colorbar(scatter, cax=cax, extend="max")
    cbar.set_label("Daily rainfall (mm)")
    cbar.set_ticks(colorbar_tickvals)
    cbar.set_ticklabels(colorbar_ticktext)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path))
    return output_path


def render_frame(
    *,
    sef_root: Path,
    spec: FrameSpec,
    output_path: Path,
    cmap: str = "YlGnBu",
    vmax: float = 50.0,
    marker_size: float = 9.0,
    error_color: str = "#d62728",
    cache: Optional[_YearCache] = None,
) -> Path:
    """Load, interpolate and render a single :class:`FrameSpec` from SEF files."""
    if cache is None:
        cache = _YearCache(sef_root)
    day_a_values = cache.day(spec.day_a)
    day_b_values = cache.day(spec.day_b) if spec.day_b != spec.day_a else day_a_values
    good, errors = interpolate_frame(day_a_values, day_b_values, spec.step)

    return render_points(
        good_points=good,
        error_points=errors,
        label_date=spec.day_a,
        output_path=output_path,
        cmap=cmap,
        vmax=vmax,
        marker_size=marker_size,
        error_color=error_color,
    )


def render_frame_range(
    *,
    sef_root: Path,
    start_date: date,
    end_date: date,
    frames_per_day: int,
    first_index: int,
    last_index: int,
    output_dir: Path,
    cmap: str = "YlGnBu",
    vmax: float = 50.0,
    marker_size: float = 9.0,
    error_color: str = "#d62728",
) -> List[Path]:
    """Render the contiguous frame indices ``[first_index, last_index]`` from SEF.

    A per-task year cache means each calendar year in the slice is parsed from its
    SEF files only once. Returns the list of written frame paths.
    """
    cache = _YearCache(sef_root)
    written: List[Path] = []
    for global_index in range(first_index, last_index + 1):
        spec = frame_spec_for_index(
            global_index, start_date, end_date, frames_per_day
        )
        out = Path(output_dir) / frame_filename(global_index)
        render_frame(
            sef_root=sef_root,
            spec=spec,
            output_path=out,
            cmap=cmap,
            vmax=vmax,
            marker_size=marker_size,
            error_color=error_color,
            cache=cache,
        )
        written.append(out)
    return written
