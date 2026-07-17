"""Interpolated daily-rainfall animation frames.

This module turns the per-day consensus rainfall maps (see
``scripts/diagnostics/plot_daily_rainfall_map.py``) into a smooth animation by
generating extra frames that linearly interpolate each station's value between
consecutive calendar days.

The work splits cleanly into three concerns so it can be parallelised on a
cluster (see the ``scripts/render_*`` entrypoints and ``scripts/slurm``):

* **Frame indexing** -- a deterministic mapping from a global frame index to a
  ``(day_offset, step)`` pair, so any subset of frames can be rendered
  independently and reassembled in order.
* **Data loading** -- station values for the two calendar days that bracket a
  frame, with a small cache so an array task rendering a contiguous slice only
  reads each day once.
* **Rendering** -- draws one frame with exactly the same map styling as the
  static daily map, so animation frames and standalone maps look identical.

Missing values follow the "interpolate to/from zero" policy: a station with no
value on a given day is treated as ``0.0`` when interpolating, so it fades in
and out of the wet colours rather than popping.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from statistics import median
from typing import Dict, List, NamedTuple, Optional, Tuple


# --------------------------------------------------------------------------- #
# Data access
# --------------------------------------------------------------------------- #
class StationDay(NamedTuple):
    """One station's consensus value on a single day.

    ``value`` is ``None`` when the station has no rainfall value for the day.
    """

    file_name: str
    location_name: Optional[str]
    latitude: float
    longitude: float
    value: Optional[float]


def _connect_immutable(path: Path) -> sqlite3.Connection:
    """Open a SQLite DB read-only (works on shared cluster filesystems)."""
    conn = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_year_records(
    ensemble_db: Path, year: int
) -> Dict[str, StationDay]:
    """Return the located stations for ``year`` keyed by ``file_name``.

    Station positions are fixed for the year, so this pre-loads the located
    set once; per-day values are filled in by :func:`load_day_values`.
    """
    with _connect_immutable(ensemble_db) as conn:
        located = conn.execute(
            """
            SELECT file_name, matched_location_name,
                   matched_latitude, matched_longitude
            FROM ensemble_files
            WHERE matched_year = ?
              AND matched_latitude IS NOT NULL
              AND matched_longitude IS NOT NULL
            """,
            (year,),
        ).fetchall()

    stations: Dict[str, StationDay] = {}
    for r in located:
        stations[str(r["file_name"])] = StationDay(
            file_name=str(r["file_name"]),
            location_name=r["matched_location_name"],
            latitude=float(r["matched_latitude"]),
            longitude=float(r["matched_longitude"]),
            value=None,
        )
    return stations


def load_day_values(ensemble_db: Path, target_day: date) -> Dict[str, StationDay]:
    """Return every located station's consensus value for ``target_day``.

    The consensus is the median over the ensemble members present for the
    ``(file_id, day_of_month, month)`` cell. Stations located for the day's
    year but with no value that day carry ``value=None``.
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
            (target_day.year,),
        ).fetchall()

        stations: Dict[str, StationDay] = {}
        for r in located:
            file_id = int(r["file_id"])
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
                    (file_id, target_day.day, target_day.month),
                )
            ]
            stations[str(r["file_name"])] = StationDay(
                file_name=str(r["file_name"]),
                location_name=r["matched_location_name"],
                latitude=float(r["matched_latitude"]),
                longitude=float(r["matched_longitude"]),
                value=float(median(values)) if values else None,
            )
    return stations


# --------------------------------------------------------------------------- #
# Frame indexing
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FrameSpec:
    """A single output frame in a deterministic global sequence.

    ``day_a`` and ``day_b`` are the bracketing calendar days and ``step`` is the
    interpolation fraction in ``[0, 1)`` between them (``0`` = exactly ``day_a``).
    ``global_index`` is the frame's position in the full animation.
    """

    global_index: int
    day_a: date
    day_b: date
    step: float


def total_frames(start_date: date, end_date: date, frames_per_day: int) -> int:
    """Return the number of frames for ``[start_date, end_date]`` inclusive.

    ``frames_per_day`` frames cover each day-to-next-day transition; the final
    day contributes a single (non-interpolated) frame so the animation ends
    exactly on ``end_date``.
    """
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    if frames_per_day < 1:
        raise ValueError("frames_per_day must be >= 1")
    day_span = (end_date - start_date).days
    return day_span * frames_per_day + 1


def frame_spec_for_index(
    global_index: int,
    start_date: date,
    end_date: date,
    frames_per_day: int,
) -> FrameSpec:
    """Map a global frame index to its bracketing days and interpolation step."""
    n_frames = total_frames(start_date, end_date, frames_per_day)
    if global_index < 0 or global_index >= n_frames:
        raise IndexError(
            f"frame index {global_index} out of range [0, {n_frames})"
        )

    day_offset = global_index // frames_per_day
    step_within_day = global_index % frames_per_day
    day_a = start_date + timedelta(days=day_offset)

    # The very last frame sits exactly on end_date with no successor day.
    if day_a >= end_date:
        return FrameSpec(global_index, end_date, end_date, 0.0)

    day_b = day_a + timedelta(days=1)
    step = step_within_day / frames_per_day
    return FrameSpec(global_index, day_a, day_b, step)


def frame_filename(global_index: int) -> str:
    """Deterministic, zero-padded frame filename for ffmpeg's numeric ordering."""
    return f"frame_{global_index:07d}.png"


# --------------------------------------------------------------------------- #
# Interpolation
# --------------------------------------------------------------------------- #
class InterpolatedPoint(NamedTuple):
    latitude: float
    longitude: float
    value: float


def interpolate_frame(
    day_a_values: Dict[str, StationDay],
    day_b_values: Dict[str, StationDay],
    step: float,
) -> List[InterpolatedPoint]:
    """Linearly interpolate station values between two days.

    Missing values (``None``) are treated as ``0.0`` (interpolate to/from zero),
    so a station present on only one of the two days fades in or out rather than
    appearing or vanishing abruptly. The union of both days' stations is used so
    coverage changes are handled smoothly.
    """
    keys = set(day_a_values) | set(day_b_values)
    points: List[InterpolatedPoint] = []
    for key in keys:
        a = day_a_values.get(key)
        b = day_b_values.get(key)
        anchor = a or b
        if anchor is None:  # pragma: no cover - key came from one of the dicts
            continue
        va = 0.0 if (a is None or a.value is None) else a.value
        vb = 0.0 if (b is None or b.value is None) else b.value
        value = va + (vb - va) * step
        points.append(
            InterpolatedPoint(anchor.latitude, anchor.longitude, value)
        )
    return points


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #
def render_points(
    *,
    points: List[InterpolatedPoint],
    label_date: date,
    output_path: Path,
    cmap: str = "YlGnBu",
    vmax: float = 2.0,
    marker_size: float = 9.0,
    subtitle: Optional[str] = None,
) -> Path:
    """Draw one animation frame with the static daily-map styling.

    ``points`` are pre-interpolated ``(lat, lon, value)`` stations. ``label_date``
    is shown in the title; ``subtitle`` (e.g. an interpolation note) is appended
    when given. The styling here is kept identical to
    ``plot_daily_rainfall_map.build_figure`` so frames match standalone maps.
    """
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure
    from cartopy import crs as ccrs
    from cartopy import feature as cfeature

    vmax = max(float(vmax), 1e-6)
    sqrt_vmax = math.sqrt(vmax)
    lats = [p.latitude for p in points]
    lons = [p.longitude for p in points]
    sqrt_values = [math.sqrt(min(max(p.value, 0.0), vmax)) for p in points]

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
    ax.set_aspect("auto")

    marker_area = (marker_size + 1) ** 2
    scatter = ax.scatter(
        lons, lats, c=sqrt_values, cmap=cmap, vmin=0.0, vmax=sqrt_vmax,
        s=marker_area, edgecolor="black", linewidth=0.5,
        transform=ccrs.PlateCarree(), zorder=20,
    )

    title = f"Consensus daily rainfall  {label_date.isoformat()}"
    if subtitle:
        title += f"\n{subtitle}"
    else:
        title += f"\n{len(points)} located stations"
    ax.set_title(title, fontsize=14)

    cax = fig.add_axes([0.87, 0.10, 0.03, 0.80])
    cbar = fig.colorbar(scatter, cax=cax, extend="max")
    cbar.set_label("Consensus daily rainfall (in)")
    cbar.set_ticks(colorbar_tickvals)
    cbar.set_ticklabels(colorbar_ticktext)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path))
    return output_path


class _DayValueCache:
    """Small LRU-ish cache so a shard reading contiguous frames re-reads a day once."""

    def __init__(self, ensemble_db: Path, max_days: int = 4) -> None:
        self._ensemble_db = ensemble_db
        self._max_days = max_days
        self._cache: Dict[date, Dict[str, StationDay]] = {}

    def get(self, day: date) -> Dict[str, StationDay]:
        cached = self._cache.get(day)
        if cached is not None:
            return cached
        values = load_day_values(self._ensemble_db, day)
        if len(self._cache) >= self._max_days:
            # Drop an arbitrary oldest-ish entry; insertion order in dict.
            oldest = next(iter(self._cache))
            del self._cache[oldest]
        self._cache[day] = values
        return values


def render_frame(
    *,
    ensemble_db: Path,
    spec: FrameSpec,
    output_path: Path,
    cmap: str = "YlGnBu",
    vmax: float = 2.0,
    marker_size: float = 9.0,
    cache: Optional[_DayValueCache] = None,
) -> Path:
    """Load, interpolate and render a single :class:`FrameSpec`."""
    if cache is None:
        cache = _DayValueCache(ensemble_db)
    day_a_values = cache.get(spec.day_a)
    day_b_values = cache.get(spec.day_b) if spec.day_b != spec.day_a else day_a_values
    points = interpolate_frame(day_a_values, day_b_values, spec.step)

    return render_points(
        points=points,
        label_date=spec.day_a,
        output_path=output_path,
        cmap=cmap,
        vmax=vmax,
        marker_size=marker_size,
    )


def render_frame_range(
    *,
    ensemble_db: Path,
    start_date: date,
    end_date: date,
    frames_per_day: int,
    first_index: int,
    last_index: int,
    output_dir: Path,
    cmap: str = "YlGnBu",
    vmax: float = 2.0,
    marker_size: float = 9.0,
) -> List[Path]:
    """Render the contiguous frame indices ``[first_index, last_index]``.

    Used by a SLURM array task to render just its slice. A per-task day-value
    cache means each calendar day in the slice is read from SQLite only once.
    Returns the list of written frame paths.
    """
    cache = _DayValueCache(ensemble_db)
    written: List[Path] = []
    for global_index in range(first_index, last_index + 1):
        spec = frame_spec_for_index(
            global_index, start_date, end_date, frames_per_day
        )
        out = Path(output_dir) / frame_filename(global_index)
        render_frame(
            ensemble_db=ensemble_db,
            spec=spec,
            output_path=out,
            cmap=cmap,
            vmax=vmax,
            marker_size=marker_size,
            cache=cache,
        )
        written.append(out)
    return written


# --------------------------------------------------------------------------- #
# Sharding helpers
# --------------------------------------------------------------------------- #
def shard_bounds(
    total: int, num_shards: int, shard_index: int
) -> Tuple[int, int]:
    """Return the inclusive ``(first, last)`` frame indices for one shard.

    Frames are split into ``num_shards`` contiguous, near-equal blocks. If a
    shard has no frames (more shards than frames) it returns an empty range
    ``(first, first - 1)``.
    """
    if num_shards < 1:
        raise ValueError("num_shards must be >= 1")
    if shard_index < 0 or shard_index >= num_shards:
        raise IndexError("shard_index out of range")
    base, extra = divmod(total, num_shards)
    # First ``extra`` shards get one more frame.
    if shard_index < extra:
        first = shard_index * (base + 1)
        count = base + 1
    else:
        first = extra * (base + 1) + (shard_index - extra) * base
        count = base
    last = first + count - 1
    return first, last
