"""Convert the exported SEF ``.tsv`` files into a compact Parquet analysis set.

The final deliverable of the pipeline is the Station-Exchange-Format (SEF) export
(:mod:`rainfall_rescue_sqlite.sef_export`). Before shipping it we want summary
figures computed *only* from those SEF files, to show the data is useful and to
surface hidden bugs. Reading the raw ``.tsv`` tree for every figure is slow, so
this module parses each SEF file **once** into two Parquet tables that the
analysis notebook then queries cheaply with DuckDB:

``observations/year=<Y>.parquet``
    One row per station-day: coordinates, the millimetre daily total, the raw QC
    verdicts and a ``passed`` flag (the observation meets the QC criteria to be
    trusted -- ``qc1 == pass`` or ``qc2 in {pass, indeterminate}``, matching the
    animation policy).

``daily_national/year=<Y>.parquet``
    One row per calendar date: the national (station-mean) daily rainfall, the
    reporting-station count and wet-/extreme-day tallies. Because every date
    belongs to exactly one year, each year's aggregate is disjoint, so the whole
    dataset is built one year per SLURM task with **no merge stage**.

Parsing reuses the SEF reader helpers from
:mod:`rainfall_rescue_sqlite.sef_animation` so the two pipelines agree on the QC
semantics byte-for-byte.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Dict, Iterator, List, NamedTuple, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

from .sef_animation import (
    _N_HEADER_LINES,
    _parse_qc_meta,
    _parse_value,
    _passed,
)

__all__ = [
    "WET_THRESHOLD_MM",
    "EXTREME_THRESHOLDS_MM",
    "SEFObservation",
    "iter_file_observations",
    "iter_year_observations",
    "discover_years",
    "build_year_parquet",
    "default_analysis_root",
    "default_sef_root",
]


# A "wet day" is one with at least 1 mm of rain (the standard threshold). The
# extreme-day tallies count station-days at or above each of these millimetre
# levels; the notebook reuses these so its figures match the precomputed columns.
WET_THRESHOLD_MM = 1.0
EXTREME_THRESHOLDS_MM = (10.0, 25.0, 50.0)


class SEFObservation(NamedTuple):
    """One station's SEF observation on a single day, with its raw QC verdicts.

    ``value_mm`` is ``None`` when the SEF ``Value`` is ``NA``. ``passed`` is
    ``True`` when the observation meets the QC criteria to be trusted as a value
    (``qc1 == pass`` or ``qc2 in {pass, indeterminate}``).
    """

    station_id: str
    location_name: Optional[str]
    latitude: float
    longitude: float
    altitude: Optional[float]
    year: int
    month: int
    day: int
    value_mm: Optional[float]
    qc1: str
    qc2: str
    passed: bool


def iter_file_observations(path: Path) -> Iterator[SEFObservation]:
    """Yield a :class:`SEFObservation` for every data row in one SEF ``.tsv``.

    Files without a usable ``Lat``/``Lon`` header, and rows with an out-of-range
    calendar date, are skipped. Parsing mirrors
    :func:`rainfall_rescue_sqlite.sef_animation.parse_sef_file` but also keeps the
    raw ``qc1``/``qc2`` verdicts and the station altitude.
    """
    path = Path(path)
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) <= _N_HEADER_LINES:
        return

    header: Dict[str, str] = {}
    for line in lines[:_N_HEADER_LINES]:
        name, _, value = line.partition("\t")
        header[name] = value

    lat = _parse_value(header.get("Lat", ""))
    lon = _parse_value(header.get("Lon", ""))
    if lat is None or lon is None:
        return

    altitude = _parse_value(header.get("Alt", ""))
    station_id = header.get("ID", path.stem)
    location_name: Optional[str] = header.get("Name") or None
    if location_name == "NA":
        location_name = None

    for line in lines[_N_HEADER_LINES + 1 :]:  # skip the data-column header row
        fields = line.split("\t")
        if len(fields) < 8:
            continue
        try:
            year = int(fields[0])
            month = int(fields[1])
            day = int(fields[2])
        except ValueError:
            continue
        # Validate the calendar date; skip impossible days (e.g. 31 April).
        try:
            date(year, month, day)
        except ValueError:
            continue
        value_mm = _parse_value(fields[6])
        qc1, qc2 = _parse_qc_meta(fields[7])
        yield SEFObservation(
            station_id=station_id,
            location_name=location_name,
            latitude=lat,
            longitude=lon,
            altitude=altitude,
            year=year,
            month=month,
            day=day,
            value_mm=value_mm,
            qc1=qc1,
            qc2=qc2,
            passed=_passed(qc1, qc2),
        )


def iter_year_observations(sef_root: Path, year: int) -> Iterator[SEFObservation]:
    """Yield every :class:`SEFObservation` under ``<sef_root>/tsv/<year>/``."""
    year_dir = Path(sef_root) / "tsv" / str(year)
    if not year_dir.is_dir():
        return
    for path in sorted(year_dir.glob("*.tsv")):
        yield from iter_file_observations(path)


def discover_years(sef_root: Path) -> List[int]:
    """Return the sorted list of SEF years found under ``<sef_root>/tsv/``."""
    tsv_root = Path(sef_root) / "tsv"
    if not tsv_root.is_dir():
        return []
    years = [
        int(child.name)
        for child in tsv_root.iterdir()
        if child.is_dir() and child.name.isdigit()
    ]
    return sorted(years)


# --------------------------------------------------------------------------- #
# Parquet build
# --------------------------------------------------------------------------- #
_OBS_SCHEMA = pa.schema(
    [
        ("station_id", pa.string()),
        ("location_name", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("altitude", pa.float64()),
        ("year", pa.int32()),
        ("month", pa.int16()),
        ("day", pa.int16()),
        ("obs_date", pa.date32()),
        ("value_mm", pa.float64()),
        ("qc1", pa.string()),
        ("qc2", pa.string()),
        ("passed", pa.bool_()),
    ]
)


def _daily_national_sql(year: int) -> str:
    """DuckDB query aggregating the registered ``obs`` table to one row per date.

    Only trusted observations (``passed`` and a non-null value) contribute to the
    national mean, the maximum and the wet/extreme tallies.
    """
    ge10, ge25, ge50 = EXTREME_THRESHOLDS_MM
    return f"""
        SELECT
            obs_date,
            {year} AS year,
            count(*) FILTER (WHERE passed AND value_mm IS NOT NULL)
                AS n_stations_reporting,
            avg(value_mm) FILTER (WHERE passed AND value_mm IS NOT NULL)
                AS mean_mm,
            max(value_mm) FILTER (WHERE passed AND value_mm IS NOT NULL)
                AS max_mm,
            arg_max(station_id, value_mm)
                FILTER (WHERE passed AND value_mm IS NOT NULL)
                AS argmax_station_id,
            arg_max(location_name, value_mm)
                FILTER (WHERE passed AND value_mm IS NOT NULL)
                AS argmax_location,
            count(*) FILTER (WHERE passed AND value_mm >= {WET_THRESHOLD_MM})
                AS n_wet,
            count(*) FILTER (WHERE passed AND value_mm >= {ge10}) AS n_ge10,
            count(*) FILTER (WHERE passed AND value_mm >= {ge25}) AS n_ge25,
            count(*) FILTER (WHERE passed AND value_mm >= {ge50}) AS n_ge50
        FROM obs
        GROUP BY obs_date
        ORDER BY obs_date
    """


def build_year_parquet(sef_root: Path, year: int, out_root: Path) -> Dict[str, int]:
    """Parse one SEF year and write its ``observations`` and ``daily_national`` parquet.

    Returns a small summary dict (``n_observations``, ``n_passed``,
    ``n_stations``, ``n_days``) for logging/verification.
    """
    out_root = Path(out_root)
    obs_dir = out_root / "observations"
    daily_dir = out_root / "daily_national"
    obs_dir.mkdir(parents=True, exist_ok=True)
    daily_dir.mkdir(parents=True, exist_ok=True)

    station_id: List[str] = []
    location_name: List[Optional[str]] = []
    latitude: List[float] = []
    longitude: List[float] = []
    altitude: List[Optional[float]] = []
    year_col: List[int] = []
    month_col: List[int] = []
    day_col: List[int] = []
    obs_date: List[date] = []
    value_mm: List[Optional[float]] = []
    qc1_col: List[str] = []
    qc2_col: List[str] = []
    passed_col: List[bool] = []

    stations = set()
    n_passed = 0
    for obs in iter_year_observations(sef_root, year):
        station_id.append(obs.station_id)
        location_name.append(obs.location_name)
        latitude.append(obs.latitude)
        longitude.append(obs.longitude)
        altitude.append(obs.altitude)
        year_col.append(obs.year)
        month_col.append(obs.month)
        day_col.append(obs.day)
        obs_date.append(date(obs.year, obs.month, obs.day))
        value_mm.append(obs.value_mm)
        qc1_col.append(obs.qc1)
        qc2_col.append(obs.qc2)
        passed_col.append(obs.passed)
        stations.add(obs.station_id)
        if obs.passed:
            n_passed += 1

    table = pa.table(
        {
            "station_id": station_id,
            "location_name": location_name,
            "latitude": latitude,
            "longitude": longitude,
            "altitude": altitude,
            "year": year_col,
            "month": month_col,
            "day": day_col,
            "obs_date": obs_date,
            "value_mm": value_mm,
            "qc1": qc1_col,
            "qc2": qc2_col,
            "passed": passed_col,
        },
        schema=_OBS_SCHEMA,
    )
    pq.write_table(table, obs_dir / f"year={year}.parquet")

    con = duckdb.connect()
    try:
        con.register("obs", table)
        daily = con.execute(_daily_national_sql(year)).fetch_arrow_table()
    finally:
        con.close()
    pq.write_table(daily, daily_dir / f"year={year}.parquet")

    return {
        "n_observations": table.num_rows,
        "n_passed": n_passed,
        "n_stations": len(stations),
        "n_days": daily.num_rows,
    }


# --------------------------------------------------------------------------- #
# Default locations
# --------------------------------------------------------------------------- #
def default_analysis_root() -> Path:
    """Return ``$PDIR/sef_analysis`` (raises if ``PDIR`` is unset)."""
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass out_root explicitly")
    return Path(pdir) / "sef_analysis"


def default_sef_root() -> Path:
    """Return ``$PDIR/sef_export`` (raises if ``PDIR`` is unset)."""
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass sef_root explicitly")
    return Path(pdir) / "sef_export"
