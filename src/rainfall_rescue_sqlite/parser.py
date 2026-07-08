"""Parsing logic for Rainfall Rescue combined CSV files."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

MONTH_NAMES: Sequence[str] = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


@dataclass(frozen=True)
class StationMetadata:
    station_file_id: str
    station_folder: str
    station_file_name: str
    location_name: Optional[str]
    grid_reference: Optional[str]
    longitude: Optional[float]
    latitude: Optional[float]
    elevation_ft: Optional[int]
    station_number: Optional[str]
    source_path: str


@dataclass(frozen=True)
class ParsedCombinedFile:
    station: StationMetadata
    monthly_rows: List[Tuple[str, int, int, float]]
    annual_rows: List[Tuple[str, int, float]]


class ParseError(ValueError):
    """Raised when a CSV cannot be parsed as a combined rainfall file."""


def _cell(row: Sequence[str], idx: int) -> str:
    if idx < len(row):
        return row[idx].strip()
    return ""


def _to_float(value: str) -> Optional[float]:
    if not value:
        return None
    lowered = value.lower()
    if lowered in {"nan", "na", "n/a"}:
        return None
    return float(value)


def _to_int(value: str) -> Optional[int]:
    if not value:
        return None
    return int(float(value))


def parse_combined_csv(path: Path, data_root: Path) -> ParsedCombinedFile:
    """Parse one combined rainfall CSV and return normalized station/month/annual rows."""
    rows: List[List[str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        rows = [row for row in reader]

    if len(rows) < 18:
        raise ParseError(f"Expected at least 18 rows, found {len(rows)}")

    relative = path.relative_to(data_root)
    station_folder = relative.parent.name
    station_file_name = path.stem
    station_file_id = f"{station_folder}/{station_file_name}"

    location_name = _cell(rows[0], 0) or None
    grid_reference = _cell(rows[1], 1) or None
    longitude = _to_float(_cell(rows[1], 3))
    latitude = _to_float(_cell(rows[1], 5))
    elevation_ft = _to_int(_cell(rows[1], 7))
    station_number = _cell(rows[2], 1) or None

    years_row = rows[4]
    year_columns: List[Tuple[int, int]] = []
    for col_idx in range(1, len(years_row)):
        value = _cell(years_row, col_idx)
        if not value:
            continue
        try:
            year_columns.append((col_idx, int(value)))
        except ValueError:
            continue

    if not year_columns:
        raise ParseError("No year headings found in row 5")

    monthly_rows: List[Tuple[str, int, int, float]] = []
    for month_idx, month_name in enumerate(MONTH_NAMES, start=1):
        csv_row_idx = 4 + month_idx
        row = rows[csv_row_idx]
        row_label = _cell(row, 0).lower()
        if row_label and month_name.lower() not in row_label:
            raise ParseError(
                f"Unexpected row label '{_cell(row, 0)}' for {month_name} in row {csv_row_idx + 1}"
            )

        for col_idx, year in year_columns:
            value = _to_float(_cell(row, col_idx))
            if value is None:
                continue
            monthly_rows.append((station_file_id, year, month_idx, value))

    total_row = rows[17]
    annual_rows: List[Tuple[str, int, float]] = []
    for col_idx, year in year_columns:
        value = _to_float(_cell(total_row, col_idx))
        if value is None:
            continue
        annual_rows.append((station_file_id, year, value))

    station = StationMetadata(
        station_file_id=station_file_id,
        station_folder=station_folder,
        station_file_name=station_file_name,
        location_name=location_name,
        grid_reference=grid_reference,
        longitude=longitude,
        latitude=latitude,
        elevation_ft=elevation_ft,
        station_number=station_number,
        source_path=str(relative),
    )

    return ParsedCombinedFile(station=station, monthly_rows=monthly_rows, annual_rows=annual_rows)
