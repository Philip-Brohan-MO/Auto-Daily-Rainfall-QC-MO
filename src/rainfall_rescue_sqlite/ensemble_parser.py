"""Parsing logic for ensemble transcription JSON files.

Each JSON file has top-level keys ``Day 1`` .. ``Day 31`` and ``Totals``.
Every key maps to a list of 12 month slots (January..December), and each
month slot is an object ``{"values": [v1, v2, v3, v4, v5]}`` holding 5
ensemble member values (rainfall in mm, or ``null``).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

EXPECTED_MONTHS = 12
EXPECTED_MEMBERS = 5
DAY_KEY_PATTERN = re.compile(r"^Day\s+(\d+)$")
TOTALS_KEY = "Totals"

# DRain_1921-1930_RainNos_Gloucestershire_C-S-435.json
FILENAME_PATTERN = re.compile(
    r"^DRain_(?P<year_start>\d{4})-(?P<year_end>\d{4})_RainNos_"
    r"(?P<descriptor>.+)_(?P<section_id>[^_]+)$"
)


@dataclass(frozen=True)
class EnsembleFileMetadata:
    file_name: str
    source_path: str
    year_start: Optional[int]
    year_end: Optional[int]
    descriptor: Optional[str]
    section_id: Optional[str]
    num_days: int


@dataclass(frozen=True)
class ParsedEnsembleFile:
    metadata: EnsembleFileMetadata
    # (day_of_month, month, ensemble_member, rainfall)
    daily_rows: List[Tuple[int, int, int, Optional[float]]]
    # (month, ensemble_member, total)
    total_rows: List[Tuple[int, int, Optional[float]]]


class EnsembleParseError(ValueError):
    """Raised when a JSON file does not match the ensemble structure."""


def _parse_filename(stem: str) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
    match = FILENAME_PATTERN.match(stem)
    if not match:
        return None, None, None, None
    return (
        int(match.group("year_start")),
        int(match.group("year_end")),
        match.group("descriptor"),
        match.group("section_id"),
    )


def _month_values(slot: object, key: str, month_idx: int) -> List[Optional[float]]:
    if not isinstance(slot, dict) or "values" not in slot:
        raise EnsembleParseError(
            f"Entry {month_idx} under '{key}' is missing a 'values' object"
        )
    values = slot["values"]
    if not isinstance(values, list) or len(values) != EXPECTED_MEMBERS:
        raise EnsembleParseError(
            f"Entry {month_idx} under '{key}' must have {EXPECTED_MEMBERS} values, "
            f"found {len(values) if isinstance(values, list) else type(values).__name__}"
        )
    coerced: List[Optional[float]] = []
    for value in values:
        if value is None:
            coerced.append(None)
        elif isinstance(value, (int, float)):
            coerced.append(float(value))
        else:
            raise EnsembleParseError(
                f"Non-numeric value '{value}' under '{key}' entry {month_idx}"
            )
    return coerced


def parse_ensemble_json(path: Path) -> ParsedEnsembleFile:
    """Parse one ensemble JSON file into normalized daily and total rows."""
    with path.open("r", encoding="utf-8") as handle:
        try:
            data = json.load(handle)
        except json.JSONDecodeError as exc:
            raise EnsembleParseError(f"Invalid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise EnsembleParseError("Top-level JSON must be an object")

    daily_rows: List[Tuple[int, int, int, Optional[float]]] = []
    total_rows: List[Tuple[int, int, Optional[float]]] = []
    days_seen = 0

    for key, slots in data.items():
        if key == TOTALS_KEY:
            target_is_total = True
            day_of_month = None
        else:
            match = DAY_KEY_PATTERN.match(key)
            if not match:
                raise EnsembleParseError(f"Unexpected top-level key '{key}'")
            target_is_total = False
            day_of_month = int(match.group(1))
            days_seen += 1

        if not isinstance(slots, list) or len(slots) != EXPECTED_MONTHS:
            raise EnsembleParseError(
                f"Key '{key}' must map to {EXPECTED_MONTHS} month entries, "
                f"found {len(slots) if isinstance(slots, list) else type(slots).__name__}"
            )

        for month_offset, slot in enumerate(slots):
            month = month_offset + 1
            values = _month_values(slot, key, month_offset)
            for member_offset, value in enumerate(values):
                member = member_offset + 1
                if target_is_total:
                    total_rows.append((month, member, value))
                else:
                    daily_rows.append((day_of_month, month, member, value))

    year_start, year_end, descriptor, section_id = _parse_filename(path.stem)
    metadata = EnsembleFileMetadata(
        file_name=path.name,
        source_path=str(path),
        year_start=year_start,
        year_end=year_end,
        descriptor=descriptor,
        section_id=section_id,
        num_days=days_seen,
    )

    return ParsedEnsembleFile(
        metadata=metadata,
        daily_rows=daily_rows,
        total_rows=total_rows,
    )
