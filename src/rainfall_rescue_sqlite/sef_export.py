"""Export located, quality-controlled daily rainfall to Station Exchange Format.

The Station Exchange Format (SEF, https://datarescue.climate.copernicus.eu/
station-exchange-format-sef) is a simple, tab-separated text format for sharing
newly digitised weather observations. One SEF file holds one variable from one
station.

In this project each ensemble transcription file is a single *station-year* of
daily rainfall: it has a matched location (name / latitude / longitude /
elevation from ``ensemble_metadata``), a ``matched_year`` and, for every day it
covers, a consensus daily total (the member median in ``daily_consensus``). This
module writes one SEF ``.tsv`` per located station-year, carrying every day's
consensus value together with its QC verdicts.

Design (locked with the maintainer)
-----------------------------------
* **One SEF file per station-year** (per ensemble ``file_id``). Files land under
  ``<output_root>/tsv/<matched_year>/<ID>.tsv`` so no single directory holds the
  whole (~500k-file) dataset.
* **All located observations** are exported (any station with coordinates, i.e.
  an exact or approximate match). The QC verdict travels in each observation's
  per-observation ``Meta`` column as ``qc1=<pass|review|fail>`` and
  ``qc2=<pass|fail|indeterminate|NA>`` (``qc2`` only exists for the QC1-fail days
  re-examined by the secondary check).
* **Units**: the stored consensus is in inches; SEF ``Value`` is converted to
  millimetres (``× 25.4``) and the file-wide ``Meta`` records ``orig.units=in``.
* **Every day** present in the transcription is emitted, including ``0.0`` (the
  consensus already stores ``0.0`` for missing members).

The per-field conventions (variable name ``rr``, statistic ``sum``, period
``1day``, observation hour 9 for the 09:00 UK rainfall day) are module constants
so they are easy to review and override.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import duckdb

from .parquet_qc_exact_monthly import default_qc_parquet_root
from .parquet_regional_stats import default_daily_consensus_parquet_root
from .parquet_secondary_qc import default_secondary_qc_parquet_root
from .parquet_similarity import _configure_duckdb, default_comparison_parquet_root

# --------------------------------------------------------------------------- #
# SEF conventions (review / override here)
# --------------------------------------------------------------------------- #
SEF_VERSION = "1.0.0"

# Header fields shared by every file produced from this dataset.
DEFAULT_SOURCE = "RainfallRescue"
DEFAULT_LINK = "NA"
VBL = "rr"          # precipitation amount (C3S recommended abbreviation)
STAT = "sum"        # daily accumulated total
UNITS = "mm"        # SEF value units (converted from the original inches)
PERIOD = "1day"     # accumulation period of each daily value

# Observation time. UK daily rainfall is the total for the 24 h ending at 09:00.
DEFAULT_OBS_HOUR = 9
DEFAULT_OBS_MINUTE = 0

# Unit conversions.
INCHES_TO_MM = 25.4
FEET_TO_M = 0.3048

# The 12 SEF header names, in their required order.
HEADER_ORDER = [
    "SEF",
    "ID",
    "Name",
    "Lat",
    "Lon",
    "Alt",
    "Source",
    "Link",
    "Vbl",
    "Stat",
    "Units",
    "Meta",
]

# The data-table column header (line 13 of the file).
DATA_COLUMNS = ["Year", "Month", "Day", "Hour", "Minute", "Period", "Value", "Meta"]


@dataclass(frozen=True)
class SEFExportResult:
    """Summary of a SEF export run over a file_id slice."""

    output_root: Path
    qc_session_id: int
    files_written: int
    obs_rows: int
    start_file_id: Optional[int]
    end_file_id: Optional[int]


# --------------------------------------------------------------------------- #
# Small helpers (mirror the sibling parquet modules)
# --------------------------------------------------------------------------- #
def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _glob_sql(dir_path: Path) -> str:
    return str((dir_path / "*.parquet").resolve())


def _connect() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection honouring env-based memory/temp limits."""
    conn = duckdb.connect()
    _configure_duckdb(conn)
    return conn


def default_sef_output_root() -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass output_root explicitly")
    return Path(pdir) / "sef_export"


def _resolve_secondary_status_source(secondary_qc_root: Path) -> Optional[str]:
    """Return a DuckDB source for the secondary-QC status, or None if absent.

    The canonical full-run file ``secondary_qc_status/secondary_qc_status.parquet``
    is preferred; otherwise every parquet shard in that directory is read. When
    no secondary-QC output exists yet, ``None`` is returned and the ``qc2`` verdict
    is reported as ``NA`` for every observation.
    """
    status_dir = secondary_qc_root / "secondary_qc_status"
    canonical = status_dir / "secondary_qc_status.parquet"
    if canonical.is_file():
        return str(canonical.resolve())
    if status_dir.is_dir() and any(status_dir.glob("*.parquet")):
        return _glob_sql(status_dir)
    return None


def _sanitise_id(specifier: str) -> str:
    """Reduce a specifier to the SEF-legal ID character set ([A-Za-z0-9._-])."""
    cleaned = "".join(c if (c.isalnum() or c in "._-") else "_" for c in specifier)
    return cleaned or "station"


def _specifier(file_name: str) -> str:
    return file_name[:-5] if file_name.endswith(".json") else file_name


def _is_missing(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _fmt_num(value, decimals: int) -> str:
    """Format a number to ``decimals`` places, or ``NA`` when missing."""
    if _is_missing(value):
        return "NA"
    return f"{float(value):.{decimals}f}"


def _fmt_flag(value) -> str:
    if _is_missing(value):
        return "NA"
    text = str(value).strip()
    return text if text else "NA"


# --------------------------------------------------------------------------- #
# Query construction
# --------------------------------------------------------------------------- #
def _build_query(
    *,
    consensus_root: Path,
    comparison_root: Path,
    qc_root: Path,
    secondary_status_source: Optional[str],
    qc_session_id: int,
    start_file_id: Optional[int],
    end_file_id: Optional[int],
) -> str:
    """Build the streamed join of consensus + located metadata + QC verdicts."""
    consensus_glob = _glob_sql(consensus_root / "daily_consensus")
    metadata_glob = _glob_sql(comparison_root / "ensemble_metadata")
    qc_status_glob = _glob_sql(qc_root / "daily_qc_status")

    range_clauses = []
    if start_file_id is not None:
        range_clauses.append(f"file_id >= {int(start_file_id)}")
    if end_file_id is not None:
        range_clauses.append(f"file_id <= {int(end_file_id)}")
    cons_where = ("WHERE " + " AND ".join(range_clauses)) if range_clauses else ""
    meta_range = (" AND " + " AND ".join(range_clauses)) if range_clauses else ""

    if secondary_status_source is not None:
        qc2_cte = f"""
            qc2 AS (
                SELECT file_id, matched_year, month, day_of_month, secondary_flag
                FROM read_parquet('{secondary_status_source}')
                WHERE qc_session_id = {int(qc_session_id)}
                QUALIFY row_number() OVER (
                    PARTITION BY file_id, matched_year, month, day_of_month
                    ORDER BY train_session_id DESC
                ) = 1
            ),
        """
        qc2_select = "q2.secondary_flag AS qc2_flag"
        qc2_join = (
            "LEFT JOIN qc2 q2\n"
            "  ON q2.file_id = c.file_id\n"
            " AND q2.matched_year = m.matched_year\n"
            " AND q2.month = c.month\n"
            " AND q2.day_of_month = c.day_of_month"
        )
    else:
        qc2_cte = ""
        qc2_select = "CAST(NULL AS VARCHAR) AS qc2_flag"
        qc2_join = ""

    return f"""
        WITH cons AS (
            SELECT file_id, month, day_of_month, consensus_value
            FROM read_parquet('{consensus_glob}')
            {cons_where}
        ),
        meta AS (
            SELECT file_id, file_name, matched_location_name,
                   matched_latitude, matched_longitude, matched_elevation_ft,
                   matched_year, match_type
            FROM read_parquet('{metadata_glob}')
            WHERE matched_latitude IS NOT NULL
              AND matched_longitude IS NOT NULL
              AND match_source_session_id = (
                  SELECT MAX(match_source_session_id)
                  FROM read_parquet('{metadata_glob}')
              )
              {meta_range}
        ),
        qc1 AS (
            SELECT file_id, month, day_of_month, final_flag
            FROM read_parquet('{qc_status_glob}')
            WHERE qc_session_id = {int(qc_session_id)}
        ),
        {qc2_cte}
        joined AS (
            SELECT
                m.file_id, m.file_name, m.matched_location_name,
                m.matched_latitude, m.matched_longitude, m.matched_elevation_ft,
                m.matched_year, m.match_type,
                c.month, c.day_of_month, c.consensus_value,
                q1.final_flag AS qc1_flag,
                {qc2_select}
            FROM cons c
            JOIN meta m ON m.file_id = c.file_id
            LEFT JOIN qc1 q1
              ON q1.file_id = c.file_id
             AND q1.month = c.month
             AND q1.day_of_month = c.day_of_month
            {qc2_join}
        )
        SELECT * FROM joined
        ORDER BY file_id, month, day_of_month
    """


# --------------------------------------------------------------------------- #
# SEF file writing
# --------------------------------------------------------------------------- #
def _header_lines(station: Dict[str, object], *, source: str, link: str) -> List[str]:
    """The 12 SEF header lines for one station."""
    location_name = station["matched_location_name"]
    alt_ft = station["matched_elevation_ft"]
    alt_m = None if _is_missing(alt_ft) else float(alt_ft) * FEET_TO_M
    match_type = _fmt_flag(station["match_type"])

    file_meta = "|".join(
        [
            "orig.units=in",
            f"match.type={match_type}",
            f"qc.session={int(station['qc_session_id'])}",
        ]
    )

    values = {
        "SEF": SEF_VERSION,
        "ID": _sanitise_id(_specifier(str(station["file_name"]))),
        "Name": "NA" if _is_missing(location_name) else str(location_name),
        "Lat": _fmt_num(station["matched_latitude"], 4),
        "Lon": _fmt_num(station["matched_longitude"], 4),
        "Alt": _fmt_num(alt_m, 1),
        "Source": source,
        "Link": link,
        "Vbl": VBL,
        "Stat": STAT,
        "Units": UNITS,
        "Meta": file_meta,
    }
    return [f"{field}\t{values[field]}" for field in HEADER_ORDER]


def _obs_line(row: Dict[str, object], *, obs_hour: int, obs_minute: int) -> str:
    """One data-table line for a single day's consensus observation."""
    value_mm = float(row["consensus_value"]) * INCHES_TO_MM
    obs_meta = "|".join(
        [
            f"qc1={_fmt_flag(row['qc1_flag'])}",
            f"qc2={_fmt_flag(row['qc2_flag'])}",
        ]
    )
    fields = [
        str(int(row["matched_year"])),
        str(int(row["month"])),
        str(int(row["day_of_month"])),
        str(int(obs_hour)),
        str(int(obs_minute)),
        PERIOD,
        f"{value_mm:.1f}",
        obs_meta,
    ]
    return "\t".join(fields)


def _write_sef_file(
    station: Dict[str, object],
    observations: List[Dict[str, object]],
    *,
    output_root: Path,
    source: str,
    link: str,
    obs_hour: int,
    obs_minute: int,
) -> int:
    """Write one station-year's SEF ``.tsv`` and return the observation count."""
    year = int(station["matched_year"])
    station_id = _sanitise_id(_specifier(str(station["file_name"])))
    out_dir = output_root / "tsv" / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{station_id}.tsv"

    lines = _header_lines(station, source=source, link=link)
    lines.append("\t".join(DATA_COLUMNS))
    lines.extend(
        _obs_line(obs, obs_hour=obs_hour, obs_minute=obs_minute)
        for obs in observations
    )

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return len(observations)


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #
def export_sef(
    *,
    output_root: Optional[Path] = None,
    comparison_root: Optional[Path] = None,
    consensus_root: Optional[Path] = None,
    qc_root: Optional[Path] = None,
    secondary_qc_root: Optional[Path] = None,
    qc_session_id: Optional[int] = None,
    start_file_id: Optional[int] = None,
    end_file_id: Optional[int] = None,
    source: str = DEFAULT_SOURCE,
    link: str = DEFAULT_LINK,
    obs_hour: int = DEFAULT_OBS_HOUR,
    obs_minute: int = DEFAULT_OBS_MINUTE,
    batch_rows: int = 200_000,
) -> SEFExportResult:
    """Write one SEF ``.tsv`` per located station-year for a file_id slice.

    Streams the consensus/metadata/QC join in ``file_id`` order and flushes a
    file each time the ``file_id`` advances, so memory stays bounded regardless of
    how many stations fall in the slice.
    """
    output_root = Path(output_root) if output_root is not None else default_sef_output_root()
    comparison_root = (
        Path(comparison_root) if comparison_root is not None else default_comparison_parquet_root()
    )
    consensus_root = (
        Path(consensus_root) if consensus_root is not None else default_daily_consensus_parquet_root()
    )
    qc_root = Path(qc_root) if qc_root is not None else default_qc_parquet_root()
    secondary_qc_root = (
        Path(secondary_qc_root) if secondary_qc_root is not None else default_secondary_qc_parquet_root()
    )

    conn = _connect()
    try:
        if qc_session_id is None:
            value = conn.execute(
                f"SELECT MAX(qc_session_id) FROM read_parquet("
                f"'{_glob_sql(qc_root / 'daily_qc_status')}')"
            ).fetchone()[0]
            if value is None:
                raise ValueError("No QC sessions found in qc_root; run QC check 1 first")
            qc_session_id = int(value)

        secondary_status_source = _resolve_secondary_status_source(secondary_qc_root)

        query = _build_query(
            consensus_root=consensus_root,
            comparison_root=comparison_root,
            qc_root=qc_root,
            secondary_status_source=secondary_status_source,
            qc_session_id=qc_session_id,
            start_file_id=start_file_id,
            end_file_id=end_file_id,
        )

        files_written = 0
        obs_rows = 0
        current_file_id: Optional[int] = None
        station: Optional[Dict[str, object]] = None
        observations: List[Dict[str, object]] = []

        def _flush() -> None:
            nonlocal files_written, obs_rows
            if station is None or not observations:
                return
            written = _write_sef_file(
                station,
                observations,
                output_root=output_root,
                source=source,
                link=link,
                obs_hour=obs_hour,
                obs_minute=obs_minute,
            )
            files_written += 1
            obs_rows += written

        reader = conn.execute(query).fetch_record_batch(batch_rows)
        for batch in reader:
            for row in batch.to_pylist():
                file_id = int(row["file_id"])
                if file_id != current_file_id:
                    _flush()
                    current_file_id = file_id
                    station = {
                        "file_name": row["file_name"],
                        "matched_location_name": row["matched_location_name"],
                        "matched_latitude": row["matched_latitude"],
                        "matched_longitude": row["matched_longitude"],
                        "matched_elevation_ft": row["matched_elevation_ft"],
                        "matched_year": row["matched_year"],
                        "match_type": row["match_type"],
                        "qc_session_id": qc_session_id,
                    }
                    observations = []
                observations.append(row)
        _flush()
    finally:
        conn.close()

    _write_manifest(
        output_root,
        qc_session_id=qc_session_id,
        files_written=files_written,
        obs_rows=obs_rows,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
    )

    return SEFExportResult(
        output_root=output_root,
        qc_session_id=qc_session_id,
        files_written=files_written,
        obs_rows=obs_rows,
        start_file_id=start_file_id,
        end_file_id=end_file_id,
    )


def _write_manifest(
    output_root: Path,
    *,
    qc_session_id: int,
    files_written: int,
    obs_rows: int,
    start_file_id: Optional[int],
    end_file_id: Optional[int],
) -> None:
    """Record a small per-run manifest of the counts for this slice."""
    manifest_dir = output_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    lo = "min" if start_file_id is None else str(int(start_file_id))
    hi = "max" if end_file_id is None else str(int(end_file_id))
    manifest_path = manifest_dir / f"manifest_{lo}_{hi}.json"
    manifest_path.write_text(
        json.dumps(
            {
                "qc_session_id": qc_session_id,
                "files_written": files_written,
                "obs_rows": obs_rows,
                "start_file_id": start_file_id,
                "end_file_id": end_file_id,
                "created_at": _utc_now(),
                "sef_version": SEF_VERSION,
                "vbl": VBL,
                "stat": STAT,
                "units": UNITS,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
