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
* **Only exact matches are exported.** Approximate matches (a centroid position
  inferred from the top-ranked candidates but no confirmed station name) are
  *not* trustworthy enough to ship and are dropped entirely -- they never reach
  a SEF file. Only files whose metadata carries ``match_type = 'exact'`` (with a
  real location name / latitude / longitude) are considered here.
* **One SEF file per real station-year.** The ensemble frequently contains
  *duplicate* transcriptions of the same station-year (several ``file_id`` values
  that all matched the same real station in the same year). These duplicates are
  merged into a single SEF file: for every calendar day the value is taken from
  the duplicate with the best QC verdict (see the QC ladder below), so no day is
  ever dropped. Exact matches are grouped by
  ``(matched_location_name, matched_latitude, matched_longitude, matched_year)``.
  Files land under ``<output_root>/tsv/<matched_year>/<ID>.tsv``.
* **QC-aware day selection.** When several duplicates cover the same day, the
  value is chosen by this preference ladder (best first): ``qc1=pass`` >
  ``qc1=fail & qc2=pass`` > ``qc1=review`` > ``qc1=fail & qc2=indeterminate`` >
  everything else; ties are broken by the lowest ``file_id``. The merged file's
  ID is the *representative* source (the duplicate contributing the most
  ``qc1=pass`` days, tie-broken by lowest ``file_id``); the file-level ``Meta``
  lists every merged source and each observation records the source it came from.
* **All exact-matched observations** are exported (every day of an exact-matched
  station-year). The QC verdict travels in each observation's
  per-observation ``Meta`` column as ``qc1=<pass|review|fail>``,
  ``qc2=<pass|fail|indeterminate|NA>`` (``qc2`` only exists for the QC1-fail days
  re-examined by the secondary check) and ``source=<specifier>`` (which duplicate
  supplied the value).
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
import shutil
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
    """Summary of a SEF export run over a matched-year slice."""

    output_root: Path
    qc_session_id: int
    files_written: int
    obs_rows: int
    start_year: Optional[int]
    end_year: Optional[int]


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


def _station_id(file_name) -> str:
    """The SEF-legal ID derived from an ensemble file name (its specifier)."""
    return _sanitise_id(_specifier(str(file_name)))


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


def _qc_rank(qc1, qc2) -> int:
    """Return the QC preference rank of a day (1 = best, 5 = worst).

    The ladder, best first, is: ``qc1=pass`` (1) > ``qc1=fail & qc2=pass`` (2) >
    ``qc1=review`` (3) > ``qc1=fail & qc2=indeterminate`` (4) > everything else
    (5, e.g. failed both checks, or no QC verdict). It is used to pick which
    duplicate transcription supplies each merged day's value.
    """
    q1 = "" if _is_missing(qc1) else str(qc1).strip().lower()
    q2 = "" if _is_missing(qc2) else str(qc2).strip().lower()
    if q1 == "pass":
        return 1
    if q1 == "fail" and q2 == "pass":
        return 2
    if q1 == "review":
        return 3
    if q1 == "fail" and q2 == "indeterminate":
        return 4
    return 5


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
    start_year: Optional[int],
    end_year: Optional[int],
) -> str:
    """Build the streamed join of consensus + located metadata + QC verdicts.

    Only exact matches are selected (``match_type = 'exact'``); approximate
    matches are filtered out and never exported. The result is ordered by
    ``(matched_year, group_key, month, day_of_month, file_id)`` so that every
    duplicate of a real station-year arrives as one contiguous block ready to be
    merged. ``group_key`` collapses exact matches that share a location and year.
    """
    consensus_glob = _glob_sql(consensus_root / "daily_consensus")
    metadata_glob = _glob_sql(comparison_root / "ensemble_metadata")
    qc_status_glob = _glob_sql(qc_root / "daily_qc_status")

    year_clauses = []
    if start_year is not None:
        year_clauses.append(f"matched_year >= {int(start_year)}")
    if end_year is not None:
        year_clauses.append(f"matched_year <= {int(end_year)}")
    meta_year = (" AND " + " AND ".join(year_clauses)) if year_clauses else ""

    if secondary_status_source is not None:
        qc2_cte = f"""
            qc2 AS (
                SELECT file_id, matched_year, month, day_of_month, secondary_flag
                FROM read_parquet('{secondary_status_source}')
                WHERE qc_session_id = {int(qc_session_id)}
                  AND file_id IN (SELECT file_id FROM meta)
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

    # Exact matches that share a location and year are one real station-year and
    # are merged. (Approximate matches are filtered out by the meta CTE below,
    # so the fallback file-keyed branch only guards exact rows lacking a name.)
    group_key_expr = (
        "CASE WHEN m.match_type = 'exact' AND m.matched_location_name IS NOT NULL\n"
        "     THEN 'loc:' || m.matched_location_name || '@'\n"
        "          || CAST(round(m.matched_latitude, 4) AS VARCHAR) || ','\n"
        "          || CAST(round(m.matched_longitude, 4) AS VARCHAR)\n"
        "     ELSE 'file:' || CAST(m.file_id AS VARCHAR)\n"
        "END AS group_key"
    )

    return f"""
        WITH meta AS (
            SELECT file_id, file_name, matched_location_name,
                   matched_latitude, matched_longitude, matched_elevation_ft,
                   matched_year, match_type
            FROM read_parquet('{metadata_glob}')
            WHERE match_type = 'exact'
              AND matched_latitude IS NOT NULL
              AND matched_longitude IS NOT NULL
              AND match_source_session_id = (
                  SELECT MAX(match_source_session_id)
                  FROM read_parquet('{metadata_glob}')
              )
              {meta_year}
        ),
        cons AS (
            SELECT file_id, month, day_of_month, consensus_value
            FROM read_parquet('{consensus_glob}')
            WHERE file_id IN (SELECT file_id FROM meta)
        ),
        qc1 AS (
            SELECT file_id, month, day_of_month, final_flag
            FROM read_parquet('{qc_status_glob}')
            WHERE qc_session_id = {int(qc_session_id)}
              AND file_id IN (SELECT file_id FROM meta)
        ),
        {qc2_cte}
        joined AS (
            SELECT
                m.file_id, m.file_name, m.matched_location_name,
                m.matched_latitude, m.matched_longitude, m.matched_elevation_ft,
                m.matched_year, m.match_type,
                {group_key_expr},
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
        ORDER BY matched_year, group_key, month, day_of_month, file_id
    """


# --------------------------------------------------------------------------- #
# Duplicate merging
# --------------------------------------------------------------------------- #
def _merge_group(group_rows: List[Dict[str, object]]):
    """Merge the duplicate transcriptions of one real station-year.

    ``group_rows`` are all the observation rows that share a ``group_key`` (and
    year): every duplicate of that exact-matched location-year. For each calendar
    day the value is taken from the duplicate with the best QC verdict
    (``_qc_rank``), ties broken by the
    lowest ``file_id``. Returns ``(station, observations)`` where ``station``
    carries the representative source's metadata plus the list of every merged
    source, and each observation records the ``source`` it was taken from.
    """
    by_day: Dict[tuple, tuple] = {}
    pass_counts: Dict[int, int] = {}
    first_row: Dict[int, Dict[str, object]] = {}

    for row in group_rows:
        fid = int(row["file_id"])
        first_row.setdefault(fid, row)
        pass_counts.setdefault(fid, 0)
        q1 = row["qc1_flag"]
        if not _is_missing(q1) and str(q1).strip().lower() == "pass":
            pass_counts[fid] += 1
        day = (int(row["month"]), int(row["day_of_month"]))
        rank = (_qc_rank(row["qc1_flag"], row["qc2_flag"]), fid)
        best = by_day.get(day)
        if best is None or rank < best[0]:
            by_day[day] = (rank, row)

    # Representative source: most qc1=pass days, tie-broken by lowest file_id.
    rep_fid = min(pass_counts, key=lambda f: (-pass_counts[f], f))
    rep_row = first_row[rep_fid]
    sources = sorted(_station_id(first_row[f]["file_name"]) for f in first_row)

    observations: List[Dict[str, object]] = []
    for day in sorted(by_day):
        _, row = by_day[day]
        obs = dict(row)
        obs["source"] = _station_id(row["file_name"])
        observations.append(obs)

    station = {
        "file_name": rep_row["file_name"],
        "matched_location_name": rep_row["matched_location_name"],
        "matched_latitude": rep_row["matched_latitude"],
        "matched_longitude": rep_row["matched_longitude"],
        "matched_elevation_ft": rep_row["matched_elevation_ft"],
        "matched_year": rep_row["matched_year"],
        "match_type": rep_row["match_type"],
        "sources": sources,
        "n_sources": len(sources),
    }
    return station, observations


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
            f"n.sources={int(station['n_sources'])}",
            f"sources={','.join(station['sources'])}",
        ]
    )

    values = {
        "SEF": SEF_VERSION,
        "ID": _station_id(station["file_name"]),
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
            f"source={_fmt_flag(row.get('source'))}",
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
    station_id = _station_id(station["file_name"])
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
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    source: str = DEFAULT_SOURCE,
    link: str = DEFAULT_LINK,
    obs_hour: int = DEFAULT_OBS_HOUR,
    obs_minute: int = DEFAULT_OBS_MINUTE,
    batch_rows: int = 200_000,
) -> SEFExportResult:
    """Write one merged SEF ``.tsv`` per real station-year for a year slice.

    Streams the consensus/metadata/QC join ordered by ``(matched_year,
    group_key)`` and flushes a file each time that key advances. All duplicate
    transcriptions of a station-year therefore arrive together and are merged
    QC-aware (see :func:`_merge_group`), so memory stays bounded to one
    station-year at a time regardless of how many stations fall in the slice.
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
            start_year=start_year,
            end_year=end_year,
        )

        files_written = 0
        obs_rows = 0
        current_key: Optional[tuple] = None
        group_rows: List[Dict[str, object]] = []
        cleared_years: set = set()

        def _clear_year(year) -> None:
            # Wipe a year's output directory the first time it appears in the
            # (year-ordered) stream, before any file for it is written. This
            # removes stale files left by a previous export -- e.g. duplicate
            # transcriptions that are now merged away and would otherwise linger
            # and be double-counted downstream. Year sharding is disjoint, so a
            # run only ever clears years it owns.
            if year is None or year in cleared_years:
                return
            year_dir = output_root / "tsv" / str(int(year))
            if year_dir.exists():
                shutil.rmtree(year_dir)
            cleared_years.add(year)

        def _flush() -> None:
            nonlocal files_written, obs_rows
            if not group_rows:
                return
            station, observations = _merge_group(group_rows)
            if not observations:
                return
            station["qc_session_id"] = qc_session_id
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
                _clear_year(row["matched_year"])
                key = (row["matched_year"], row["group_key"])
                if key != current_key:
                    _flush()
                    current_key = key
                    group_rows = []
                group_rows.append(row)
        _flush()
    finally:
        conn.close()

    _write_manifest(
        output_root,
        qc_session_id=qc_session_id,
        files_written=files_written,
        obs_rows=obs_rows,
        start_year=start_year,
        end_year=end_year,
    )

    return SEFExportResult(
        output_root=output_root,
        qc_session_id=qc_session_id,
        files_written=files_written,
        obs_rows=obs_rows,
        start_year=start_year,
        end_year=end_year,
    )


def _write_manifest(
    output_root: Path,
    *,
    qc_session_id: int,
    files_written: int,
    obs_rows: int,
    start_year: Optional[int],
    end_year: Optional[int],
) -> None:
    """Record a small per-run manifest of the counts for this slice."""
    manifest_dir = output_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    lo = "min" if start_year is None else str(int(start_year))
    hi = "max" if end_year is None else str(int(end_year))
    manifest_path = manifest_dir / f"manifest_{lo}_{hi}.json"
    manifest_path.write_text(
        json.dumps(
            {
                "qc_session_id": qc_session_id,
                "files_written": files_written,
                "obs_rows": obs_rows,
                "start_year": start_year,
                "end_year": end_year,
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
