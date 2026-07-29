#!/usr/bin/env python
"""Interactive map of day-level QC flags for one date.

Draws the same UK station map style as the rainfall interactive map, but colours
points by final QC flag (pass/review/fail) from ``daily_qc_status`` for the
selected date and QC session.

Located stations with no QC row for the date are shown as "no_qc".
Clicking a point copies the station's specifier (file-name stem) to clipboard.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Optional

import duckdb

from src.rainfall_rescue_sqlite.parquet_ingest import default_ensemble_parquet_root
from src.rainfall_rescue_sqlite.parquet_qc_exact_monthly import default_qc_parquet_root
from src.rainfall_rescue_sqlite.parquet_similarity import default_comparison_parquet_root


def _pdir() -> Path:
    pdir = os.getenv("PDIR")
    if not pdir:
        raise SystemExit("PDIR environment variable is not set.")
    return Path(pdir)


def _glob_sql(dir_path: Path) -> str:
    return str((dir_path / "*.parquet").resolve())


@dataclass(frozen=True)
class QCRecord:
    file_name: str
    location_name: Optional[str]
    latitude: float
    longitude: float
    qc_flag: str


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
"""


def _specifier(file_name: str) -> str:
    return file_name[:-5] if file_name.endswith(".json") else file_name


def _normalise_flag(flag: str) -> str:
    value = (flag or "").strip().lower()
    if value in {"pass", "review", "fail", "no_qc"}:
        return value
    return "other"


def load_qc_records_for_date(
    ensemble_db: Optional[Path],
    *,
    target_date: date,
    qc_session_id: Optional[int] = None,
    include_unassessed: bool = False,
    backend: str = "sqlite",
    ensemble_dataset_root: Optional[Path] = None,
    comparison_root: Optional[Path] = None,
    qc_root: Optional[Path] = None,
    similarity_session_id: Optional[int] = None,
) -> tuple[int, List[QCRecord]]:
    """Load located stations and their QC flag for a target date.

    By default this returns only file-days that were assessed in
    ``daily_qc_status`` for the selected session/date, which is much faster than
    scanning all located stations and tagging missing rows as ``no_qc``.
    """
    if backend == "sqlite":
        if ensemble_db is None:
            raise SystemExit("ensemble_db is required for sqlite backend")
        with sqlite3.connect(f"file:{ensemble_db}?immutable=1", uri=True) as conn:
            conn.row_factory = sqlite3.Row

            if qc_session_id is None:
                row = conn.execute("SELECT MAX(qc_session_id) FROM qc_sessions").fetchone()
                if row is None or row[0] is None:
                    raise SystemExit("No QC sessions found in ensemble DB.")
                qc_session_id = int(row[0])

            if include_unassessed:
                rows = conn.execute(
                    """
                    SELECT
                        ef.file_name,
                        ef.matched_location_name,
                        ef.matched_latitude,
                        ef.matched_longitude,
                        COALESCE(qs.final_flag, 'no_qc') AS qc_flag
                    FROM ensemble_files ef
                    LEFT JOIN daily_qc_status qs
                      ON qs.qc_session_id = ?
                     AND qs.file_id = ef.file_id
                     AND qs.month = ?
                     AND qs.day_of_month = ?
                    WHERE ef.matched_year = ?
                      AND ef.matched_latitude IS NOT NULL
                      AND ef.matched_longitude IS NOT NULL
                    ORDER BY ef.file_id
                    """,
                    (qc_session_id, target_date.month, target_date.day, target_date.year),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        ef.file_name,
                        ef.matched_location_name,
                        ef.matched_latitude,
                        ef.matched_longitude,
                        qs.final_flag AS qc_flag
                    FROM daily_qc_status qs
                    JOIN ensemble_files ef
                      ON ef.file_id = qs.file_id
                    WHERE qs.qc_session_id = ?
                      AND qs.month = ?
                      AND qs.day_of_month = ?
                      AND ef.matched_year = ?
                      AND ef.matched_latitude IS NOT NULL
                      AND ef.matched_longitude IS NOT NULL
                    ORDER BY ef.file_id
                    """,
                    (qc_session_id, target_date.month, target_date.day, target_date.year),
                ).fetchall()
    else:
        ensemble_dataset_root = ensemble_dataset_root or default_ensemble_parquet_root()
        comparison_root = comparison_root or default_comparison_parquet_root()
        qc_root = qc_root or default_qc_parquet_root()

        conn = duckdb.connect()
        try:
            if qc_session_id is None:
                row = conn.execute(
                    f"SELECT MAX(qc_session_id) FROM read_parquet('{_glob_sql(qc_root / 'qc_sessions')}')"
                ).fetchone()
                if row is None or row[0] is None:
                    raise SystemExit("No QC sessions found in parquet QC dataset.")
                qc_session_id = int(row[0])

            if similarity_session_id is None:
                row = conn.execute(
                    f"""
                    SELECT CAST(json_extract_string(config_json, '$.similarity_session_id') AS BIGINT)
                    FROM read_parquet('{_glob_sql(qc_root / 'qc_sessions')}')
                    WHERE qc_session_id = ?
                    LIMIT 1
                    """,
                    [int(qc_session_id)],
                ).fetchone()
                if row is None or row[0] is None:
                    row2 = conn.execute(
                        f"SELECT MAX(session_id) FROM read_parquet('{_glob_sql(comparison_root / 'similarity_sessions')}')"
                    ).fetchone()
                    if row2 is None or row2[0] is None:
                        raise SystemExit("No similarity sessions found in parquet comparison dataset.")
                    similarity_session_id = int(row2[0])
                else:
                    similarity_session_id = int(row[0])

            base_cte = f"""
                WITH exact_located AS (
                    SELECT
                        CAST(REPLACE(sm.ensemble_vector_id, 'ensemble_file::', '') AS BIGINT) AS file_id,
                        ef.file_name,
                        rv.location_name,
                        rv.latitude,
                        rv.longitude,
                        rv.year
                    FROM read_parquet('{_glob_sql(comparison_root / 'similarity_matches')}') sm
                    JOIN read_parquet('{_glob_sql(comparison_root / 'rr_monthly_vectors')}') rv
                      ON rv.rr_vector_id = sm.rr_vector_id
                    JOIN read_parquet('{_glob_sql(ensemble_dataset_root / 'ensemble_files')}') ef
                      ON ef.file_id = CAST(REPLACE(sm.ensemble_vector_id, 'ensemble_file::', '') AS BIGINT)
                    WHERE sm.session_id = {int(similarity_session_id)}
                      AND sm.query_rank = 1
                      AND sm.exact_agreement_count >= 9
                      AND rv.latitude IS NOT NULL
                      AND rv.longitude IS NOT NULL
                      AND rv.year = {int(target_date.year)}
                )
            """

            if include_unassessed:
                rows = conn.execute(
                    base_cte
                    + f"""
                    SELECT
                        e.file_name,
                        e.location_name,
                        e.latitude,
                        e.longitude,
                        COALESCE(qs.final_flag, 'no_qc') AS qc_flag
                    FROM exact_located e
                    LEFT JOIN read_parquet('{_glob_sql(qc_root / 'daily_qc_status')}') qs
                      ON qs.qc_session_id = {int(qc_session_id)}
                     AND qs.file_id = e.file_id
                     AND qs.month = {int(target_date.month)}
                     AND qs.day_of_month = {int(target_date.day)}
                    ORDER BY e.file_id
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    base_cte
                    + f"""
                    SELECT
                        e.file_name,
                        e.location_name,
                        e.latitude,
                        e.longitude,
                        qs.final_flag AS qc_flag
                    FROM read_parquet('{_glob_sql(qc_root / 'daily_qc_status')}') qs
                    JOIN exact_located e
                      ON e.file_id = qs.file_id
                    WHERE qs.qc_session_id = {int(qc_session_id)}
                      AND qs.month = {int(target_date.month)}
                      AND qs.day_of_month = {int(target_date.day)}
                    ORDER BY e.file_id
                    """
                ).fetchall()
        finally:
            conn.close()

    records: List[QCRecord] = []
    for r in rows:
        if hasattr(r, "keys"):
            keys = set(r.keys())
            location_name = r["matched_location_name"] if "matched_location_name" in keys else r["location_name"]
            latitude = r["matched_latitude"] if "matched_latitude" in keys else r["latitude"]
            longitude = r["matched_longitude"] if "matched_longitude" in keys else r["longitude"]
            records.append(
                QCRecord(
                    file_name=str(r["file_name"]),
                    location_name=location_name,
                    latitude=float(latitude),
                    longitude=float(longitude),
                    qc_flag=str(r["qc_flag"]),
                )
            )
        else:
            records.append(
                QCRecord(
                    file_name=str(r[0]),
                    location_name=r[1],
                    latitude=float(r[2]),
                    longitude=float(r[3]),
                    qc_flag=str(r[4]),
                )
            )
    return qc_session_id, records


def build_figure(
    *,
    target_date: date,
    ensemble_db: Optional[Path],
    qc_session_id: Optional[int] = None,
    output_path: Optional[Path] = None,
    marker_size: float = 9.0,
    include_unassessed: bool = False,
    backend: str = "sqlite",
    ensemble_dataset_root: Optional[Path] = None,
    comparison_root: Optional[Path] = None,
    qc_root: Optional[Path] = None,
    similarity_session_id: Optional[int] = None,
):
    """Build the interactive UK QC-flag map for ``target_date``."""
    import plotly.graph_objects as go

    qc_session_id, records = load_qc_records_for_date(
        ensemble_db,
        target_date=target_date,
        qc_session_id=qc_session_id,
        include_unassessed=include_unassessed,
        backend=backend,
        ensemble_dataset_root=ensemble_dataset_root,
        comparison_root=comparison_root,
        qc_root=qc_root,
        similarity_session_id=similarity_session_id,
    )
    if not records:
        raise SystemExit(
            f"No located ensemble records found for {target_date.isoformat()}."
        )

    # Draw order is bottom-to-top: later traces render above earlier ones in
    # Plotly, so listing "pass" last puts passing stations on top of failing ones.
    flag_order = ["no_qc", "other", "review", "fail", "pass"]
    flag_colors = {
        "pass": "#7a9ac4",
        "review": "#ffbf00",
        "fail": "#cc8585",
        "no_qc": "#9e9e9e",
        "other": "#4f81bd",
    }

    groups: dict[str, list[QCRecord]] = {flag: [] for flag in flag_order}
    for record in records:
        groups[_normalise_flag(record.qc_flag)].append(record)

    fig = go.Figure()

    for flag in flag_order:
        group = groups[flag]
        if not group:
            continue
        fig.add_trace(
            go.Scattergeo(
                lon=[r.longitude for r in group],
                lat=[r.latitude for r in group],
                mode="markers",
                name=flag,
                marker=dict(
                    size=marker_size + 1,
                    color=flag_colors[flag],
                    line=dict(width=0.5, color="black"),
                ),
                customdata=[
                    [
                        _specifier(r.file_name),
                        r.location_name or "",
                        r.qc_flag,
                    ]
                    for r in group
                ],
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "%{customdata[1]}<br>"
                    "qc=%{customdata[2]}"
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
            f"Daily QC flags  {target_date.isoformat()}  (session {qc_session_id})<br>"
            f"<sup>{len(records)} located stations "
            f"(click a point to copy its source specifier)</sup>"
        ),
        width=800,
        height=1000,
        margin=dict(l=10, r=10, t=70, b=10),
        legend_title="QC flag",
    )

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.write_html(str(output_path), include_plotlyjs=True, post_script=_CLICK_TO_COPY_JS)

    return fig


def inline_html(fig) -> str:
    """Return an HTML fragment with click-to-copy behavior."""
    return fig.to_html(
        include_plotlyjs=True,
        full_html=False,
        post_script=_CLICK_TO_COPY_JS,
    )


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"'{value}' is not a valid date; expected YYYY-MM-DD"
        ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--backend",
        choices=("duckdb", "sqlite"),
        default="duckdb",
        help="Storage backend for QC/session data",
    )
    parser.add_argument("date", type=_parse_date, help="Date to plot, as YYYY-MM-DD")
    parser.add_argument(
        "--ensemble-db",
        type=Path,
        default=None,
        help="Path to ensemble_transcriptions.sqlite (default: $PDIR/...)",
    )
    parser.add_argument(
        "--ensemble-dataset-root",
        type=Path,
        default=None,
        help="Path to ensemble_transcriptions_parquet root",
    )
    parser.add_argument(
        "--comparison-root",
        type=Path,
        default=None,
        help="Path to monthly_similarity_parquet root",
    )
    parser.add_argument(
        "--qc-root",
        type=Path,
        default=None,
        help="Path to qc_parquet root",
    )
    parser.add_argument(
        "--similarity-session-id",
        type=int,
        default=None,
        help="Similarity session id (default: from QC session config or latest)",
    )
    parser.add_argument(
        "--qc-session-id",
        type=int,
        default=None,
        help="QC session id (default: latest)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output HTML path (default: $PDIR/diagnostics/daily_qc_map_<date>.html)",
    )
    parser.add_argument(
        "--include-unassessed",
        action="store_true",
        help="Include located stations with no QC row for the date as no_qc",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensemble_db = args.ensemble_db or (_pdir() / "ensemble_transcriptions.sqlite")
    output_path = args.output or (
        _pdir() / "diagnostics" / f"daily_qc_map_{args.date.isoformat()}.html"
    )

    build_figure(
        target_date=args.date,
        ensemble_db=ensemble_db if args.backend == "sqlite" else None,
        qc_session_id=args.qc_session_id,
        output_path=output_path,
        include_unassessed=args.include_unassessed,
        backend=args.backend,
        ensemble_dataset_root=args.ensemble_dataset_root,
        comparison_root=args.comparison_root,
        qc_root=args.qc_root,
        similarity_session_id=args.similarity_session_id,
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
