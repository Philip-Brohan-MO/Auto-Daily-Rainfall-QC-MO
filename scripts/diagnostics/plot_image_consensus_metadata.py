#!/usr/bin/env python
"""Diagnostic figure for one daily-rainfall transcription.

Given a daily-data specifier (the ensemble file name stem, e.g.
``DRain_1911-1920_RainNos_Middlesex_H-P-17``) this builds a single figure with:

* left    - the original scanned image
* middle  - the ensemble daily-transcription consensus (median over members)
* top-right - monthly-total comparison: all 5 ensemble members vs a selected-rank
              Rainfall-Rescue station-year, plus a differences panel
* bottom-right - a UK map showing the selected-rank station location

All data comes from this project's three SQLite databases (built under ``$PDIR``):

* ``ensemble_transcriptions.sqlite`` - daily ensemble transcriptions
* ``rainfall_rescue.sqlite``         - Rainfall-Rescue monthly data (via the
                                       comparison DB's ``rr_monthly_vectors``)
* ``monthly_similarity.sqlite``      - comparison vectors + similarity matches

Example
-------
    python scripts/diagnostics/plot_image_consensus_metadata.py \
        --specifier DRain_1911-1920_RainNos_Middlesex_H-P-17 \
    --comparison-rank 1 \
        --top-k 5 --output /var/tmp/diag.webp
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional, Tuple

import numpy as np

MONTH_LABELS = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)
# matplotlib default (tab10) cycle, used to keep monthly-plot and map colours in sync
TAB10 = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]


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


def lookup_ensemble_file(ensemble_db: Path, specifier: str) -> sqlite3.Row:
    """Find the ensemble file row for a specifier (file-name stem or .json name)."""
    name = specifier
    if not name.endswith(".json"):
        name = f"{name}.json"
    with _connect_immutable(ensemble_db) as conn:
        row = conn.execute(
            "SELECT file_id, file_name, source_path, descriptor, section_id, "
            "year_start, year_end FROM ensemble_files WHERE file_name = ?",
            (name,),
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT file_id, file_name, source_path, descriptor, section_id, "
                "year_start, year_end FROM ensemble_files WHERE file_name LIKE ?",
                (f"%{specifier}%",),
            ).fetchone()
    if row is None:
        raise SystemExit(f"No ensemble file found matching '{specifier}'.")
    return row


def load_daily_consensus(
    ensemble_db: Path, file_id: int
) -> Tuple[np.ndarray, np.ndarray]:
    """Return (consensus, spread) arrays shaped (31 days, 12 months).

    consensus = per-cell median over ensemble members (NaN where no data);
    spread    = per-cell inter-member range (max - min), a crude uncertainty.
    """
    consensus = np.full((31, 12), np.nan)
    spread = np.full((31, 12), np.nan)
    grouped: Dict[Tuple[int, int], List[float]] = {}
    with _connect_immutable(ensemble_db) as conn:
        for row in conn.execute(
            "SELECT day_of_month, month, rainfall FROM ensemble_daily_values "
            "WHERE file_id = ? AND rainfall IS NOT NULL",
            (file_id,),
        ):
            day = int(row["day_of_month"])
            month = int(row["month"])
            if 1 <= day <= 31 and 1 <= month <= 12:
                grouped.setdefault((day, month), []).append(float(row["rainfall"]))
    for (day, month), values in grouped.items():
        consensus[day - 1, month - 1] = median(values)
        spread[day - 1, month - 1] = max(values) - min(values)
    return consensus, spread


def load_monthly_consensus(comparison_db: Path, ensemble_vector_id: str) -> List[Optional[float]]:
    """Return the 12-month ensemble consensus monthly-total vector."""
    with _connect_immutable(comparison_db) as conn:
        row = conn.execute(
            "SELECT raw_vector_json FROM ensemble_consensus_vectors "
            "WHERE ensemble_vector_id = ?",
            (ensemble_vector_id,),
        ).fetchone()
    if row is None:
        raise SystemExit(
            f"No comparison vector for '{ensemble_vector_id}'. "
            "Has build_comparison_vectors been run?"
        )
    return json.loads(row["raw_vector_json"])


def load_ensemble_member_monthly(
    comparison_db: Path, ensemble_vector_id: str
) -> List[List[Optional[float]]]:
    """Return per-member monthly totals as a list of 5 lists (each 12 values)."""
    members: Dict[int, Dict[int, Optional[float]]] = {}
    with _connect_immutable(comparison_db) as conn:
        rows = conn.execute(
            "SELECT month, ensemble_member, total "
            "FROM ensemble_member_monthly_values "
            "WHERE ensemble_vector_id = ? "
            "ORDER BY ensemble_member, month",
            (ensemble_vector_id,),
        ).fetchall()
    for row in rows:
        m = int(row["month"])
        mbr = int(row["ensemble_member"])
        members.setdefault(mbr, {})[m] = row["total"]
    result = []
    for mbr in sorted(members.keys()):
        result.append([members[mbr].get(mo) for mo in range(1, 13)])
    return result


def load_matches(
    comparison_db: Path, ensemble_vector_id: str, top_k: int
) -> List[dict]:
    """Return the top-K matching RR station-years (latest session)."""
    matches: List[dict] = []
    with _connect_immutable(comparison_db) as conn:
        session_id = conn.execute(
            "SELECT MAX(session_id) FROM similarity_sessions"
        ).fetchone()[0]
        if session_id is None:
            raise SystemExit("No similarity_sessions found; run the matcher first.")
        rows = conn.execute(
            """
                 SELECT m.query_rank, m.exact_agreement_count,
                     m.cosine_similarity, m.adjusted_score,
                   m.overlap_months, r.location_name, r.station_number, r.year,
                   r.latitude, r.longitude, r.raw_vector_json
            FROM similarity_matches m
            JOIN rr_monthly_vectors r ON r.rr_vector_id = m.rr_vector_id
            WHERE m.session_id = ? AND m.ensemble_vector_id = ?
            ORDER BY m.query_rank
            LIMIT ?
            """,
            (session_id, ensemble_vector_id, top_k),
        ).fetchall()
    for row in rows:
        matches.append(
            {
                "rank": row["query_rank"],
                "exact": row["exact_agreement_count"],
                "cosine": row["cosine_similarity"],
                "adjusted": row["adjusted_score"],
                "overlap": row["overlap_months"],
                "location_name": row["location_name"],
                "station_number": row["station_number"],
                "year": row["year"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "monthly": json.loads(row["raw_vector_json"]),
            }
        )
    return matches


def resolve_image_path(source_path: str, file_name: str) -> Optional[Path]:
    """Derive the scanned-image path from the JSON source path."""
    stem = Path(file_name).stem
    src = Path(source_path)
    # .../operational_sample/ensemble_transcriptions/<name>.json
    #   -> .../operational_sample/images/<name>.jpg
    images_dir = src.parent.parent / "images"
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff"):
        candidate = images_dir / f"{stem}{ext}"
        if candidate.exists():
            return candidate
    return None


# --------------------------------------------------------------------------- #
# Plotting
# --------------------------------------------------------------------------- #
def _plot_image(ax, image_path: Optional[Path]) -> None:
    ax.set_xticks([])
    ax.set_yticks([])
    if image_path is None:
        ax.text(0.5, 0.5, "image not found", ha="center", va="center")
        return
    import matplotlib.image as mpimg

    ax.imshow(mpimg.imread(str(image_path)))
    ax.set_title(image_path.name, fontsize="small")


def _plot_daily_consensus(
    ax, consensus: np.ndarray, spread: np.ndarray, monthly_totals
) -> None:
    """Render the daily consensus as a text table (days x months).

    A "Total" row of monthly totals is drawn beneath day 31.
    """
    ax.set_title("Daily transcription consensus (median over members)")
    ax.set_xlim(-0.5, 11.5)
    ax.set_ylim(0.5, 33.0)
    ax.invert_yaxis()  # day 1 at the top

    # Month headers along the top; day numbers (plus a Total row) down the left.
    ax.set_xticks(range(12))
    ax.set_xticklabels(MONTH_LABELS, fontsize="small")
    ax.xaxis.set_ticks_position("top")
    ax.xaxis.set_label_position("top")
    ax.set_yticks(list(range(1, 32)) + [32.5])
    ax.set_yticklabels([str(d) for d in range(1, 32)] + ["Tot"], fontsize=6)
    ax.tick_params(length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Light gridlines to separate cells.
    for c in range(13):
        ax.axvline(c - 0.5, color="0.85", lw=0.5, zorder=0)
    for r in range(1, 33):
        ax.axhline(r - 0.5, color="0.85", lw=0.5, zorder=0)
    # Heavier separator above the totals row.
    ax.axhline(31.9, color="0.4", lw=0.9, zorder=1)

    # Highlight cells where the ensemble members disagree (non-zero spread).
    max_spread = np.nanmax(spread) if np.isfinite(spread).any() else 0.0
    for day in range(1, 32):
        for month in range(1, 13):
            value = consensus[day - 1, month - 1]
            if np.isnan(value):
                continue
            disagreement = spread[day - 1, month - 1]
            if max_spread > 0 and disagreement and disagreement > 0:
                colour = "#c62828"  # members disagree
            else:
                colour = "black"
            ax.text(
                month - 1,
                day,
                f"{value:g}",
                ha="center",
                va="center",
                fontsize=9,
                color=colour,
                zorder=5,
            )

    # Monthly totals row.
    for month in range(1, 13):
        value = monthly_totals[month - 1]
        if value is None:
            continue
        try:
            value = float(value)
        except (TypeError, ValueError):
            continue
        ax.text(
            month - 1,
            32.5,
            f"{value:g}",
            ha="center",
            va="center",
            fontsize=9,
            fontweight="bold",
            zorder=5,
        )


MEMBER_COLOURS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]


def _plot_selected_rank_member_comparison(
    ax_top, ax_bot, member_monthly: List[List[Optional[float]]], match: dict
) -> None:
    """Plot all 5 ensemble member monthly values against a selected-rank RR match."""
    months = list(range(1, 13))
    rr = [float(v) if v is not None else np.nan for v in match["monthly"]]

    for i, month_vals in enumerate(member_monthly):
        vals = [float(v) if v is not None else np.nan for v in month_vals]
        ax_top.plot(
            months, vals,
            marker="o", ls="-", color=MEMBER_COLOURS[i % len(MEMBER_COLOURS)],
            alpha=0.7, lw=1.2, markersize=4,
            label=f"Member {i + 1}",
        )

    rr_label = (
        f"RR rank-{int(match['rank'])}: {int(match['exact']):>2d} exact  "
        f"{int(match['year']):4d}  {match['location_name']}"
    )
    ax_top.plot(months, rr, marker="s", color="black", lw=2, zorder=10, label=rr_label)

    ax_top.set_title(f"Ensemble members vs rank-{int(match['rank'])} RR match")
    ax_top.set_ylabel("Monthly total")
    ax_top.set_xticks([])

    handles, labels = ax_top.get_legend_handles_labels()
    ax_bot.legend(
        handles=handles,
        labels=labels,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.35),
        bbox_transform=ax_bot.transAxes,
        ncol=1,
        fontsize="small",
        prop={"family": "monospace"},
    )

    for i, month_vals in enumerate(member_monthly):
        diff = []
        for j in range(12):
            try:
                val = float(month_vals[j])
            except (TypeError, ValueError):
                val = np.nan
            diff.append(val - rr[j])
        ax_bot.plot(
            months, diff,
            marker="o", color=MEMBER_COLOURS[i % len(MEMBER_COLOURS)],
            alpha=0.7, lw=1.2, markersize=4,
        )
    ax_bot.axhline(0.0, color="black", lw=0.8, ls="--")
    ax_bot.set_xticks(months)
    ax_bot.set_xticklabels(MONTH_LABELS)
    ax_bot.set_xlabel("Month")
    ax_bot.set_ylabel(f"member − RR rank-{int(match['rank'])}")


def _plot_map(fig, rect, matches) -> None:
    from cartopy import crs as ccrs
    from cartopy.io import shapereader

    ax = fig.add_axes(rect, projection=ccrs.PlateCarree())
    ax.set_extent([-11, 4, 49, 61], crs=ccrs.PlateCarree())
    shp = shapereader.natural_earth(
        resolution="10m", category="cultural", name="admin_0_countries"
    )
    reader = shapereader.Reader(shp)
    for rec in reader.records():
        admin = rec.attributes.get("ADMIN") or rec.attributes.get("NAME")
        iso = rec.attributes.get("ISO_A3")
        if admin == "United Kingdom" or iso == "GBR":
            ax.add_geometries(
                [rec.geometry], ccrs.PlateCarree(),
                facecolor="none", edgecolor="black", linewidth=0.9,
            )
            break
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_aspect("equal")

    for i, m in enumerate(matches):
        lat, lon = m["latitude"], m["longitude"]
        if lat is None or lon is None:
            continue
        ax.plot(
            lon, lat, marker="o", color=TAB10[i % len(TAB10)],
            markersize=8, markeredgecolor="black",
            transform=ccrs.PlateCarree(), zorder=20,
        )


def build_figure(
    *,
    specifier: str,
    ensemble_db: Path,
    comparison_db: Path,
    top_k: int,
    comparison_rank: int = 1,
    output_path: Path,
) -> Path:
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure

    file_row = lookup_ensemble_file(ensemble_db, specifier)
    file_id = int(file_row["file_id"])
    ensemble_vector_id = f"ensemble_file::{file_id}"

    consensus_daily, spread_daily = load_daily_consensus(ensemble_db, file_id)
    consensus_monthly = load_monthly_consensus(comparison_db, ensemble_vector_id)
    member_monthly = load_ensemble_member_monthly(comparison_db, ensemble_vector_id)
    if comparison_rank < 1:
        raise SystemExit("comparison_rank must be >= 1")
    # Ensure the selected rank is available even if top_k is smaller.
    match_limit = max(top_k, comparison_rank)
    matches = load_matches(comparison_db, ensemble_vector_id, match_limit)
    image_path = resolve_image_path(file_row["source_path"], file_row["file_name"])
    selected_match = next((m for m in matches if int(m["rank"]) == comparison_rank), None)

    fig = Figure(figsize=(20, 10), dpi=100, facecolor=(0.95, 0.95, 0.95, 1))
    FigureCanvas(fig)

    # Left: original image
    ax_image = fig.add_axes([0.01, 0.04, 0.32, 0.90])
    _plot_image(ax_image, image_path)

    # Middle: daily consensus as a text table
    ax_daily = fig.add_axes([0.37, 0.04, 0.30, 0.86])
    _plot_daily_consensus(ax_daily, consensus_daily, spread_daily, consensus_monthly)

    # Top-right: 5 ensemble members vs selected-rank RR match
    ax_month_top = fig.add_axes([0.72, 0.70, 0.26, 0.20])
    ax_month_bot = fig.add_axes([0.72, 0.53, 0.26, 0.15])
    if selected_match is not None:
        _plot_selected_rank_member_comparison(
            ax_month_top, ax_month_bot, member_monthly, selected_match
        )
    else:
        ax_month_top.text(
            0.5,
            0.5,
            f"No rank-{comparison_rank} match available",
            ha="center",
            va="center",
            transform=ax_month_top.transAxes,
        )
        ax_month_top.set_axis_off()
        ax_month_bot.set_axis_off()

    # Bottom-right: UK map showing only the selected-rank matched station
    map_matches = [selected_match] if selected_match is not None else []
    _plot_map(fig, [0.72, 0.02, 0.26, 0.30], map_matches)

    title = f"{file_row['file_name']}   (file_id={file_id})"
    fig.suptitle(title, x=0.5, y=0.98, ha="center", va="top", fontsize="x-large")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path))
    return output_path


def _default_paths() -> Tuple[Path, Path]:
    pdir = _pdir()
    return (
        pdir / "ensemble_transcriptions.sqlite",
        pdir / "monthly_similarity.sqlite",
    )


def main() -> None:
    ens_default, cmp_default = _default_paths()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--specifier", required=True,
        help="Daily-data specifier, e.g. DRain_1911-1920_RainNos_Middlesex_H-P-17",
    )
    parser.add_argument("--top-k", type=int, default=5, help="Matches to display")
    parser.add_argument(
        "--ensemble-db", type=Path, default=ens_default,
        help=f"ensemble_transcriptions.sqlite (default: {ens_default})",
    )
    parser.add_argument(
        "--comparison-db", type=Path, default=cmp_default,
        help=f"monthly_similarity.sqlite (default: {cmp_default})",
    )
    parser.add_argument(
        "--comparison-rank",
        type=int,
        default=1,
        help="Rank of match to compare against (default: 1)",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Output image path (default: <specifier>_diagnostic.webp in cwd)",
    )
    args = parser.parse_args()

    output = args.output or Path(f"{args.specifier}_diagnostic.webp")
    result = build_figure(
        specifier=args.specifier,
        ensemble_db=args.ensemble_db,
        comparison_db=args.comparison_db,
        top_k=args.top_k,
        comparison_rank=args.comparison_rank,
        output_path=output,
    )
    print(f"Wrote {result}")


if __name__ == "__main__":
    main()
