#!/usr/bin/env python
"""Plot exact-agreement distribution for matches at a selected rank.

Builds a bar chart of `exact_agreement_count` across all matches with a chosen
`query_rank` for a similarity session in `monthly_similarity.sqlite`.

Example:
    python scripts/diagnostics/plot_rank1_exact_agreement_distribution.py \
        --output /var/tmp/rank1_exact_distribution.webp
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Tuple


def _pdir() -> Path:
    pdir = os.getenv("PDIR")
    if not pdir:
        raise SystemExit("PDIR environment variable is not set.")
    return Path(pdir)


def _default_db_path() -> Path:
    return _pdir() / "monthly_similarity.sqlite"


def _connect_immutable(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_distribution(
    comparison_db: Path,
    session_id: Optional[int],
    rank: int,
) -> Tuple[int, Dict[int, int], int]:
    """Return (session_id, counts_by_exact, total_rows_for_rank)."""
    if rank < 1:
        raise SystemExit("Rank must be >= 1.")

    with _connect_immutable(comparison_db) as conn:
        selected_session = session_id
        if selected_session is None:
            row = conn.execute("SELECT MAX(session_id) AS sid FROM similarity_sessions").fetchone()
            selected_session = row["sid"]

        if selected_session is None:
            raise SystemExit("No similarity sessions found.")

        rows = conn.execute(
            """
            SELECT exact_agreement_count, COUNT(*) AS n
            FROM similarity_matches
            WHERE session_id = ? AND query_rank = ?
            GROUP BY exact_agreement_count
            ORDER BY exact_agreement_count
            """,
            (selected_session, rank),
        ).fetchall()

    counts = {int(r["exact_agreement_count"]): int(r["n"]) for r in rows}
    total = sum(counts.values())
    if total == 0:
        raise SystemExit(
            f"No rank-{rank} matches found for session {selected_session}."
        )
    return selected_session, counts, total


def plot_distribution(
    counts: Dict[int, int],
    total: int,
    session_id: int,
    rank: int,
    output_path: Path,
) -> Path:
    import matplotlib.pyplot as plt

    x = list(range(13))
    y = [counts.get(i, 0) for i in x]
    pct = [(v / total) * 100.0 for v in y]

    fig, ax = plt.subplots(figsize=(11, 6), dpi=120)
    bars = ax.bar(x, y, color="#1f77b4", edgecolor="black", linewidth=0.6)

    ax.set_title(
        f"Rank-{rank} Match Distribution by Exact Agreement Count (session {session_id})"
    )
    ax.set_xlabel("Exact agreement count (months)")
    ax.set_ylabel(f"Number of rank-{rank} matches")
    ax.set_xticks(x)

    # Keep labels readable: only annotate bars with non-zero counts.
    for i, b in enumerate(bars):
        if y[i] == 0:
            continue
        ax.text(
            b.get_x() + b.get_width() / 2,
            b.get_height(),
            f"{y[i]}\n({pct[i]:.1f}%)",
            ha="center",
            va="bottom",
            fontsize=8,
        )

    ax.text(
        0.99,
        0.98,
        f"Total rank-{rank} rows: {total}",
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=10,
        bbox={"facecolor": "white", "alpha": 0.8, "edgecolor": "0.7"},
    )

    ax.margins(y=0.08)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path))
    plt.close(fig)
    return output_path


def main() -> None:
    default_db = _default_db_path()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--comparison-db",
        type=Path,
        default=default_db,
        help=f"Path to monthly_similarity.sqlite (default: {default_db})",
    )
    parser.add_argument(
        "--session-id",
        type=int,
        default=None,
        help="Similarity session ID (default: latest)",
    )
    parser.add_argument(
        "--rank",
        type=int,
        default=1,
        help="Match rank to summarise (default: 1)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("rank1_exact_agreement_distribution.webp"),
        help="Output image path",
    )
    args = parser.parse_args()

    session_id, counts, total = load_distribution(args.comparison_db, args.session_id, args.rank)
    result = plot_distribution(counts, total, session_id, args.rank, args.output)
    print(f"Wrote {result}")


if __name__ == "__main__":
    main()
