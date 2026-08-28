from pathlib import Path

import duckdb

from src.rainfall_rescue_sqlite.parquet_similarity import (
    default_allsheets_comparison_parquet_root,
    default_comparison_parquet_root,
)

roots = [
    default_comparison_parquet_root(),
    default_allsheets_comparison_parquet_root(),
]
daily_glob = "/Volumes/Scratch/ADRQ/ensemble_transcriptions_parquet/ensemble_daily_values/*.parquet"

print("roots:", roots)
for root in roots:
    root = Path(root)
    meta_glob = f"{root}/ensemble_metadata/*.parquet"
    print("\nROOT", root)
    conn = duckdb.connect()
    try:
        try:
            nrows = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{meta_glob}')"
            ).fetchone()[0]
        except Exception as exc:
            print(" no metadata parquet:", exc)
            continue

        print(" metadata rows:", nrows)
        years = conn.execute(
            f"SELECT MIN(matched_year), MAX(matched_year), COUNT(DISTINCT matched_year) FROM read_parquet('{meta_glob}') WHERE matched_year IS NOT NULL"
        ).fetchone()
        print(" year range/distinct:", years)

        max_sid = conn.execute(
            f"SELECT MAX(match_source_session_id) FROM read_parquet('{meta_glob}')"
        ).fetchone()[0]
        print(" max match_source_session_id:", max_sid)

        located_any = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{meta_glob}')
            WHERE matched_year=1895
              AND matched_latitude IS NOT NULL
              AND matched_longitude IS NOT NULL
            """
        ).fetchone()[0]
        print(" located 1895 rows (all sessions):", located_any)

        located_latest = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{meta_glob}')
            WHERE matched_year=1895
              AND matched_latitude IS NOT NULL
              AND matched_longitude IS NOT NULL
              AND match_source_session_id = ?
            """,
            [max_sid],
        ).fetchone()[0]
        print(" located 1895 rows (max session):", located_latest)

        rainy_latest = conn.execute(
            f"""
            WITH meta AS (
                SELECT file_id
                FROM read_parquet('{meta_glob}')
                WHERE matched_year=1895
                  AND matched_latitude IS NOT NULL
                  AND matched_longitude IS NOT NULL
                  AND match_source_session_id = ?
            )
            SELECT COUNT(*)
            FROM read_parquet('{daily_glob}') d
            WHERE d.file_id IN (SELECT file_id FROM meta)
              AND d.month = 11
              AND d.day_of_month = 3
              AND d.rainfall IS NOT NULL
            """,
            [max_sid],
        ).fetchone()[0]
        print(" records with rainfall on 1895-11-03 (max session):", rainy_latest)

        top = conn.execute(
            f"""
            WITH latest AS (
                SELECT MAX(match_source_session_id) AS sid
                FROM read_parquet('{meta_glob}')
            ),
            meta AS (
                SELECT file_id, matched_year
                FROM read_parquet('{meta_glob}'), latest
                WHERE match_source_session_id = latest.sid
                  AND matched_year IS NOT NULL
                  AND matched_latitude IS NOT NULL
                  AND matched_longitude IS NOT NULL
            ),
            daily AS (
                SELECT file_id, month, day_of_month
                FROM read_parquet('{daily_glob}')
                WHERE rainfall IS NOT NULL
            )
            SELECT matched_year, month, day_of_month, COUNT(*) AS n
            FROM meta
            JOIN daily USING (file_id)
            GROUP BY 1,2,3
            ORDER BY n DESC, 1,2,3
            LIMIT 3
            """
        ).fetchall()
        print(" top dates in latest session:", top)
    finally:
        conn.close()
