from pathlib import Path

import duckdb

roots = [
    Path('/Volumes/Scratch/ADRQ/monthly_similarity_parquet'),
    Path('/Volumes/Scratch/ADRQ/monthly_similarity_allsheets_parquet'),
]

con = duckdb.connect()
try:
    for root in roots:
        print(f"\nROOT: {root}")
        meta_files = sorted((root / 'ensemble_metadata').glob('session_*.parquet'))
        print(f"  metadata files: {len(meta_files)}")
        if meta_files:
            latest_meta = max(meta_files, key=lambda p: int(p.stem.split('_')[1]))
            print(f"  latest metadata file: {latest_meta.name}")
            src = con.execute(
                f"SELECT match_source_session_id, COUNT(*) FROM read_parquet('{latest_meta}') "
                f"GROUP BY 1 ORDER BY 1"
            ).fetchall()
            print(f"  metadata match_source_session_id counts: {src[:6]}")

        sim_sessions_glob = root / 'similarity_sessions' / '*.parquet'
        sim_matches_glob = root / 'similarity_matches' / '*.parquet'
        if not any((root / 'similarity_sessions').glob('*.parquet')):
            print('  no similarity_sessions parquet')
            continue

        sessions = con.execute(
            f"SELECT session_id, ensemble_queries, matches_written "
            f"FROM read_parquet('{sim_sessions_glob}') ORDER BY session_id"
        ).fetchall()
        print(f"  sessions: {sessions[:8]}")

        rank1 = con.execute(
            f"SELECT session_id, COUNT(*) AS n "
            f"FROM read_parquet('{sim_matches_glob}') "
            f"WHERE query_rank = 1 GROUP BY 1 ORDER BY n DESC"
        ).fetchall()
        print(f"  rank1 rows by session (top): {rank1[:8]}")
finally:
    con.close()
