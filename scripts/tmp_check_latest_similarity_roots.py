from pathlib import Path

import duckdb

ROOTS = [
    Path('/Volumes/Scratch/ADRQ/monthly_similarity_parquet'),
    Path('/Volumes/Scratch/ADRQ/monthly_similarity_allsheets_parquet'),
]

for root in ROOTS:
    print(f"\nROOT: {root}")
    sim_sessions = root / 'similarity_sessions' / '*.parquet'
    sim_matches = root / 'similarity_matches' / '*.parquet'
    meta_dir = root / 'ensemble_metadata'
    has_meta = meta_dir.exists() and any(meta_dir.glob('session_*.parquet'))
    print(f"  has ensemble_metadata sessions: {has_meta}")

    con = duckdb.connect()
    try:
        latest_sim = con.execute(
            f"SELECT MAX(session_id) FROM read_parquet('{sim_sessions}')"
        ).fetchone()[0]
        if latest_sim is None:
            print('  latest similarity session: none')
            continue

        latest_sim = int(latest_sim)
        rank1_n = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{sim_matches}') WHERE session_id = ? AND query_rank = 1",
            [latest_sim],
        ).fetchone()[0]
        total_n = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{sim_matches}') WHERE session_id = ?",
            [latest_sim],
        ).fetchone()[0]

        print(f"  latest similarity session_id: {latest_sim}")
        print(f"  latest similarity rank-1 rows: {rank1_n}")
        print(f"  latest similarity total rows: {total_n}")

        if has_meta:
            latest_meta_file = max(meta_dir.glob('session_*.parquet'), key=lambda p: int(p.stem.split('_')[1]))
            print(f"  latest metadata file: {latest_meta_file.name}")
            src_sid = con.execute(
                f"SELECT MAX(match_source_session_id) FROM read_parquet('{latest_meta_file}')"
            ).fetchone()[0]
            src_rows = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{latest_meta_file}') WHERE match_source_session_id IS NOT NULL"
            ).fetchone()[0]
            print(f"  metadata source session_id (max): {src_sid}")
            print(f"  metadata rows with source session: {src_rows}")
    finally:
        con.close()
