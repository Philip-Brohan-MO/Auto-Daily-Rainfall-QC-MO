"""Merge per-shard ensemble ingest databases into one ensemble database.

Reads every ``ens_shard_*.sqlite`` in the shard directory and combines them into
a single ``ensemble_transcriptions.sqlite``, offsetting each shard's ``file_id``
values so the child-table foreign keys stay consistent.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from src.rainfall_rescue_sqlite.ensemble_ingest import (
    default_ensemble_db_path,
    default_ensemble_root,
    merge_ensemble_shards,
)
from src.rainfall_rescue_sqlite.parquet_ingest import default_ensemble_parquet_root
from src.rainfall_rescue_sqlite.sqlite_staging import local_scratch_dir, publish_db


def _pdir_path(*parts: str) -> Path:
    pdir = os.environ.get("PDIR")
    if not pdir:
        raise EnvironmentError("PDIR is not set; pass the path explicitly")
    return Path(pdir).joinpath(*parts)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _glob_sql(dir_path: Path) -> str:
    return str((dir_path / "*.parquet").resolve())


def _merge_parquet_shards(
    *,
    shard_dirs: list[Path],
    output_root: Path,
    ensemble_root: Path,
    max_files: int | None,
    overwrite: bool,
) -> dict:
    if output_root.exists():
        if not overwrite:
            raise SystemExit(
                f"Parquet dataset root already exists: {output_root}. "
                "Pass --overwrite to rebuild."
            )
        shutil.rmtree(output_root)

    (output_root / "ensemble_files").mkdir(parents=True, exist_ok=True)
    (output_root / "ensemble_daily_values").mkdir(parents=True, exist_ok=True)
    (output_root / "ensemble_monthly_totals").mkdir(parents=True, exist_ok=True)
    (output_root / "ensemble_ingestion_file_errors").mkdir(parents=True, exist_ok=True)
    (output_root / "_metadata").mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()
    try:
        offsets: list[int] = []
        current_offset = 0
        shard_file_counts: list[int] = []

        for shard_dir in shard_dirs:
            n_files = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{_glob_sql(shard_dir / 'ensemble_files')}')"
            ).fetchone()[0]
            offsets.append(current_offset)
            shard_file_counts.append(int(n_files or 0))
            current_offset += int(n_files or 0)

        files_parts = []
        daily_parts = []
        totals_parts = []
        error_parts = []
        for shard_dir, offset in zip(shard_dirs, offsets):
            esc_files = _glob_sql(shard_dir / "ensemble_files").replace("'", "''")
            esc_daily = _glob_sql(shard_dir / "ensemble_daily_values").replace("'", "''")
            esc_totals = _glob_sql(shard_dir / "ensemble_monthly_totals").replace("'", "''")
            error_glob = _glob_sql(shard_dir / "ensemble_ingestion_file_errors")

            files_parts.append(
                "SELECT "
                f"file_id + {offset} AS file_id, file_name, source_path, year_start, year_end, descriptor, section_id, num_days "
                f"FROM read_parquet('{esc_files}')"
            )
            daily_parts.append(
                "SELECT "
                f"file_id + {offset} AS file_id, day_of_month, month, ensemble_member, rainfall, is_missing "
                f"FROM read_parquet('{esc_daily}')"
            )
            totals_parts.append(
                "SELECT "
                f"file_id + {offset} AS file_id, month, ensemble_member, total, is_missing "
                f"FROM read_parquet('{esc_totals}')"
            )
            if (shard_dir / "ensemble_ingestion_file_errors").exists():
                esc_errors = error_glob.replace("'", "''")
                error_parts.append(
                    "SELECT source_path, error_message "
                    f"FROM read_parquet('{esc_errors}')"
                )

        files_query = " UNION ALL ".join(files_parts) if files_parts else "SELECT * FROM (VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) WHERE 1=0"
        daily_query = " UNION ALL ".join(daily_parts) if daily_parts else "SELECT * FROM (VALUES (NULL, NULL, NULL, NULL, NULL, NULL)) WHERE 1=0"
        totals_query = " UNION ALL ".join(totals_parts) if totals_parts else "SELECT * FROM (VALUES (NULL, NULL, NULL, NULL, NULL)) WHERE 1=0"
        errors_query = (
            " UNION ALL ".join(error_parts)
            if error_parts
            else "SELECT CAST(NULL AS VARCHAR) AS source_path, CAST(NULL AS VARCHAR) AS error_message WHERE 1=0"
        )

        conn.execute(
            f"""
            COPY ({files_query})
            TO '{(output_root / 'ensemble_files' / 'part_000000.parquet').resolve()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        conn.execute(
            f"""
            COPY ({daily_query})
            TO '{(output_root / 'ensemble_daily_values' / 'part_000000.parquet').resolve()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        conn.execute(
            f"""
            COPY ({totals_query})
            TO '{(output_root / 'ensemble_monthly_totals' / 'part_000000.parquet').resolve()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        conn.execute(
            f"""
            COPY ({errors_query})
            TO '{(output_root / 'ensemble_ingestion_file_errors' / 'part_000000.parquet').resolve()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        files_ingested = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_glob_sql(output_root / 'ensemble_files')}')"
        ).fetchone()[0]
        daily_rows = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_glob_sql(output_root / 'ensemble_daily_values')}')"
        ).fetchone()[0]
        total_rows = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_glob_sql(output_root / 'ensemble_monthly_totals')}')"
        ).fetchone()[0]
        errors = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_glob_sql(output_root / 'ensemble_ingestion_file_errors')}')"
        ).fetchone()[0]
    finally:
        conn.close()

    payload = {
        "started_at": _utc_now(),
        "completed_at": _utc_now(),
        "source_root": str(ensemble_root),
        "dataset_root": str(output_root),
        "files_discovered": int(files_ingested),
        "files_ingested": int(files_ingested),
        "daily_rows": int(daily_rows),
        "total_rows": int(total_rows),
        "errors": int(errors),
        "status": "success" if int(errors) == 0 else "completed_with_errors",
        "shard_index": None,
        "num_shards": len(shard_dirs),
        "max_files": max_files,
        "part_counts": {
            "ensemble_files": 1,
            "ensemble_daily_values": 1,
            "ensemble_monthly_totals": 1,
            "ensemble_ingestion_file_errors": 1,
        },
    }
    (output_root / "_metadata" / "ingestion_run.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge ensemble ingest shards")
    parser.add_argument(
        "--backend",
        choices=("duckdb", "sqlite"),
        default="duckdb",
        help="Storage backend for merge output",
    )
    parser.add_argument("--ensemble-db-path", type=Path, default=None)
    parser.add_argument("--dataset-root", type=Path, default=None)
    parser.add_argument("--shard-dir", type=Path, default=None)
    parser.add_argument(
        "--ensemble-root",
        type=Path,
        default=None,
        help="Recorded as the source root in the merged ingestion-run row",
    )
    parser.add_argument(
        "--expected-shards",
        type=int,
        default=None,
        help="If set, fail unless exactly this many shard files are present",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional source cap recorded in parquet metadata",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing parquet dataset root",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    shard_dir = args.shard_dir or _pdir_path("ensemble_shards")
    ensemble_root = args.ensemble_root or default_ensemble_root()

    if args.backend == "sqlite":
        ensemble_db_path = args.ensemble_db_path or default_ensemble_db_path()
        shard_paths = sorted(Path(shard_dir).glob("ens_shard_*.sqlite"))
        if not shard_paths:
            raise SystemExit(f"No ens_shard_*.sqlite files found in {shard_dir}")
        if args.expected_shards is not None and len(shard_paths) != args.expected_shards:
            raise SystemExit(
                f"Expected {args.expected_shards} shards but found {len(shard_paths)} "
                f"in {shard_dir}"
            )

        local_db = local_scratch_dir() / f"merge_{os.getpid()}_ensemble.sqlite"
        result = merge_ensemble_shards(local_db, shard_paths, ensemble_root)
        publish_db(local_db, ensemble_db_path)
        print(f"Merged {len(shard_paths)} shards")
        print(result)
        print(f"Published -> {ensemble_db_path}")
        return

    shard_dirs = sorted(
        p for p in Path(shard_dir).glob("ens_shard_*") if p.is_dir()
    )
    if not shard_dirs:
        raise SystemExit(f"No ens_shard_* directories found in {shard_dir}")
    if args.expected_shards is not None and len(shard_dirs) != args.expected_shards:
        raise SystemExit(
            f"Expected {args.expected_shards} shards but found {len(shard_dirs)} "
            f"in {shard_dir}"
        )

    dataset_root = args.dataset_root or default_ensemble_parquet_root()
    payload = _merge_parquet_shards(
        shard_dirs=shard_dirs,
        output_root=dataset_root,
        ensemble_root=ensemble_root,
        max_files=args.max_files,
        overwrite=args.overwrite,
    )
    print(f"Merged {len(shard_dirs)} shards")
    print(payload)
    print(f"Published -> {dataset_root}")


if __name__ == "__main__":
    main()
