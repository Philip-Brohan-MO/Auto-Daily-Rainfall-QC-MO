"""SQLite ingestion utilities for Rainfall Rescue and ensemble transcription data."""

from .comparison_baseline import (
	build_comparison_vectors,
	merge_shard_matches,
	run_baseline_matching,
	run_matching_shard,
)
from .ensemble_ingest import ingest_ensemble_json
from .ingest import ingest_combined_csvs
from .parquet_ingest import ingest_ensemble_to_parquet, ingest_rainfall_rescue_to_parquet
from .parquet_similarity import build_comparison_vectors_parquet, run_baseline_matching_parquet
from .parquet_qc_exact_monthly import (
	merge_exact_monthly_qc_shards_parquet,
	run_exact_monthly_consistency_check_parquet,
	run_exact_monthly_consistency_shard_parquet,
)
from .qc_exact_monthly import run_exact_monthly_consistency_check

__all__ = [
	"ingest_combined_csvs",
	"ingest_ensemble_json",
	"ingest_rainfall_rescue_to_parquet",
	"ingest_ensemble_to_parquet",
	"build_comparison_vectors_parquet",
	"run_baseline_matching_parquet",
	"run_exact_monthly_consistency_check_parquet",
	"run_exact_monthly_consistency_shard_parquet",
	"merge_exact_monthly_qc_shards_parquet",
	"build_comparison_vectors",
	"run_baseline_matching",
	"run_matching_shard",
	"merge_shard_matches",
	"run_exact_monthly_consistency_check",
]
