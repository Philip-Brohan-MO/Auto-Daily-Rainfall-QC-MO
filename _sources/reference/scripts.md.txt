# Scripts and SLURM jobs

The `scripts/` directory holds the command-line entry points that the notebooks
call and the SLURM jobs run. This page is an inventory; for how they fit together
see the [workflow overview](../workflow/overview.md) and the
[SLURM reference](slurm.md).

## Processing scripts (`scripts/`)

These are plain Python entry points, each a thin wrapper around a module in
`src/rainfall_rescue_sqlite/`. A single SLURM array task typically runs one of
these for its shard.

| Script | Stage | Purpose |
|--------|-------|---------|
| `build_rainfall_rescue_parquet.py` | Ingestion | Rainfall Rescue CSV → Parquet |
| `build_ensemble_transcriptions_parquet.py` | Ingestion | Ensemble JSON → Parquet |
| `run_ensemble_ingest_shard.py` / `merge_ensemble_shards.py` | Ingestion | Sharded ensemble ingest and merge |
| `build_similarity_vectors.py` | Matching | Build the normalised comparison vectors |
| `run_similarity_shard.py` / `merge_similarity_shards.py` | Matching | Sharded matching and merge |
| `run_monthly_similarity_baseline.py` | Matching | Baseline similarity scoring |
| `assign_ensemble_metadata.py` | Matching | Copy RR metadata onto matched records |
| `run_qc_check_exact_monthly.py` / `run_qc_shard.py` / `merge_qc_shards.py` | QC1 | Monthly-total check, sharded, and merge |
| `run_daily_consensus_shard.py` / `merge_daily_consensus_shards.py` | QC2 | Build the daily-consensus table |
| `run_regional_stats_shard.py` / `merge_regional_stats_shards.py` | QC2 | Regional neighbour statistics |
| `train_secondary_qc_models.py` / `score_secondary_qc.py` | QC2 | Train and score the secondary ML check |
| `export_sef.py` | Export | Merge duplicates → SEF `.tsv` |
| `build_sef_analysis_parquet.py` | Analysis | Parse SEF → analysis Parquet |
| `render_animation_manifest.py` / `render_animation_shard.py` / `render_animation_validate.py` / `render_animation_encode.py` | Analysis | Consensus-rainfall animation stages |
| `render_sef_animation_manifest.py` / `render_sef_animation_shard.py` | Analysis | SEF-rainfall animation stages |

## SLURM jobs (`scripts/slurm/`)

Each pipeline has a `submit_*.sh` driver that sources `config.sh` and submits the
`.sbatch` files below with the right dependencies.

| Driver | `.sbatch` files | Shape |
|--------|-----------------|-------|
| `submit_ensemble_ingest.sh` | `ingest_ensemble_array`, `merge_ensemble_shards` | array + merge |
| `submit_all.sh` | `build_vectors`, `match_array`, `merge_shards` | build + array + merge |
| `submit_qc.sh` | `qc_array`, `qc_merge` | array + merge |
| `submit_daily_consensus.sh` | `daily_consensus_array`, `daily_consensus_merge` | array + merge |
| `submit_regional_stats.sh` | `regional_stats_array`, `regional_stats_merge` | array + merge |
| `submit_secondary_qc.sh` | `secondary_qc_train`, `secondary_qc_score` | train + score |
| `submit_sef_export.sh` | `sef_export_array` | array |
| `submit_sef_analysis.sh` | `sef_analysis_array` | array |
| `submit_animation.sh` | `render_precompute`, `render_array`, `render_validate`, `render_encode` | 4-stage |
| `submit_sef_animation.sh` | `sef_render_precompute`, `sef_render_array`, `sef_render_validate`, `sef_render_encode` | 4-stage |

`config.sh` is the single source of truth for paths, shard counts, parameters and
resources; see the [SLURM reference](slurm.md).

## Diagnostic scripts (`scripts/diagnostics/`)

These build the maps and figures the notebooks embed. Most have a `build_figure`
function the notebooks import, and a `__main__` block so they can be run from the
command line to write a standalone image or self-contained interactive HTML.

| Script | Produces |
|--------|----------|
| `plot_image_consensus_metadata.py` | One-page per-transcription diagnostic (image, consensus table, monthly comparison, map) |
| `plot_daily_rainfall_map.py` | Static UK map of consensus rainfall for a date |
| `plot_daily_rainfall_interactive.py` | Interactive (Plotly) version of the daily map |
| `plot_daily_qc_interactive.py` | Interactive map coloured by QC1 flag |
| `plot_regional_stat_interactive.py` | Interactive map of any regional statistic |
| `plot_secondary_qc_interactive.py` | Interactive map coloured by the secondary-QC flag |
| `plot_rr_match_counts_interactive.py` | Matching-health map: exact matches per RR station |
| `plot_rank1_exact_agreement_distribution.py` | Distribution of rank-1 exact-agreement counts |

For example:

```bash
python scripts/diagnostics/plot_daily_rainfall_map.py 1931-10-15 \
    --output daily_map.webp
```
