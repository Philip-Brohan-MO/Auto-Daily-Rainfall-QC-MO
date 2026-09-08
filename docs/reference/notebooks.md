# Notebook catalog

This page is the notebook index for contributors and operators. The workflow
pages focus on production stages; this catalog also lists diagnostics,
experimentation, and operations notebooks.

## Primary workflow notebooks

Run these in the stage order shown in the [workflow overview](../workflow/overview.md).

Transcription QC sits between ingestion and matching and is currently
script-driven (`submit_transcription_qc.sh`) rather than notebook-driven.

| Stage | Notebook | Purpose |
|------|----------|---------|
| Ingestion | [RR_data_ingest.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/RR_data_ingest.ipynb) | Rainfall Rescue monthly ingest to Parquet |
| Ingestion | [Daily_transcriptions_ingest.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/Daily_transcriptions_ingest.ipynb) | Ensemble JSON ingest to Parquet |
| Matching | [match_metadata_runbook.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/match_metadata_runbook.ipynb) | Primary RR-DATA matching and metadata assignment |
| Matching | [match_metadata_allsheets.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/match_metadata_allsheets.ipynb) | Residual ALLSHEETS matching for unmatched files |
| Quality control | [qc_RR_monthly_total.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/qc_RR_monthly_total.ipynb) | QC1 monthly-total consistency check |
| Quality control | [qc_RR_regional_stats.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/qc_RR_regional_stats.ipynb) | QC2 stage 1 regional-neighbour statistics |
| Quality control | [qc_RR_secondary_ml.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/qc_RR_secondary_ml.ipynb) | QC2 stage 2 secondary ML check |
| Export | [export_sef.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/export_sef.ipynb) | SEF export generation |
| Analysis | [analyse_sef_output.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/analyse_sef_output.ipynb) | SEF summary analysis |
| Analysis | [generate_rainfall_animation.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/generate_rainfall_animation.ipynb) | Consensus rainfall animation |
| Analysis | [generate_sef_animation.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/generate_sef_animation.ipynb) | SEF rainfall animation with QC display |

## Diagnostics and sandbox notebooks

Use these for inspection and experimentation, not as the canonical production
path.

| Notebook | Purpose |
|----------|---------|
| [match_metadata_diagnostics.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/match_metadata_diagnostics.ipynb) | Matching diagnostics and exploratory checks |
| [match_metadata_sandbox.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/match_metadata_sandbox.ipynb) | Sandbox for matching experiments |

## Operations notebook

| Notebook | Purpose |
|----------|---------|
| [operations.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/operations.ipynb) | Operator checklist for orchestration, reruns, and tuning |
