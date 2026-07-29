# Workflow overview

From a user's point of view, the whole project is controlled through a series of
[Jupyter notebooks](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/tree/main/notebooks).
Each notebook documents and drives one stage of the pipeline: it explains the
stage, runs a small demonstration on a bounded slice of data so the notebook
stays fast, and then shows how the same work is submitted at full scale to the
SPICE cluster with [SLURM](../reference/slurm.md). To reproduce or extend the
project, you open the notebooks and run them in order.

```{important}
Run every notebook with the `ADRQ` kernel (`conda activate ADRQ`). See
[Installation](../installation.md).
```

## The stages

The notebooks are grouped into five stages, run top to bottom:

```text
ingestion/            Load the Rainfall Rescue monthly data and the daily
      │               ensemble transcriptions into Parquet / DuckDB
      ▼
matching/             Match each daily transcription to a Rainfall Rescue
      │               station-year and borrow its location metadata
      ▼
quality control/      Check daily values against monthly totals (QC1) and
      │               against regional neighbours (QC2)
      ▼
export/               Merge duplicates and write one SEF .tsv per station-year,
      │               in millimetres, carrying the QC verdicts
      ▼
analysis/             Summarise the shared dataset and animate the rainfall field
```

Two ideas run through the whole workflow:

- **Consensus from the ensemble.** Every station-year was transcribed five times
  by the parent project. The *consensus* value for a day is the median across
  those five members. Where the members disagree we can see it, and quality
  control can act on it.
- **Borrow metadata, don't invent it.** A daily transcription only gets a
  location when it matches a Rainfall Rescue station-year closely enough to be
  confident it is the same station. Everything downstream — regional QC, mapping,
  the SEF export — depends on that georeferencing being trustworthy.

## The notebooks, in order

| # | Stage | Notebook | What it does |
|---|-------|----------|--------------|
| 1 | Ingestion | [RR_data_ingest](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/RR_data_ingest.ipynb) | Fetch the Rainfall Rescue monthly data and ingest it into Parquet |
| 2 | Ingestion | [Daily_transcriptions_ingest](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/Daily_transcriptions_ingest.ipynb) | Ingest the daily ensemble transcription JSON into Parquet |
| 3 | Matching | [match_metadata](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/match_metadata.ipynb) | Match each transcription to an RR station-year and assign metadata |
| 4 | Quality control | [qc_RR_monthly_total](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/qc_RR_monthly_total.ipynb) | QC1: check daily totals against the matched RR monthly value |
| 5 | Quality control | [qc_RR_regional_stats](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/qc_RR_regional_stats.ipynb) | QC2 stage 1: compute regional neighbour statistics |
| 6 | Quality control | [qc_RR_secondary_ml](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/qc_RR_secondary_ml.ipynb) | QC2 stage 2: re-examine QC1 failures with an ML model |
| 7 | Export | [export_sef](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/export_sef.ipynb) | Export the QC'd data to Station Exchange Format `.tsv` files |
| 8 | Analysis | [analyse_sef_output](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/analyse_sef_output.ipynb) | Summary figures computed only from the exported SEF files |
| 9 | Analysis | [generate_rainfall_animation](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/generate_rainfall_animation.ipynb) | Animated map of consensus daily rainfall |
| 10 | Analysis | [generate_sef_animation](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/generate_sef_animation.ipynb) | Animated map from the shared SEF data, showing QC verdicts |

Work through the stage pages next, starting with [Ingestion](ingestion.md).

## How each notebook is structured

Most notebooks follow the same shape, so once you have read one the rest are
familiar:

1. **A demonstration run** on a small, bounded slice of data, executed inline so
   the notebook produces real output on a workstation in seconds to minutes.
2. **A hand-verification** step that independently recomputes a result to confirm
   the module is doing what it claims.
3. **The full-scale SLURM submission** — the exact `scripts/slurm/submit_*.sh`
   command that runs the same stage over the whole dataset, plus the commands to
   monitor the jobs and inspect the merged output.
4. **Interactive diagnostics** — Plotly maps you can click to inspect individual
   stations, backed by the scripts under `scripts/diagnostics/`.
