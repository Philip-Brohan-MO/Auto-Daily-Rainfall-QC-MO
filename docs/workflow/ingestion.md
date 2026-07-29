# Ingestion

Before anything can be matched or checked, the two source datasets have to be
loaded into a fast, columnar backend. This stage does that, converting both into
[Parquet](https://parquet.apache.org/) datasets that the rest of the pipeline
queries with [DuckDB](https://duckdb.org/).

Two notebooks:

- [RR_data_ingest.ipynb](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/RR_data_ingest.ipynb)
- [Daily_transcriptions_ingest.ipynb](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/Daily_transcriptions_ingest.ipynb)

## The two data sources

The pipeline joins together two independent datasets:

- **Rainfall Rescue monthly data** — the volunteer-digitised *monthly* rainfall
  totals from [Ed Hawkins' Rainfall Rescue project](https://github.com/ed-hawkins/rainfall-rescue).
  Crucially, these records already carry **station names, coordinates, and other
  metadata**. They are the anchor we use to locate the daily transcriptions.
- **Daily ensemble transcriptions** — the output of the parent
  [Auto Daily Rainfall](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-MO)
  project: one JSON file per station-year image, holding keys `Day 1` … `Day 31`
  plus a `Totals` block, with **five ensemble-member values** for every day and
  month slot.

## Rainfall Rescue monthly data

The [RR_data_ingest](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/RR_data_ingest.ipynb)
notebook clones the Rainfall Rescue data from GitHub and ingests the combined
station CSV files into a Parquet dataset for fast DuckDB processing. It then
draws an interactive map of every station's location for a chosen year and month,
coloured by that month's rainfall, so you can confirm the ingest looks right.

This dataset is small enough to ingest in a single process; no cluster job is
needed.

## Daily ensemble transcriptions

The [Daily_transcriptions_ingest](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/Daily_transcriptions_ingest.ipynb)
notebook rebuilds the ensemble Parquet dataset from the JSON transcription files.
The in-process rebuild shown first is fine for smoke tests, but the full dataset
is far too large for one Python process.

Because each JSON file is self-contained, the work is **embarrassingly
parallel**: the sorted file list is split into shards (default 100) that each
parse their own slice, followed by a single merge step that renumbers the
`file_id` values and writes one consolidated dataset.

```{list-table}
:header-rows: 1

* - Stage
  - Script
  - SLURM
  - Purpose
* - ingest
  - `scripts/slurm/ingest_ensemble_array.sbatch`
  - array `0–99`
  - parse each shard's JSON slice in parallel
* - merge
  - `scripts/slurm/merge_ensemble_shards.sbatch`
  - 1 job
  - combine shards into one Parquet dataset
```

Launch the whole pipeline from a login node, pointing `ENSEMBLE_ROOT` at the JSON
tree to ingest:

```bash
ENSEMBLE_ROOT=/path/to/ensemble_transcriptions \
    scripts/slurm/submit_ensemble_ingest.sh
```

The two jobs are chained with `--dependency=afterok`, so the merge only runs once
every shard has succeeded. Shard count, an optional file cap for testing, and the
per-job resources are all set in `scripts/slurm/config.sh`. See the
[SLURM reference](../reference/slurm.md) for how to monitor jobs.

Next: [Matching](matching.md).
