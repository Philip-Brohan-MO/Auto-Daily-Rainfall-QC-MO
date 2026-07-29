# Architecture

## The data backend

The pipeline is built on **[Parquet](https://parquet.apache.org/) datasets
queried with [DuckDB](https://duckdb.org/)**. Columnar Parquet files with
row-group pruning let each SLURM shard read only the slice it needs and keep its
memory bounded, which is what makes the array + merge pattern work over datasets
far too large to hold in RAM. (An earlier iteration used SQLite; the `parquet_*`
modules are the current backend.)

All datasets live under `$PDIR` (see the [SLURM reference](slurm.md)).

## Module overview

The reusable logic lives in the `rainfall_rescue_sqlite` package; the notebooks
and `scripts/` entry points are thin wrappers around it.

```text
src/rainfall_rescue_sqlite/
├── parquet_ingest.py            Rainfall Rescue + ensemble JSON → Parquet
├── ensemble_parser.py           Parse an ensemble transcription JSON file
├── parquet_similarity.py        Build vectors, match RR ↔ ensemble
├── comparison_baseline.py       Similarity scoring (exact-month agreement)
├── assign_ensemble_metadata.py  Copy RR metadata onto matched records
├── parquet_qc_exact_monthly.py  QC1: monthly-total consistency
├── parquet_regional_stats.py    QC2 stage 1: regional neighbour statistics
├── parquet_secondary_qc.py      QC2 stage 2: XGBoost expectation-range check
├── sef_export.py                Merge duplicates → SEF .tsv files
├── sef_analysis.py              Parse SEF → analysis Parquet + aggregates
├── rainfall_animation.py        Consensus-rainfall frame rendering
└── sef_animation.py             SEF-rainfall frame rendering
```

## Data flow

```text
Rainfall Rescue CSV        Ensemble transcription JSON
        │                              │
        ▼                              ▼
   parquet_ingest.py             parquet_ingest.py
   RR monthly Parquet            ensemble Parquet (5 members / cell)
        │                              │
        └──────────────┬───────────────┘
                       ▼
              parquet_similarity.py            match each transcription
              assign_ensemble_metadata.py      to an RR station-year, borrow
                       │                        location metadata
                       ▼
              parquet_qc_exact_monthly.py       QC1 (monthly totals)
                       │
                       ▼
              daily_consensus  →  parquet_regional_stats.py     QC2 stage 1
                       │
                       ▼
              parquet_secondary_qc.py           QC2 stage 2 (XGBoost)
                       │
                       ▼
                 sef_export.py                  → SEF .tsv (mm, QC verdicts)
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼               ▼
 sef_analysis.py  rainfall_animation  sef_animation
 summary figures     .mp4 (inches)    .mp4 (mm, QC shown)
```

## Key design decisions

### Consensus, not a single reading

Every station-year was transcribed five times by the parent project. The
pipeline works with the **median across those five members** as the consensus
value, and uses the spread between members as a signal that quality control can
act on.

### Conservative georeferencing

A wrong station location would corrupt the regional QC, the maps, and the SEF
export. Metadata is therefore only assigned on a confident match — an exact
rank-1 agreement, or a tightly-clustered top-3 centroid — and left null
otherwise. See [Matching](../workflow/matching.md).

### Two independent QC checks

QC1 (monthly totals) and QC2 (regional neighbours) test different things and rely
on different data, so an observation that passes both is trustworthy for
different reasons. QC2 also acts as a rescue path for observations QC1 rejects, so
a single bad month doesn't discard a whole file's good days. See
[Quality control](../workflow/quality-control.md).

### Analyse only the deliverable

The analysis and SEF-animation stages read **only the exported SEF files**,
nothing from the working databases. This validates the actual shared product and
catches export bugs that upstream checks would miss.
