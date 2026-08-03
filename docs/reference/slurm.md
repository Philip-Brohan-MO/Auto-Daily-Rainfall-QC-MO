# SLURM and HPC

This guide covers running the pipeline at scale on the
[SPICE HPC cluster](https://www.metoffice.gov.uk/) with the
[SLURM](https://slurm.schedmd.com/) scheduler. The notebooks demonstrate each
stage on a small slice locally; the full dataset — hundreds of thousands of
station-years and hundreds of millions of station-days — is processed here.

## The sharding pattern

Almost every heavy stage follows the same **array + merge** shape, because the
work is naturally parallel over independent units (files, station-years, or
frames):

1. An optional **build/precompute** job prepares shared inputs once (the
   RAM-heavy step).
2. An **array job** splits the work into `N` shards that run in parallel, each
   writing a private output file.
3. A single **merge** job consolidates the shard outputs.

The stages are chained with `--dependency=afterok`, so each starts only when the
previous one has succeeded, and a failed shard can be re-run on its own without
disturbing the others.

## Configuration

Every parameter — paths, shard counts, matching and QC parameters, and the
per-stage resource requests (cores, memory, wall-clock time, QOS) — lives in a
single file:

```text
scripts/slurm/config.sh
```

The `submit_*.sh` driver scripts source this file and pass the values to `sbatch`
as CLI flags, so they override the `#SBATCH` defaults baked into each `.sbatch`
file. Most values can also be overridden inline at submit time, for example:

```bash
ENSEMBLE_ROOT=/path/to/json ENSEMBLE_MAX_FILES=500 \
    scripts/slurm/submit_ensemble_ingest.sh
```

Two paths anchor everything:

| Variable | Meaning | Default |
|----------|---------|---------|
| `REPO_ROOT` | the repository checkout | `…/Auto-Daily-Rainfall-QC-MO` |
| `PDIR` | shared disc for all datasets, shards and logs | `/data/scratch/philip.brohan/ADRQ` |

## The pipelines

Each workflow stage has a `submit_*.sh` driver. Run them from a login node with
the repository root as the working directory.

| Stage | Submit script | Shape |
|-------|---------------|-------|
| Ensemble ingest | `submit_ensemble_ingest.sh` | array + merge |
| Similarity matching | `submit_all.sh` | build + array + merge |
| QC 1 (monthly total) | `submit_qc.sh` | array + merge |
| Daily consensus | `submit_daily_consensus.sh` | array + merge |
| QC 2 stage 1 (regional stats) | `submit_regional_stats.sh` | array + merge |
| QC 2 stage 2 (secondary ML) | `submit_secondary_qc.sh` | train + score |
| SEF export | `submit_sef_export.sh` | array (no merge) |
| SEF analysis dataset | `submit_sef_analysis.sh` | array (no merge) |
| Consensus animation | `submit_animation.sh` | precompute + array + validate + encode |
| SEF animation | `submit_sef_animation.sh` | precompute + array + validate + encode |

See the [scripts reference](scripts.md) for the individual `.sbatch` files behind
each driver, and the [workflow pages](../workflow/overview.md) for the context in
which each is run.

### Ordering constraints

Most stages depend on the output of the one before it, so run the notebooks (and
their submit scripts) in order. Two dependencies are worth calling out:

- **Regional stats** requires the **daily-consensus** table first — run
  `submit_daily_consensus.sh` before `submit_regional_stats.sh`. The latter
  guards on the consensus table existing and exits with instructions if it is
  missing.
- **The secondary ML check** requires both **QC1** (`daily_qc_status`) and **QC2
  stage 1** (the regional-stats table) to exist.

## Monitoring jobs

```bash
squeue -u $USER                                              # queued / running
sacct --format=JobID%15,JobName%14,State,ExitCode,Elapsed   # final states
ls -t $PDIR/slurm_logs | head                               # newest per-job logs
```

A pipeline is finished when the queue is empty and the expected output (a merged
Parquet dataset, a tree of `.tsv` files, or an `.mp4`) appears under `$PDIR`.
