# Transcription QC

Before similarity matching, the ingested transcription dataset gets a dedicated
quality-control pass focused on transcription structure and internal
consistency. This catches malformed or suspicious records early and produces a
cleaner input set for matching.

This stage is script-driven (no dedicated notebook page yet):

- `scripts/run_transcription_qc_shard.py`
- `scripts/merge_transcription_qc_shards.py`
- `scripts/slurm/submit_transcription_qc.sh`

## What this stage does

The transcription-QC pass runs over the ingested ensemble records in shards and
writes per-record QC outputs that summarise checks such as missing-day structure
and transcription completeness indicators used for downstream diagnostics.

Outputs are written as sessioned Parquet tables under the configured
transcription-QC root, so later stages can resolve the latest run automatically
or pin a specific session.

## Full-scale run on SLURM

The cluster run follows the familiar array-plus-merge pattern:

```{list-table}
:header-rows: 1

* - Stage
  - Script
  - SLURM
  - Purpose
* - qc
  - `scripts/slurm/transcription_qc_array.sbatch`
  - array `0–(N-1)`
  - run transcription QC over each shard's `file_id` slice
* - merge
  - `scripts/slurm/transcription_qc_merge.sbatch`
  - 1 job
  - combine shard outputs into one sessioned result
```

Launch from a login node:

```bash
scripts/slurm/submit_transcription_qc.sh
```

Shard count, paths, and resources are configured in `scripts/slurm/config.sh`.
Use the [SLURM reference](../reference/slurm.md) for monitoring and reruns.

## Why this stage is separate

Keeping transcription QC as its own stage has two benefits:

- It isolates transcription-quality diagnostics from similarity-scoring logic.
- It allows quick reruns of matching with fixed transcription inputs while
  preserving transcription-QC session history.

Next: [Matching](matching.md).