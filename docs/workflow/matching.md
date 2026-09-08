# Matching

A daily transcription on its own is just a grid of numbers — we don't know which
station or where. This stage fixes that by matching every transcription to a
Rainfall Rescue station-year and borrowing its location metadata.

Two primary notebooks:

- [match_metadata_runbook.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/match_metadata_runbook.ipynb)
- [match_metadata_allsheets.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/match_metadata_allsheets.ipynb)

Supplementary matching notebooks (diagnostics and experimentation) are listed in
the [Notebook catalog](../reference/notebooks.md).

We do matching in two passes because there are two sets of Rainfall Rescue data to match the transcriptions to. The first pass matches against the RR main output (the DATA) directory. The second pass matches only unmatched transcriptions to the less well quality-controlled data in the RR ALLSHEETS directory. The process is the same in the two cases, they just match against different data.

## The matching idea

Both the Rainfall Rescue monthly data and the transcribed daily data datasets come from the same physical stations, so their **monthly totals
should agree**. Matching compares these month-by-month:

- **RR vectors** — station-year monthly profiles from the monthly data.
- **Ensemble vectors** — monthly values from all five ensemble members.
- **Primary score** — the count of months where the RR value equals *any*
  ensemble member value (both rounded to two decimal places).
- **Tie-breaker** — the higher count of overlapping (jointly non-blank) months.

Cosine and adjusted scores are also stored, for diagnostics and comparability
with earlier versions.

Exact matching requires 9+ out of 12 months where the monthly averages from the daily sheets, and the monthly averages from Rainfall Rescue, are the same (within a tolerance of 0.01 inches). If there is a match, all 12 months should be the same, but we should allow for a few mistakes from imperfect transcriptions. Inspection shows that it is very rare for non-matching stations to have more than 4 months agree (that is - if we compare the monthly values from RR for a particular station and year, to *all* the other stations and years, very rarely will more than 4 months match even in the best case agreement) so a 9-11 month match is almost certain to be a full 12 month match, contaminated by a few transcription errors. 86% of the transcribed daily sheets end up with a match in the RR data, and so can be geolocated precisely.

## Primary matching on RR DATA

## Full-scale matching on SLURM

The notebook runs the matcher interactively on a bounded slice to stay fast.
Matching *every* transcription (~514,000 files) against *every* RR station-year
(~285,000) is a cluster job, split into shards:

```{list-table}
:header-rows: 1

* - Stage
  - Script
  - SLURM
  - Purpose
* - build
  - `scripts/slurm/build_vectors.sbatch`
  - 1 job
  - build the normalised comparison-vector Parquet files (the RAM-heavy step)
* - match
  - `scripts/slurm/match_array.sbatch`
  - array `0–99`
  - match each shard's slice against all RR candidates
* - merge
  - `scripts/slurm/merge_shards.sbatch`
  - 1 job
  - consolidate the shard results into one session
```

Launch it from a login node:

```bash
scripts/slurm/submit_all.sh
```

Pass `--skip-build` to reuse previously built vectors when only the matching
parameters have changed:

```bash
scripts/slurm/submit_all.sh --skip-build
```

Shard count, matching parameters, and the Parquet paths are configured in
`scripts/slurm/config.sh`.

## Residual matching on RR ALLSHEETS

After the primary run, a subset of files can remain unmatched. The residual
workflow compares those records against Rainfall Rescue ALLSHEETS station-year
vectors and writes a second matching session for recovery of additional exact
matches.

Run the residual pipeline with:

```bash
scripts/slurm/submit_allsheets.sh
```

This pipeline has four steps (build vectors, match shards, merge shards,
finalize metadata) and is intentionally separate so it can be rerun
independently of the main RR-DATA match.

## Assigning the metadata

With the matches in hand, each ensemble record is enriched with station metadata.
The rule is deliberately conservative — a wrong location poisons everything
downstream, so we only assign one when we are confident:

- **Exact match** — a rank-1 match with at least 9 exactly-agreeing months, and
  no more than 3 monthly values that are exactly zero in the transcription (the
  zero guard rejects spurious agreement on empty months). All metadata is copied:
  location name, year, latitude, longitude, and elevation.
- **Approximate match** — the top-3 ranks by score. A **year** is assigned only
  if all three agree on it; a **position** only if all three lie within 1.0° of
  each other (then the centroid is used). Location name and elevation stay null.
- **Unmatched** — all metadata stays null.

For SEF export, only records with trustworthy coordinates are emitted. Exact
matches from both the primary RR-DATA and residual ALLSHEETS passes can qualify.

## Diagnostics

Several scripts under `scripts/diagnostics/` let you inspect the result:

- `plot_image_consensus_metadata.py` — a one-page diagnostic for a single
  transcription: the scanned image, the consensus table, the monthly-total
  comparison against the matched RR station-year, and a map of the match.
- `plot_daily_rainfall_map.py` — every located station's consensus rainfall for a
  single date, on a UK map.
- `plot_daily_rainfall_interactive.py` — the same map as an interactive Plotly
  figure; clicking a station copies its specifier to the clipboard.
- `plot_rr_match_counts_interactive.py` — a matching-health map showing how many
  ensemble files exactly match each RR station-year, to spot "attractor"
  station-years that soak up too many matches.

```{figure} ../_static/figures/daily_rainfall_map.png
:alt: Consensus daily rainfall for every located station on a single date
:width: 60%

Consensus daily rainfall for every located station on a single date (drawn with
the same code as `plot_daily_rainfall_map.py`). Each point is one matched
station-year — over 9,000 for this day in 1931.
```

One thing that is clear from the matching is that the Daily Rainfall Records contain *many* duplicates. The most common cause for duplicated is one sheet that has been photographed twice (deliberately - both with and without the attached note stapled to the top of many of them). But there are also many station years in the record twice (2 different pages with the same data). We don't deduplicate the data at this point - everything is kept for matching and QC - but only one copy of any duplicated observation is exported for use.

Next: [Quality control](quality-control.md).
