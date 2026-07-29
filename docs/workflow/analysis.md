# Analysis

The final stage checks that the shared dataset is **useful** and free of hidden
bugs, and brings the rescued rainfall to life as animated maps. Everything here
is computed from the deliverable itself — the exported SEF files — not from the
working databases.

Three notebooks:

- [analyse_sef_output.ipynb](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/analyse_sef_output.ipynb)
- [generate_rainfall_animation.ipynb](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/generate_rainfall_animation.ipynb)
- [generate_sef_animation.ipynb](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/generate_sef_animation.ipynb)

## Summarising the shared data

[analyse_sef_output](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/analyse_sef_output.ipynb)
produces a series of summary figures — coverage, QC pass rates, rainfall trends
and intensity, the wettest days, extreme-rainfall frequency, and a cross-check
against known historical floods and droughts. The goal is twofold: show the data
is useful, and surface bugs (a bad transcription batch, a units slip, a QC edge
case) before shipping.

Reading the raw `.tsv` tree for every figure would be slow, so the SEF files are
first parsed **once** into a compact Parquet analysis dataset (`observations`
plus precomputed `daily_national` aggregates) that the notebook then queries
cheaply with DuckDB. Build that dataset in parallel on the cluster — one array
task per SEF year, no merge stage — with:

```bash
scripts/slurm/submit_sef_analysis.sh
```

```{important}
The station network grows enormously over the record, from a handful of gauges
to thousands. The national series are simple means over the reporting stations,
so absolute levels are coverage-influenced; coverage-sensitive metrics are shown
as rates per reporting station. Treat between-era comparisons with care.
```

## Animating the rainfall field

Two notebooks turn the daily rainfall into a smoothed animated map of the UK,
interpolating several frames between each day so the field appears to evolve
continuously. Rendering a full run is thousands of frames, so the work is done on
the cluster and only the finished MP4 is brought back for display.

Both use the same four-stage SLURM pipeline, chained with `--dependency=afterok`:

```{list-table}
:header-rows: 1

* - Stage
  - SLURM
  - Purpose
* - precompute
  - 1 job
  - write `manifest.json` describing every frame and the shard boundaries
* - render
  - array `0–(N-1)`
  - render each shard's contiguous block of frames in parallel
* - validate
  - 1 job
  - confirm every expected frame exists
* - encode
  - 1 job
  - `ffmpeg` the frame sequence into an H.264 MP4
```

Every frame has a global index fixed by the date range and the frames-per-day
setting, so shards never collide and any failed shard can be re-run on its own.

### Consensus animation

[generate_rainfall_animation](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/generate_rainfall_animation.ipynb)
animates the **consensus** daily rainfall (median over the five members, values
in **inches**), styled like the static maps. Start with a single test year, then
raise the shard count for the full record:

```bash
scripts/slurm/submit_animation.sh
```

### Shared (SEF) animation

[generate_sef_animation](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/generate_sef_animation.ipynb)
animates the data **exactly as it is shared** — reading only the SEF `.tsv` files.
It is the counterpart to the consensus animation, with two differences: values
are in **millimetres**, and **QC is made visible** — a station that passed either
check is drawn with its value, while one that failed both is drawn as a red error
cross and excluded from the field.

```bash
scripts/slurm/submit_sef_animation.sh
```

```{figure} ../_static/figures/sef_animation_frame.png
:alt: A single frame from the shared SEF rainfall animation
:width: 60%

A single frame from the shared (SEF) rainfall animation. Values are in
millimetres, and stations that failed both QC checks are marked with a red cross
rather than a rainfall value.
```

This is the end of the workflow. To reproduce or extend any part of it, see
[How to reproduce and extend](../reproduce.md).
