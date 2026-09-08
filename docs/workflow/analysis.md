# Analysis

The task is done with the [SEF export](export.md) - Analysis is not part of this project. But it's not a good idea to release a dataset without checking it at all. Soi we do need to do some basic checks on the output files.Everything here
is computed from the deliverable itself — the exported SEF files — not from the
working databases.

Two notebooks:

- [analyse_sef_output.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/analyse_sef_output.ipynb)
- [generate_sef_animation.ipynb](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/generate_sef_animation.ipynb)

## Summarising the shared data

[analyse_sef_output](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/analyse_sef_output.ipynb)
produces a series of summary figures — coverage, QC pass rates, rainfall trends
and intensity, the wettest days, extreme-rainfall frequency, and a cross-check
against known historical floods and droughts. The goal is twofold: show the data
is easily usable, and surface obvious bugs before shipping.

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

Two examples from `analyse_sef_output.ipynb`:

```{figure} ../_static/figures/analysis_example_stations.png
:alt: Number of reporting stations over time in the SEF analysis
:width: 75%

Reporting station count through time from the SEF analysis workflow. The strong
growth of the network is why coverage caveats matter for national aggregates.
```

```{figure} ../_static/figures/analysis_example_annual_total.png
:alt: Annual national-mean rainfall total from the SEF analysis
:width: 75%

Annual national-mean rainfall total (station-mean), as used in the notebook's
trend sanity checks.
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

[generate_rainfall_animation](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/generate_rainfall_animation.ipynb)
animates the **consensus** daily rainfall (median over the five members, values
in **inches**), styled like the static maps. Start with a single test year, then
raise the shard count for the full record:

```bash
scripts/slurm/submit_animation.sh
```

### Shared (SEF) animation

[generate_sef_animation](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/blob/main/notebooks/generate_sef_animation.ipynb)
animates the data **exactly as it is shared** — reading only the SEF `.tsv` files.
It is the counterpart to the consensus animation, with two differences: values
are in **millimetres**, and **QC is made visible** — a station that passed either
check is drawn with its value, while one that failed both is drawn as a red error
cross and excluded from the field.

```bash
scripts/slurm/submit_sef_animation.sh
```

```{raw} html
<div style="position: relative; padding-bottom: 56.25%; height: 0; overflow: hidden; max-width: 100%; margin: 1rem 0;">
  <iframe
    src="https://player.vimeo.com/video/1223307103"
    title="Shared SEF rainfall animation"
    frameborder="0"
    allow="autoplay; fullscreen; picture-in-picture"
    allowfullscreen
    style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"
  ></iframe>
</div>
```

Shared (SEF) rainfall animation (hosted on Vimeo):
[https://vimeo.com/1223307103](https://vimeo.com/1223307103)

The analysis results are encouraging - the exported data are sensible and plausible. There's a lot of scope for further analysis, but we're not doing that here.
