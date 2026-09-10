# Auto Daily Rainfall QC

This is the third of a set of three projects demonstrating a 100% AI method to do large-scale [Climate Data Rescue](https://climate.copernicus.eu/sites/default/files/2020-02/BestPracticeGuidelines_ClimateDataRescue_0.pdf):

- **[Robot Rainfall Rescue](https://brohan.org/Robot_Rainfall_Rescue/)** demonstrated the basic approach: How to fine-tune an ensemble of small Vision Language Models to convert a large collection of photographs of historical documents containing numerical weather records into computer-readable form.
- **[Auto Daily Rainfall](https://brohan.org/Auto-Daily-Rainfall/)** applied the approach to the 660,000 pages of the UK Daily Rainfall Reports (England and Wales). It demonstrated fine-tuning without any training data, and produced a full ensemble transcription.
- **This Project** takes the raw transcriptions, applies metadata (locations and dates), does basic QC and deduplication, and outputs [73 million daily rainfall observations](https://doi.org/10.5281/zenodo.21905160) as ready-to-use [Station Exchange Format (SEF)](https://datarescue.climate.copernicus.eu/station-exchange-format-sef) files.

The preceding project, [Auto Daily Rainfall](https://brohan.org/Auto-Daily-Rainfall/), outputs [an ensemble of five transcriptions of each page in the UK Daily Rainfall Reports (England and Wales)](https://doi.org/10.5281/zenodo.22078935). These transcriptions are the inputs to this project, but the raw transcriptions are not yet usable science. Before the recovered numbers can be used they need three more steps of processing:

1. **Location.** A transcription on its own is just a grid of numbers. We match
   each one to the already-digitised
   [Rainfall Rescue](https://climatelabbook.substack.com/p/rainfall-rescue-5-years-on)
   monthly records, which carry station names and coordinates, so every daily
   record becomes a *georeferenced* station-year.
2. **Quality control.** We check the daily values two independent ways — against
   the station's own monthly totals, and against what neighbouring stations
   recorded on the same day — and attach a quality verdict to every observation.
3. **Sharing.** We export the located, quality-controlled data in the
   [Station Exchange Format (SEF)](https://datarescue.climate.copernicus.eu/station-exchange-format-sef),
   the community standard for rescued climate observations.

This project performs these steps, and outputs [73 million daily rainfall observations in ready-to-use format](https://doi.org/10.5281/zenodo.21905160). ([Watch the Video](https://vimeo.com/1223307103))

## The approach

As with the preceding project, the whole thing is **driven by a series of
notebooks** that you can open, read, and run. Each notebook documents one stage
of the pipeline: it explains what the stage does, runs a small demonstration
locally, and submits the full-scale work to the
[SPICE HPC cluster](reference/slurm.md) via SLURM. Five transcriptions of each of 365 days data, from each of 660,000 page records, means we have more than 1 billion observation records to work on. So we can't just load the whole dataset into a laptop, some engineering effort is required to do the work efficiently and here we have parallelised as much as possible using the Met Office SPICE cluster. (It is possible to run the whole process on a single powerful system - see the local-parallel branch in the repository for an example).

Two ideas run through the workflow:

- **Consensus from the ensemble.** Every station-year was transcribed five
  times. The *consensus* value for a day is the median across those five
  members; where the members disagree we can see it, and quality control can act
  on it.
- **Borrow metadata, don't invent it.** We never guess a station's location. We
  only assign coordinates when a daily record matches a Rainfall Rescue
  station-year closely enough to be confident it is the same station.

The [workflow overview](workflow/overview.md) explains how the notebooks fit
together; the pages under it walk through each stage.

## The pipeline at a glance

| Stage | Notebooks | What happens |
|-------|-----------|--------------|
| [Ingestion](workflow/ingestion.md) | 2 | Load the Rainfall Rescue monthly data and the daily ensemble transcriptions into a Parquet/DuckDB backend |
| [Transcription QC](workflow/transcription-qc.md) | 0 | Check and summarise transcription structure/quality before similarity matching |
| [Matching](workflow/matching.md) | 2 primary (+2 appendix) | Match transcriptions to Rainfall Rescue station-years, then run residual ALLSHEETS matching |
| [Quality control](workflow/quality-control.md) | 3 | Check daily values against monthly totals (QC1) and against regional neighbours (QC2) |
| [Export](workflow/export.md) | 1 | Merge duplicates and write one SEF `.tsv` per real station-year, in millimeters, with QC verdicts |
| [Analysis](workflow/analysis.md) | 3 | Summarise the shared dataset and animate the daily rainfall field |

## Do-It-Yourself

- [Installation](installation.md) — set up the `ADRQ` environment.
- [Workflow overview](workflow/overview.md) — the staged, notebook-driven pipeline.
- [How to reproduce and extend](reproduce.md) — code & compute.

## Results

 - [73 million England and Wales daily-rainfall observations, ready for use.](https://doi.org/10.5281/zenodo.21905160)
 - [Video of England and Wales rainfall, 1871-1960.](https://vimeo.com/1223307103)

## Credits

- [Authors and acknowledgements](credits.md)

This document is distributed under the terms of the [Open Government Licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/). Source code included is distributed under the terms of the [BSD license](https://opensource.org/licenses/BSD-2-Clause).


```{toctree}
:maxdepth: 2
:hidden:
:caption: Getting started

installation
workflow/overview
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: The workflow

workflow/ingestion
workflow/transcription-qc
workflow/matching
workflow/quality-control
workflow/export
workflow/analysis
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: Reference

reference/slurm
reference/architecture
reference/scripts
reference/notebooks
reproduce
credits
```
