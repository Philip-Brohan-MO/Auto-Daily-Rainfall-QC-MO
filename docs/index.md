# Auto Daily Rainfall QC

**Locating, quality-controlling, and sharing hundreds of thousands of
machine-transcribed daily rainfall observations rescued from historical
documents.**

---

This is the quality-control follow-on to
[Auto Daily Rainfall](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-MO),
which used an ensemble of small
[vision-language models (VLMs)](https://huggingface.co/blog/vlms) to read about
**660,000** scanned daily rainfall registers into structured JSON — five
independent transcriptions of every station-year image.

Raw transcriptions are not yet usable science. Before the recovered numbers can
be trusted or shared they need three things, and that is what this project does:

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

## The approach

As with the parent project, the whole thing is **driven by a series of
notebooks** that you can open, read, and run. Each notebook documents one stage
of the pipeline: it explains what the stage does, runs a small demonstration
locally, and submits the full-scale work to the
[SPICE HPC cluster](reference/slurm.md) via SLURM.

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
| [Matching](workflow/matching.md) | 1 | Match each daily transcription to a Rainfall Rescue station-year and borrow its location metadata |
| [Quality control](workflow/quality-control.md) | 3 | Check daily values against monthly totals (QC1) and against regional neighbours (QC2) |
| [Export](workflow/export.md) | 1 | Merge duplicates and write one SEF `.tsv` per real station-year, in millimetres, with QC verdicts |
| [Analysis](workflow/analysis.md) | 3 | Summarise the shared dataset and animate the daily rainfall field |

## Get started

- [Installation](installation.md) — set up the `ADRQ` environment.
- [Workflow overview](workflow/overview.md) — the staged, notebook-driven pipeline.
- [How to reproduce and extend](reproduce.md) — code, compute, and credits.

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
reproduce
```
