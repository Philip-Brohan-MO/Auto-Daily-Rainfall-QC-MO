# How to reproduce and extend

This project is designed to be reproduced and extended. Everything — code,
notebooks, environment specification, and this documentation — lives in a single
Git repository:
[github.com/Philip-Brohan/Auto-Daily-Rainfall-QC](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC).

If you are familiar with GitHub, fork or clone the repository. If you'd rather
not, you can download the whole thing as a
[zip file](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/archive/refs/heads/main.zip).

## Software environment

Everything runs in the `ADRQ` Conda environment specified in
`environments/ADRQ.yml`. See [Installation](installation.md) for the one-time
setup, then activate it before doing anything else:

```bash
conda activate ADRQ
```

## Compute

The local machine is used only for orchestration and the small in-notebook
demonstrations. The compute-intensive work — matching every transcription
against every Rainfall Rescue station-year, quality control over hundreds of
millions of station-days, the SEF export, and the animation renders — runs on the
[SPICE HPC cluster](reference/slurm.md) via SLURM.

The processing code itself is not cluster-specific and will run in any suitable
Python environment; the submission scripts and job specs under `scripts/slurm/`
are written for a SLURM scheduler and configured through
`scripts/slurm/config.sh`.

## Running the workflow

The workflow is driven by the notebooks under `notebooks/`, run in order. Start
with the [workflow overview](workflow/overview.md), which lays out the stages and
links to each notebook.

## The documentation

These web pages are built with [Sphinx](https://www.sphinx-doc.org/) from the
Markdown sources in the `docs/` directory.

This repository currently documents the local Sphinx build flow. If a GitHub
Pages CI workflow is added in future, document it explicitly in this section.

To build the pages locally:

```bash
pip install sphinx myst-parser
sphinx-build -b html docs docs/_build/html
```

or, from inside the `docs/` directory with the `ADRQ` environment active:

```bash
make html
```


This document is distributed under the terms of the
[Open Government Licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/).
Source code is distributed under the terms of the
[BSD licence](https://opensource.org/licenses/BSD-2-Clause).
