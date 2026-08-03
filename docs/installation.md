# Installation

Everything in this project — every script and every notebook — is intended to run
inside the `ADRQ` Conda environment. Setting that up is the only prerequisite for
reading and running the workflow notebooks.

## Prerequisites

- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or Anaconda
- Git
- A local machine is enough for orchestration and the small in-notebook
  demonstrations. The heavy work — matching, quality control, export, and
  rendering over the full dataset — runs on the SPICE HPC cluster via
  [SLURM](reference/slurm.md); see that page for the compute setup.

## 1 — Clone the repository

```bash
git clone https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO.git
cd Auto-Daily-Rainfall-QC-MO
```

## 2 — Create the Conda environment

The `environments/ADRQ.yml` file pins all dependencies, including DuckDB,
PyArrow, XGBoost, scikit-learn, Cartopy, and the Milvus vector database:

```bash
conda env create -f environments/ADRQ.yml
conda activate ADRQ
```

```{important}
Activate `ADRQ` before running any script or notebook. This is non-negotiable
for reproducibility — the notebooks assume this environment and its `ipykernel`
kernel.
```

## 3 — Configure the project paths

The environment file sets two variables that the code relies on:

- `PYTHONPATH` — the repository root, so the project modules import cleanly.
- `PDIR` — the data directory that holds the Parquet datasets and all SLURM shard
  outputs (defaults to `/data/scratch/philip.brohan/ADRQ`).

Edit these in `environments/ADRQ.yml` (or export them in your shell) to match
your own installation. The SLURM jobs read the same values from
`scripts/slurm/config.sh`.

## 4 — Select the kernel in Jupyter / VS Code

Open any notebook under `notebooks/` and select the **ADRQ** kernel. Each
notebook's header cell repeats this reminder.

## Updating

```bash
git pull
conda env update -f environments/ADRQ.yml --prune
```
