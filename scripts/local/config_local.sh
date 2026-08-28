#!/bin/bash
# Local (non-SLURM) overrides for running the sharded pipelines on this machine.
# Source this AFTER scripts/slurm/config.sh -- it overrides the cluster-only
# paths and adds the local concurrency knobs used by run_array_local.sh.
#
# PDIR is deliberately NOT defaulted here: set it in your shell exactly as the
# notebooks require (see docs/installation.md).

# Repo root = this checkout, wherever it's cloned locally. Unconditional: this
# file runs right after scripts/slurm/config.sh specifically to override its
# cluster-only REPO_ROOT/CONDA_ENV_PREFIX, which would otherwise already be set.
export REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# Mac conda env is a plain named env (see environments/ADRQ_mac.yml), not a
# shared-disc prefix, but conda run -p also accepts an env's own directory.
export CONDA_ENV_PREFIX="${LOCAL_CONDA_ENV_PREFIX:-$HOME/miniconda3/envs/ADRQ}"

: "${PDIR:?PDIR must be set before sourcing config_local.sh (see docs/installation.md)}"

# Default to the same ensemble input tree defined in the notebook setup cell.
# This keeps the local-parallel branch aligned with the ADRQ environment while
# still allowing an explicit ENSEMBLE_ROOT override when needed.
export ENSEMBLE_ROOT="${ENSEMBLE_ROOT:-${ENSEMBLE_TRANSCRIPTIONS_ROOT:-${PDIR}/../documents/Daily_Rainfall_UK/operational_full/ensemble_transcriptions}}"

# Local capacity budget (M2 Studio: 24 cores / 64GB). Leave headroom for the OS.
export LOCAL_TOTAL_CORES="${LOCAL_TOTAL_CORES:-24}"
export LOCAL_TOTAL_MEM_MB="${LOCAL_TOTAL_MEM_MB:-55000}"
# Fixed, conservative worker count rather than one-worker-per-core. The
# transcription-QC shards are memory-heavy, and 4 workers keeps each shard above
# the ~8-10GB working set needed for the daily-consensus aggregation while still
# using the machine well enough to finish promptly.
export LOCAL_WORKERS="${LOCAL_WORKERS:-4}"
export LOCAL_LOG_DIR="${LOCAL_LOG_DIR:-${PDIR}/local_logs}"
