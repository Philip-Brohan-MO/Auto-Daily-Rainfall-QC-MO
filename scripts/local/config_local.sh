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

# Local capacity budget (M2 Studio: 24 cores / 64GB). Leave headroom for the OS.
export LOCAL_TOTAL_CORES="${LOCAL_TOTAL_CORES:-24}"
export LOCAL_TOTAL_MEM_MB="${LOCAL_TOTAL_MEM_MB:-55000}"
# Fixed, conservative worker count rather than one-worker-per-core, to leave
# each concurrent DuckDB shard enough memory headroom.
export LOCAL_WORKERS="${LOCAL_WORKERS:-8}"
export LOCAL_LOG_DIR="${LOCAL_LOG_DIR:-${PDIR}/local_logs}"
