#!/bin/bash -l
#SBATCH --job-name=rqc_sefstats
#SBATCH --output=%x_%A_%a.out
#SBATCH --error=%x_%A_%a.err
# Default resource requests (overridden by submit_sef_analysis.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=8000
#SBATCH --time=30
# NOTE: the array range is set at submit time (see submit_sef_analysis.sh) to
#       match the number of SEF years, e.g. --array=0-124. A default is below.
#SBATCH --array=0-124

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_sef_analysis.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for
# direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${SEFSTATS_ROOT}"

cd "${REPO_ROOT}"

# Cap DuckDB's memory to this job's cgroup allocation (it otherwise sizes from
# the node's physical RAM) and let the daily aggregate spill to node-local
# scratch instead of failing.
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${SEFSTATS_MEM_MB:-8000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_sefstats_${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

echo "SEF analysis year-index ${SLURM_ARRAY_TASK_ID}: reading ${SEFSTATS_SEF_ROOT}/tsv"
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
run_py scripts/build_sef_analysis_parquet.py \
    --sef-root "${SEFSTATS_SEF_ROOT}" \
    --out-root "${SEFSTATS_ROOT}" \
    --year-index "${SLURM_ARRAY_TASK_ID}"

echo "SEF analysis year-index ${SLURM_ARRAY_TASK_ID} complete."
