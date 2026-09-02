#!/bin/bash -l
#SBATCH --job-name=rqc_sef
#SBATCH --output=%x_%A_%a.out
#SBATCH --error=%x_%A_%a.err
# Default resource requests (overridden by submit_sef_export.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=16000
#SBATCH --time=60
# NOTE: the array range is set at submit time (see submit_sef_export.sh) to
#       match SEF_NUM_SHARDS, e.g. --array=0-199. A default is provided below.
#SBATCH --array=0-199

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_sef_export.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for
# direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${SEF_OUTPUT_ROOT}"

cd "${REPO_ROOT}"

# Cap DuckDB's memory to this job's cgroup allocation (it otherwise sizes from
# the node's physical RAM and can be OOM-killed) and let the ordered join spill
# to node-local scratch instead of failing.
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${SEF_MEM_MB:-16000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_sef_${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

echo "SEF export shard ${SLURM_ARRAY_TASK_ID}/${SEF_NUM_SHARDS}: years partitioned from ${SEF_MIN_YEAR}-${SEF_MAX_YEAR}"
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
run_py scripts/export_sef.py \
    --output-root "${SEF_OUTPUT_ROOT}" \
    --source "${SEF_SOURCE}" \
    --link "${SEF_LINK}" \
    --obs-hour "${SEF_OBS_HOUR}" \
    --batch-rows "${SEF_BATCH_ROWS:-50000}" \
    --num-shards "${SEF_NUM_SHARDS}" \
    --shard-index "${SLURM_ARRAY_TASK_ID}" \
    --min-year "${SEF_MIN_YEAR}" \
    --max-year "${SEF_MAX_YEAR}"

echo "SEF export shard ${SLURM_ARRAY_TASK_ID} complete."
