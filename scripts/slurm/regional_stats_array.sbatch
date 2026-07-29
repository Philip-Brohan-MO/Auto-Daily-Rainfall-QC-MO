#!/bin/bash -l
#SBATCH --job-name=rqc_regional
#SBATCH --output=%x_%A_%a.out
#SBATCH --error=%x_%A_%a.err
# Default resource requests (overridden by submit_regional_stats.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=12000
#SBATCH --time=90
# NOTE: the array range is set at submit time (see submit_regional_stats.sh) to
#       match REGIONAL_NUM_SHARDS, e.g. --array=0-199. A default is provided below.
#SBATCH --array=0-199

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_regional_stats.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for
# direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${REGIONAL_SHARD_DIR}"

cd "${REPO_ROOT}"

# Cap DuckDB's memory to this job's cgroup allocation (it otherwise sizes from
# the node's physical RAM and can be OOM-killed) and let large aggregations
# (the daily median GROUP BY and the spatial self-join) spill to node-local
# scratch instead of failing.
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${REGIONAL_MEM_MB:-12000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_regional_${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

echo "Regional-stats shard ${SLURM_ARRAY_TASK_ID}/${REGIONAL_NUM_SHARDS}: targets partitioned from ${REGIONAL_TOTAL_FILE_IDS} file_ids"
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
run_py scripts/run_regional_stats_shard.py \
    --shard-dir "${REGIONAL_SHARD_DIR}" \
    --consensus-root "${CONSENSUS_ROOT}" \
    --num-shards "${REGIONAL_NUM_SHARDS}" \
    --shard-index "${SLURM_ARRAY_TASK_ID}" \
    --total-file-ids "${REGIONAL_TOTAL_FILE_IDS}"

echo "Regional-stats shard ${SLURM_ARRAY_TASK_ID} complete."
