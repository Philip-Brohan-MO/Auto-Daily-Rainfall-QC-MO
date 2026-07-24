#!/bin/bash -l
#SBATCH --job-name=rqc_consensus
#SBATCH --output=%x_%A_%a.out
#SBATCH --error=%x_%A_%a.err
# Default resource requests (overridden by submit_daily_consensus.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=12000
#SBATCH --time=30
#SBATCH --array=0-99

set -euo pipefail

SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${CONSENSUS_SHARD_DIR}"

cd "${REPO_ROOT}"

# Cap DuckDB memory to this job's cgroup allocation and give it a spill dir.
# Each shard covers a CONTIGUOUS file_id range, so DuckDB prunes row groups and
# the holistic median only buffers the slice -- but keep the cap for safety.
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${CONSENSUS_MEM_MB:-12000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_consensus_${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

echo "Daily-consensus shard ${SLURM_ARRAY_TASK_ID}/${CONSENSUS_NUM_SHARDS} from ${CONSENSUS_TOTAL_FILE_IDS} file_ids"
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
run_py scripts/run_daily_consensus_shard.py \
    --shard-dir "${CONSENSUS_SHARD_DIR}" \
    --num-shards "${CONSENSUS_NUM_SHARDS}" \
    --shard-index "${SLURM_ARRAY_TASK_ID}" \
    --total-file-ids "${CONSENSUS_TOTAL_FILE_IDS}"

echo "Daily-consensus shard ${SLURM_ARRAY_TASK_ID} complete."
