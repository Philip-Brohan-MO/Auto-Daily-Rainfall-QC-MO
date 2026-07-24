#!/bin/bash -l
#SBATCH --job-name=rqc_merge
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_all.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=4000
#SBATCH --time=20

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_all.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

cd "${REPO_ROOT}"

# Cap DuckDB's memory to this job's cgroup allocation (it otherwise sizes from
# the node's physical RAM and can be OOM-killed) and let the ORDER BY sort of
# the merged rows spill to node-local scratch instead of failing.
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${MERGE_MEM_MB:-8000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_merge_${SLURM_JOB_ID:-$$}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

echo "Merging ${NUM_SHARDS} shards from ${SIMILARITY_SHARD_DIR} into ${COMPARISON_PARQUET_ROOT}"
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
run_py scripts/merge_similarity_shards.py \
    --backend duckdb \
    --comparison-root "${COMPARISON_PARQUET_ROOT}" \
    --shard-dir "${SIMILARITY_SHARD_DIR}" \
    --top-k "${TOP_K}" \
    --min-overlap "${MIN_OVERLAP}" \
    --uncertainty-weight "${UNCERTAINTY_WEIGHT}" \
    --expected-shards "${NUM_SHARDS}"

echo "Merge complete."
