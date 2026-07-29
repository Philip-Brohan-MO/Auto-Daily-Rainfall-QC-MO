#!/bin/bash -l
#SBATCH --job-name=rqc_regional_merge
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_regional_stats.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=24000
#SBATCH --time=120

set -euo pipefail

SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

cd "${REPO_ROOT}"

# Cap DuckDB memory / spill for the merge (a streaming concat of the shard
# Parquet files -- low memory, but keep the cap + temp dir for safety).
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${REGIONAL_MERGE_MEM_MB:-24000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_regional_merge_${SLURM_JOB_ID:-$$}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

echo "Merging ${REGIONAL_NUM_SHARDS} regional-stats shards from ${REGIONAL_SHARD_DIR}"
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
run_py scripts/merge_regional_stats_shards.py \
    --shard-dir "${REGIONAL_SHARD_DIR}" \
    --expected-shards "${REGIONAL_NUM_SHARDS}"

echo "Regional-stats merge complete."
