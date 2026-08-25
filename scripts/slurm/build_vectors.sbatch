#!/bin/bash -l
#SBATCH --job-name=rqc_build
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_all.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=2
#SBATCH --ntasks-per-core=1
#SBATCH --mem=8000
#SBATCH --time=20

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_all.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${SIMILARITY_SHARD_DIR}" "${SLURM_LOG_DIR}"

cd "${REPO_ROOT}"

# Cap DuckDB's memory to this job's cgroup allocation (it otherwise sizes from
# the node's physical RAM and can be OOM-killed) and let large aggregations
# spill to node-local scratch instead of failing.
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${BUILD_MEM_MB:-12000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_build_${SLURM_JOB_ID:-$$}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

echo "Building comparison vectors from ${MATCH_ENSEMBLE_PARQUET_ROOT} -> ${COMPARISON_PARQUET_ROOT}"
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
run_py scripts/build_similarity_vectors.py \
    --backend duckdb \
    --ensemble-dataset-root "${MATCH_ENSEMBLE_PARQUET_ROOT}" \
    --comparison-root "${COMPARISON_PARQUET_ROOT}"

echo "Build complete."
