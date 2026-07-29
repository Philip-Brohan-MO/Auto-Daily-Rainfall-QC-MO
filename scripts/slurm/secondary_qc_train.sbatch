#!/bin/bash -l
#SBATCH --job-name=rqc_sqc_train
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_secondary_qc.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=8
#SBATCH --ntasks-per-core=1
#SBATCH --mem=32000
#SBATCH --time=120

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_secondary_qc.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for
# direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${SECONDARY_QC_ROOT}"

cd "${REPO_ROOT}"

# Cap DuckDB's memory to this job's cgroup allocation (it otherwise sizes from
# the node's physical RAM) and spill the training-frame scan to node-local
# scratch. XGBoost itself works in RAM on the sampled frame.
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${SECONDARY_TRAIN_MEM_MB:-32000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_sqc_train_${SLURM_JOB_ID:-$$}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

echo "Secondary-QC training: max_rows=${SECONDARY_MAX_TRAIN_ROWS}, coverage=${SECONDARY_COVERAGE_TARGET}"
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
run_py scripts/train_secondary_qc_models.py \
    --secondary-qc-root "${SECONDARY_QC_ROOT}" \
    --max-rows "${SECONDARY_MAX_TRAIN_ROWS}" \
    --coverage-target "${SECONDARY_COVERAGE_TARGET}" \
    --seed "${SECONDARY_SEED}"

echo "Secondary-QC training complete."
