#!/bin/bash -l
#SBATCH --job-name=rqc_sqc_score
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_secondary_qc.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=4
#SBATCH --ntasks-per-core=1
#SBATCH --mem=16000
#SBATCH --time=60

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_secondary_qc.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for
# direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${SECONDARY_QC_ROOT}"

cd "${REPO_ROOT}"

# Cap DuckDB's memory to this job's cgroup allocation and spill the pass/fail-row
# scan to node-local scratch. Rows are streamed and scored in batches, so
# memory stays flat regardless of the fail-set size.
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${SECONDARY_SCORE_MEM_MB:-16000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_sqc_score_${SLURM_JOB_ID:-$$}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

echo "Secondary-QC scoring QC1 pass/fail station-days."
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
SECONDARY_QC_SESSION_ARG=()
if [[ -n "${QC_SESSION_ID}" ]]; then
    SECONDARY_QC_SESSION_ARG=(--qc-session-id "${QC_SESSION_ID}")
fi

cmd=(
    scripts/score_secondary_qc.py
    --secondary-qc-root "${SECONDARY_QC_ROOT}"
)
if [[ ${#SECONDARY_QC_SESSION_ARG[@]} -gt 0 ]]; then
    cmd+=("${SECONDARY_QC_SESSION_ARG[@]}")
fi
run_py "${cmd[@]}"

echo "Secondary-QC scoring complete."
