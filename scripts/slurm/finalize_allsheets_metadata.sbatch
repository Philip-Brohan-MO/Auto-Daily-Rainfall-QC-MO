#!/bin/bash -l
#SBATCH --job-name=rqc_as_final
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_allsheets.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=2
#SBATCH --ntasks-per-core=1
#SBATCH --mem=16000
#SBATCH --time=60

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_allsheets.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

cd "${REPO_ROOT}"

# Cap DuckDB's memory to this job's cgroup allocation and spill to node-local
# scratch (the assign + combine steps stream every ensemble file through DuckDB).
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${ALLSHEETS_FINAL_MEM_MB:-16000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_as_final_${SLURM_JOB_ID:-$$}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

# Optional explicit DATA metadata parquet; empty -> the script auto-resolves the
# latest ensemble_metadata/session_*.parquet under the DATA comparison root.
META_ARG=()
if [[ -n "${ALLSHEETS_DATA_METADATA}" ]]; then
    META_ARG=(--data-metadata-path "${ALLSHEETS_DATA_METADATA}")
fi

# Optional explicit LeftOverSites.csv; empty -> the combine step auto-resolves
# LeftOverSites.csv under the Rainfall Rescue root.
CSV_ARG=()
if [[ -n "${ALLSHEETS_LEFTOVER_CSV}" ]]; then
    CSV_ARG=(--leftover-sites-csv "${ALLSHEETS_LEFTOVER_CSV}")
fi

echo "Finalising ALLSHEETS metadata -> new combined session under ${COMPARISON_PARQUET_ROOT}"
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
run_py scripts/finalize_allsheets_metadata.py \
    --allsheets-comparison-root "${ALLSHEETS_COMPARISON_PARQUET_ROOT}" \
    --source-comparison-root "${COMPARISON_PARQUET_ROOT}" \
    --ensemble-dataset-root "${ENSEMBLE_PARQUET_ROOT}" \
    --allsheets-dataset-root "${ALLSHEETS_PARQUET_ROOT}" \
    --allsheets-match-type "${ALLSHEETS_MATCH_TYPE}" \
    "${META_ARG[@]}" \
    "${CSV_ARG[@]}"

echo "ALLSHEETS finalise complete."
