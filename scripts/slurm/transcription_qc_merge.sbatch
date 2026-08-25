#!/bin/bash -l
#SBATCH --job-name=rqc_tqc_merge
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_transcription_qc.sh via CLI).
#SBATCH --qos=normal
#SBATCH --ntasks=2
#SBATCH --ntasks-per-core=1
#SBATCH --mem=24000
#SBATCH --time=120

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_transcription_qc.sh exports RQC_SLURM_DIR. Fall back for direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

cd "${REPO_ROOT}"

# Cap DuckDB's memory to this job's cgroup allocation and let the duplicate
# self-join spill to node-local scratch instead of failing.
JOB_MEM_MB="${SLURM_MEM_PER_NODE:-${TQC_MERGE_MEM_MB:-24000}}"
DUCKDB_LIMIT_MB=$(( JOB_MEM_MB > 5000 ? JOB_MEM_MB - 3000 : 2000 ))
export DUCKDB_MEMORY_LIMIT="${DUCKDB_LIMIT_MB}MB"
export DUCKDB_TEMP_DIR="${TMPDIR:-/var/tmp}/duckdb_tqc_merge_${SLURM_JOB_ID:-$$}"
mkdir -p "${DUCKDB_TEMP_DIR}"
trap 'rm -rf "${DUCKDB_TEMP_DIR}"' EXIT

echo "Merging ${TQC_NUM_SHARDS} transcription-QC shards from ${TQC_SHARD_DIR} into ${TQC_ROOT}"
echo "  DuckDB memory_limit=${DUCKDB_MEMORY_LIMIT}, temp_directory=${DUCKDB_TEMP_DIR}"
run_py scripts/merge_transcription_qc_shards.py \
    --shard-dir "${TQC_SHARD_DIR}" \
    --qc-root "${TQC_ROOT}" \
    --ensemble-dataset-root "${ENSEMBLE_PARQUET_ROOT}" \
    --good-dataset-root "${TQC_GOOD_ROOT}" \
    --expected-shards "${TQC_NUM_SHARDS}" \
    --min-nonzero-days "${TQC_MIN_NONZERO_DAYS}" \
    --round-decimals "${TQC_ROUND_DECIMALS}" \
    --match-tol "${TQC_MATCH_TOL}" \
    --min-overlap-days "${TQC_MIN_OVERLAP_DAYS}" \
    --min-agreement "${TQC_MIN_AGREEMENT}" \
    --max-block "${TQC_MAX_BLOCK}"

echo "Transcription-QC merge complete."
