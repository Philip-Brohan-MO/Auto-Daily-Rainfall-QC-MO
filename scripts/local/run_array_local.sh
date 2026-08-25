#!/bin/bash
# Generic local replacement for `sbatch --array=0-N script.sbatch`.
#
# Each SLURM *_array.sbatch file is a plain bash script (the #SBATCH lines are
# comments), so it can be run directly once REPO_ROOT/CONDA_ENV_PREFIX point at
# this machine (see config_local.sh). This driver fans SLURM_ARRAY_TASK_ID
# 0..num_shards-1 out across a bounded worker pool instead of a real array job,
# and fails if any shard fails -- the local stand-in for SLURM's afterok gate.
#
# Usage: scripts/local/run_array_local.sh <array_sbatch_file> <num_shards> [workers]

set -o pipefail  # not -e/-u: config.sh isn't -u-safe, and shard failures are handled explicitly below

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../slurm/config.sh"
source "${SCRIPT_DIR}/config_local.sh"

ARRAY_SBATCH_ARG="${1:?usage: run_array_local.sh <array_sbatch_file> <num_shards> [workers]}"
NUM_SHARDS="${2:?usage: run_array_local.sh <array_sbatch_file> <num_shards> [workers]}"
WORKERS="${3:-${LOCAL_WORKERS}}"

ARRAY_SBATCH="${ARRAY_SBATCH_ARG}"
if [[ ! -f "${ARRAY_SBATCH}" ]]; then
    # Allow a bare filename relative to scripts/slurm/.
    ARRAY_SBATCH="${SCRIPT_DIR}/../slurm/${ARRAY_SBATCH_ARG}"
fi
if [[ ! -f "${ARRAY_SBATCH}" ]]; then
    echo "ERROR: array sbatch file not found: ${ARRAY_SBATCH_ARG}" >&2
    exit 1
fi

JOB_NAME="$(basename "${ARRAY_SBATCH}" .sbatch)"
mkdir -p "${LOCAL_LOG_DIR}"
rm -f "${LOCAL_LOG_DIR}/${JOB_NAME}_FAILED_"*

# Split the machine's cores/memory evenly across concurrent shard workers, so
# the DuckDB-memory-cap / BLAS-thread-pinning logic already inside every
# *_array.sbatch (keyed on SLURM_MEM_PER_NODE / SLURM_CPUS_PER_TASK) sizes
# itself correctly without any change to that file.
export SLURM_MEM_PER_NODE=$(( LOCAL_TOTAL_MEM_MB / WORKERS ))
PER_WORKER_CORES=$(( LOCAL_TOTAL_CORES / WORKERS ))
export SLURM_CPUS_PER_TASK=$(( PER_WORKER_CORES > 0 ? PER_WORKER_CORES : 1 ))

echo "Running ${JOB_NAME}: ${NUM_SHARDS} shards, ${WORKERS} concurrent workers"
echo "  per-worker memory=${SLURM_MEM_PER_NODE}MB, cpus=${SLURM_CPUS_PER_TASK}"
echo "  logs -> ${LOCAL_LOG_DIR}/${JOB_NAME}_<index>.log"

export ARRAY_SBATCH LOCAL_LOG_DIR JOB_NAME

seq 0 $(( NUM_SHARDS - 1 )) | xargs -P "${WORKERS}" -I{} bash -c '
    index="$1"
    log_file="${LOCAL_LOG_DIR}/${JOB_NAME}_$(printf "%05d" "${index}").log"
    SLURM_ARRAY_TASK_ID="${index}" bash "${ARRAY_SBATCH}" >"${log_file}" 2>&1
    rc=$?
    if [[ ${rc} -ne 0 ]]; then
        touch "${LOCAL_LOG_DIR}/${JOB_NAME}_FAILED_${index}"
        echo "FAILED shard ${index} (exit ${rc}) -- see ${log_file}" >&2
    fi
' _ {}

if compgen -G "${LOCAL_LOG_DIR}/${JOB_NAME}_FAILED_*" > /dev/null; then
    echo "ERROR: one or more shards of ${JOB_NAME} failed; see ${LOCAL_LOG_DIR}" >&2
    exit 1
fi

echo "${JOB_NAME}: all ${NUM_SHARDS} shards completed successfully"
