#!/bin/bash
# Submit the full sharded matching pipeline as three dependent SLURM stages:
#   1. build_vectors   (single job)
#   2. match_array     (NUM_SHARDS array tasks, starts after build succeeds)
#   3. merge_shards     (single job, starts after the whole array succeeds)
#
# Usage:
#   scripts/slurm/submit_all.sh              # uses defaults (NUM_SHARDS=100)
#   NUM_SHARDS=200 scripts/slurm/submit_all.sh
#   scripts/slurm/submit_all.sh --skip-build # reuse an existing vectors DB
#
# Requires: sbatch on PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

SKIP_BUILD=0
for arg in "$@"; do
    case "${arg}" in
        --skip-build) SKIP_BUILD=1 ;;
        *) echo "Unknown option: ${arg}" >&2; exit 2 ;;
    esac
done

mkdir -p "${SIMILARITY_SHARD_DIR}" "${SLURM_LOG_DIR}"

# Pass shared config through to the jobs via --export so config values chosen
# here (e.g. NUM_SHARDS) are honoured inside each job. RQC_SLURM_DIR lets the
# jobs locate config.sh (sbatch copies the script away from this directory).
EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR},NUM_SHARDS=${NUM_SHARDS},TOP_K=${TOP_K},MIN_OVERLAP=${MIN_OVERLAP}"
EXPORTS="${EXPORTS},UNCERTAINTY_WEIGHT=${UNCERTAINTY_WEIGHT},BATCH_SIZE=${BATCH_SIZE}"
EXPORTS="${EXPORTS},PROGRESS_INTERVAL=${PROGRESS_INTERVAL},PDIR=${PDIR}"

ARRAY_MAX=$(( NUM_SHARDS - 1 ))

# Resource requests per stage (qos / cores via --ntasks / RAM in MB / minutes).
BUILD_RES="--qos=${SLURM_QOS} --ntasks=${BUILD_CORES} --ntasks-per-core=1 --mem=${BUILD_MEM_MB} --time=${BUILD_TIME_MIN}"
MATCH_RES="--qos=${SLURM_QOS} --ntasks=${MATCH_CORES} --ntasks-per-core=1 --mem=${MATCH_MEM_MB} --time=${MATCH_TIME_MIN}"
MERGE_RES="--qos=${SLURM_QOS} --ntasks=${MERGE_CORES} --ntasks-per-core=1 --mem=${MERGE_MEM_MB} --time=${MERGE_TIME_MIN}"

BUILD_DEP=""
if [[ "${SKIP_BUILD}" -eq 0 ]]; then
    BUILD_ID=$(sbatch --parsable \
        ${BUILD_RES} \
        --chdir="${SLURM_LOG_DIR}" \
        --export="${EXPORTS}" \
        "${SCRIPT_DIR}/build_vectors.sbatch")
    echo "Submitted build job: ${BUILD_ID}"
    BUILD_DEP="--dependency=afterok:${BUILD_ID}"
else
    echo "Skipping build; reusing ${COMPARISON_DB}"
fi

ARRAY_ID=$(sbatch --parsable \
    ${BUILD_DEP} \
    ${MATCH_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/match_array.sbatch")
echo "Submitted match array: ${ARRAY_ID} (0-${ARRAY_MAX})"

MERGE_ID=$(sbatch --parsable \
    --dependency=afterok:${ARRAY_ID} \
    ${MERGE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/merge_shards.sbatch")
echo "Submitted merge job: ${MERGE_ID}"

echo
echo "Pipeline submitted. Track with:  squeue -u \$USER"
echo "Final results land in: ${COMPARISON_PARQUET_ROOT} (latest similarity_sessions row)"
