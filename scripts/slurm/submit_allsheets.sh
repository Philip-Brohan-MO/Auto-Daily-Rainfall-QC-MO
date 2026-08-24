#!/bin/bash
# Submit the full ALLSHEETS residual-matching pipeline as four dependent stages:
#   1. build_allsheets_vectors    (single job)
#   2. match_allsheets_array      (ALLSHEETS_NUM_SHARDS array tasks, after build)
#   3. merge_allsheets_shards     (single job, after the whole array succeeds)
#   4. finalize_allsheets_metadata (single job, assign + combine into the final
#                                   ensemble_metadata session)
#
# PREREQUISITES (run these first):
#   - The DATA pipeline (scripts/slurm/submit_all.sh) has produced a similarity
#     session in COMPARISON_PARQUET_ROOT.
#   - The DATA metadata assignment has written an ensemble_metadata/session_*.parquet
#     under COMPARISON_PARQUET_ROOT (assign_ensemble_metadata_parquet; e.g. the
#     assignment cell in notebooks/match_metadata.ipynb). This supplies both the
#     residual filter (records without an exact DATA match) and the combine base.
#   - The ALLSHEETS Parquet store exists at ALLSHEETS_PARQUET_ROOT (built by the
#     ALLSHEETS ingest section of notebooks/RR_data_ingest.ipynb).
#
# Usage:
#   scripts/slurm/submit_allsheets.sh                 # defaults (ALLSHEETS_NUM_SHARDS=NUM_SHARDS)
#   ALLSHEETS_NUM_SHARDS=200 scripts/slurm/submit_allsheets.sh
#   scripts/slurm/submit_allsheets.sh --skip-build    # reuse existing ALLSHEETS vectors
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

mkdir -p "${ALLSHEETS_SIMILARITY_SHARD_DIR}" "${SLURM_LOG_DIR}"

# Pass shared config through to the jobs via --export so config values chosen
# here (e.g. ALLSHEETS_NUM_SHARDS) are honoured inside each job. RQC_SLURM_DIR
# lets the jobs locate config.sh (sbatch copies the script away from this dir).
EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR},NUM_SHARDS=${NUM_SHARDS}"
EXPORTS="${EXPORTS},ALLSHEETS_NUM_SHARDS=${ALLSHEETS_NUM_SHARDS}"
EXPORTS="${EXPORTS},TOP_K=${TOP_K},MIN_OVERLAP=${MIN_OVERLAP}"
EXPORTS="${EXPORTS},UNCERTAINTY_WEIGHT=${UNCERTAINTY_WEIGHT},BATCH_SIZE=${BATCH_SIZE}"
EXPORTS="${EXPORTS},PROGRESS_INTERVAL=${PROGRESS_INTERVAL},PDIR=${PDIR}"
EXPORTS="${EXPORTS},ALLSHEETS_PARQUET_ROOT=${ALLSHEETS_PARQUET_ROOT}"
EXPORTS="${EXPORTS},ALLSHEETS_COMPARISON_PARQUET_ROOT=${ALLSHEETS_COMPARISON_PARQUET_ROOT}"
EXPORTS="${EXPORTS},ALLSHEETS_SIMILARITY_SHARD_DIR=${ALLSHEETS_SIMILARITY_SHARD_DIR}"
EXPORTS="${EXPORTS},ALLSHEETS_DATA_METADATA=${ALLSHEETS_DATA_METADATA}"
EXPORTS="${EXPORTS},ALLSHEETS_MATCH_TYPE=${ALLSHEETS_MATCH_TYPE}"

ARRAY_MAX=$(( ALLSHEETS_NUM_SHARDS - 1 ))

# Resource requests per stage (qos / cores via --ntasks / RAM in MB / minutes).
BUILD_RES="--qos=${SLURM_QOS} --ntasks=${ALLSHEETS_BUILD_CORES} --ntasks-per-core=1 --mem=${ALLSHEETS_BUILD_MEM_MB} --time=${ALLSHEETS_BUILD_TIME_MIN}"
MATCH_RES="--qos=${SLURM_QOS} --ntasks=${ALLSHEETS_MATCH_CORES} --ntasks-per-core=1 --mem=${ALLSHEETS_MATCH_MEM_MB} --time=${ALLSHEETS_MATCH_TIME_MIN}"
MERGE_RES="--qos=${SLURM_QOS} --ntasks=${ALLSHEETS_MERGE_CORES} --ntasks-per-core=1 --mem=${ALLSHEETS_MERGE_MEM_MB} --time=${ALLSHEETS_MERGE_TIME_MIN}"
FINAL_RES="--qos=${SLURM_QOS} --ntasks=${ALLSHEETS_FINAL_CORES} --ntasks-per-core=1 --mem=${ALLSHEETS_FINAL_MEM_MB} --time=${ALLSHEETS_FINAL_TIME_MIN}"

BUILD_DEP=""
if [[ "${SKIP_BUILD}" -eq 0 ]]; then
    BUILD_ID=$(sbatch --parsable \
        ${BUILD_RES} \
        --chdir="${SLURM_LOG_DIR}" \
        --export="${EXPORTS}" \
        "${SCRIPT_DIR}/build_allsheets_vectors.sbatch")
    echo "Submitted ALLSHEETS build job: ${BUILD_ID}"
    BUILD_DEP="--dependency=afterok:${BUILD_ID}"
else
    echo "Skipping build; reusing ${ALLSHEETS_COMPARISON_PARQUET_ROOT}"
fi

ARRAY_ID=$(sbatch --parsable \
    ${BUILD_DEP} \
    ${MATCH_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/match_allsheets_array.sbatch")
echo "Submitted ALLSHEETS match array: ${ARRAY_ID} (0-${ARRAY_MAX})"

MERGE_ID=$(sbatch --parsable \
    --dependency=afterok:${ARRAY_ID} \
    ${MERGE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/merge_allsheets_shards.sbatch")
echo "Submitted ALLSHEETS merge job: ${MERGE_ID}"

FINAL_ID=$(sbatch --parsable \
    --dependency=afterok:${MERGE_ID} \
    ${FINAL_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/finalize_allsheets_metadata.sbatch")
echo "Submitted ALLSHEETS finalise job: ${FINAL_ID}"

echo
echo "Pipeline submitted. Track with:  squeue -u \$USER"
echo "Final combined metadata lands in: ${COMPARISON_PARQUET_ROOT} (new ensemble_metadata/session_*.parquet)"
