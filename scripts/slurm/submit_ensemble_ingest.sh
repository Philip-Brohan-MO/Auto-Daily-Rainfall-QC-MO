#!/bin/bash
# Submit the sharded ensemble-transcription ingest as two dependent SLURM stages:
#   1. ingest_ensemble_array  (ENSEMBLE_NUM_SHARDS array tasks)
#   2. merge_ensemble_shards  (single job, starts after the whole array succeeds)
#
# Usage:
#   scripts/slurm/submit_ensemble_ingest.sh                     # all files, 100 shards
#   ENSEMBLE_NUM_SHARDS=200 scripts/slurm/submit_ensemble_ingest.sh
#   ENSEMBLE_MAX_FILES=500 scripts/slurm/submit_ensemble_ingest.sh   # quick test slice
#
# Requires: sbatch on PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

mkdir -p "${ENSEMBLE_SHARD_DIR}" "${SLURM_LOG_DIR}"

# Remove any shard files from a previous run so the merge sees only this run's
# output (and its --expected-shards count is meaningful).
rm -f "${ENSEMBLE_SHARD_DIR}"/ens_shard_*.sqlite

# Pass shared config through to the jobs via --export so values chosen here
# (e.g. ENSEMBLE_NUM_SHARDS) are honoured inside each job. RQC_SLURM_DIR lets the
# jobs locate config.sh (sbatch copies the script away from this directory).
EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR},ENSEMBLE_NUM_SHARDS=${ENSEMBLE_NUM_SHARDS}"
EXPORTS="${EXPORTS},ENSEMBLE_MAX_FILES=${ENSEMBLE_MAX_FILES},PDIR=${PDIR}"

ARRAY_MAX=$(( ENSEMBLE_NUM_SHARDS - 1 ))

# Resource requests per stage (qos / cores via --ntasks / RAM in MB / minutes).
INGEST_RES="--qos=${SLURM_QOS} --ntasks=${EINGEST_CORES} --ntasks-per-core=1 --mem=${EINGEST_MEM_MB} --time=${EINGEST_TIME_MIN}"
MERGE_RES="--qos=${SLURM_QOS} --ntasks=${EMERGE_CORES} --ntasks-per-core=1 --mem=${EMERGE_MEM_MB} --time=${EMERGE_TIME_MIN}"

INGEST_ID=$(sbatch --parsable \
    ${INGEST_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/ingest_ensemble_array.sbatch")
echo "Submitted ingest array: ${INGEST_ID} (0-${ARRAY_MAX})"

MERGE_ID=$(sbatch --parsable \
    --dependency=afterok:${INGEST_ID} \
    ${MERGE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/merge_ensemble_shards.sbatch")
echo "Submitted merge job: ${MERGE_ID}"

echo
echo "Pipeline submitted. Track with:  squeue -u \$USER"
echo "Final database lands in: ${ENSEMBLE_DB}"
