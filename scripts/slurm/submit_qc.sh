#!/bin/bash
# Submit the QC exact-monthly-consistency pipeline as two dependent SLURM stages:
#   1. qc_array   (QC_NUM_SHARDS array tasks)
#   2. qc_merge   (single job, starts after the whole array succeeds)
#
# Usage:
#   scripts/slurm/submit_qc.sh
#   QC_NUM_SHARDS=200 scripts/slurm/submit_qc.sh
#   QC_TOTAL_FILE_IDS=514000 scripts/slurm/submit_qc.sh
#
# After the merge finishes, inspect results in the notebook:
#   notebooks/qc_RR_monthly_total.ipynb
#
# Requires: sbatch on PATH, PDIR set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

mkdir -p "${QC_SHARD_DIR}" "${SLURM_LOG_DIR}"

# Avoid mixing stale shard files from an earlier failed run with new output.
rm -f "${QC_SHARD_DIR}"/qc_shard_*.sqlite
rm -f "${QC_SHARD_DIR}"/qc_shard_*.parquet

ARRAY_MAX=$(( QC_NUM_SHARDS - 1 ))

EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR}"
EXPORTS="${EXPORTS},QC_NUM_SHARDS=${QC_NUM_SHARDS}"
EXPORTS="${EXPORTS},QC_TOTAL_FILE_IDS=${QC_TOTAL_FILE_IDS}"
EXPORTS="${EXPORTS},QC_SHARD_DIR=${QC_SHARD_DIR}"
EXPORTS="${EXPORTS},QC_TOLERANCE=${QC_TOLERANCE}"
EXPORTS="${EXPORTS},PDIR=${PDIR}"

ARRAY_RES="--qos=${SLURM_QOS} --ntasks=${QC_CORES} --ntasks-per-core=1 --mem=${QC_MEM_MB} --time=${QC_TIME_MIN}"
MERGE_RES="--qos=${SLURM_QOS} --ntasks=${QC_MERGE_CORES} --ntasks-per-core=1 --mem=${QC_MERGE_MEM_MB} --time=${QC_MERGE_TIME_MIN}"

ARRAY_ID=$(sbatch --parsable \
    ${ARRAY_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/qc_array.sbatch")
echo "Submitted QC array: ${ARRAY_ID} (0-${ARRAY_MAX}, ${QC_NUM_SHARDS} shards)"

MERGE_ID=$(sbatch --parsable \
    --dependency=afterok:${ARRAY_ID} \
    ${MERGE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/qc_merge.sbatch")
echo "Submitted QC merge: ${MERGE_ID}"

echo
echo "Pipeline submitted. Track with:  squeue -u \$USER"
echo "Shards land in: ${QC_SHARD_DIR}"
echo "Results land in parquet QC dataset: ${PDIR}/qc_parquet (latest qc_sessions row)"
