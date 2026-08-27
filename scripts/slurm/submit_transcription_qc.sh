#!/bin/bash
# Submit the transcription-source QC pipeline as two dependent SLURM stages:
#   1. tqc_array   (TQC_NUM_SHARDS array tasks: per-file missing stats + vectors)
#   2. tqc_merge   (single job: flag bad sources + detect duplicates, starts
#                   after the whole array succeeds)
#
# Usage:
#   scripts/slurm/submit_transcription_qc.sh
#   TQC_NUM_SHARDS=200 scripts/slurm/submit_transcription_qc.sh
#   TQC_TOTAL_FILE_IDS=700000 scripts/slurm/submit_transcription_qc.sh
#   TQC_MIN_NONZERO_DAYS=70 scripts/slurm/submit_transcription_qc.sh
#
# After the merge finishes, inspect results in the notebook:
#   notebooks/Daily_transcriptions_ingest.ipynb (Quality control section)
#
# Requires: sbatch on PATH, PDIR set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

mkdir -p "${TQC_SHARD_DIR}" "${SLURM_LOG_DIR}"

# Avoid mixing stale shard files from an earlier failed run with new output.
rm -f "${TQC_SHARD_DIR}"/tqc_shard_*.parquet

ARRAY_MAX=$(( TQC_NUM_SHARDS - 1 ))

EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR}"
EXPORTS="${EXPORTS},PDIR=${PDIR}"
EXPORTS="${EXPORTS},TQC_ROOT=${TQC_ROOT}"
EXPORTS="${EXPORTS},TQC_SHARD_DIR=${TQC_SHARD_DIR}"
EXPORTS="${EXPORTS},TQC_GOOD_ROOT=${TQC_GOOD_ROOT}"
EXPORTS="${EXPORTS},TQC_NUM_SHARDS=${TQC_NUM_SHARDS}"
EXPORTS="${EXPORTS},TQC_TOTAL_FILE_IDS=${TQC_TOTAL_FILE_IDS}"
EXPORTS="${EXPORTS},TQC_MIN_NONZERO_DAYS=${TQC_MIN_NONZERO_DAYS}"
EXPORTS="${EXPORTS},TQC_ROUND_DECIMALS=${TQC_ROUND_DECIMALS}"
EXPORTS="${EXPORTS},TQC_MATCH_TOL=${TQC_MATCH_TOL}"
EXPORTS="${EXPORTS},TQC_MIN_OVERLAP_DAYS=${TQC_MIN_OVERLAP_DAYS}"
EXPORTS="${EXPORTS},TQC_MIN_AGREEMENT=${TQC_MIN_AGREEMENT}"
EXPORTS="${EXPORTS},TQC_MAX_BLOCK=${TQC_MAX_BLOCK}"

ARRAY_RES="--qos=${SLURM_QOS} --ntasks=${TQC_CORES} --ntasks-per-core=1 --mem=${TQC_MEM_MB} --time=${TQC_TIME_MIN}"
MERGE_RES="--qos=${SLURM_QOS} --ntasks=${TQC_MERGE_CORES} --ntasks-per-core=1 --mem=${TQC_MERGE_MEM_MB} --time=${TQC_MERGE_TIME_MIN}"

ARRAY_ID=$(sbatch --parsable \
    ${ARRAY_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/transcription_qc_array.sbatch")
echo "Submitted transcription-QC array: ${ARRAY_ID} (0-${ARRAY_MAX}, ${TQC_NUM_SHARDS} shards)"

MERGE_ID=$(sbatch --parsable \
    --dependency=afterok:${ARRAY_ID} \
    ${MERGE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/transcription_qc_merge.sbatch")
echo "Submitted transcription-QC merge: ${MERGE_ID}"

echo
echo "Pipeline submitted. Track with:  squeue -u \$USER"
echo "Shards land in:  ${TQC_SHARD_DIR}"
echo "Results land in: ${TQC_ROOT} (latest qc_sessions row)"
