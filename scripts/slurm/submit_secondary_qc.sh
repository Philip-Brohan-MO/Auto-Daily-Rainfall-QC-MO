#!/bin/bash
# Submit the secondary-QC model pipeline (QC check 2, stage 2) as two dependent
# single-node SLURM stages:
#   1. secondary_qc_train  (fit models 1 & 2, calibrate k, persist artifacts)
#   2. secondary_qc_score  (apply the models to the QC1-fail rows; starts after
#      training succeeds)
#
# Usage:
#   scripts/slurm/submit_secondary_qc.sh
#   SECONDARY_MAX_TRAIN_ROWS=10000000 scripts/slurm/submit_secondary_qc.sh
#   SECONDARY_COVERAGE_TARGET=0.995 scripts/slurm/submit_secondary_qc.sh
#
# Prerequisites:
#   1. QC check 1 must have been run (daily_qc_status must exist) -- supplies the
#      pass rows (training) and fail rows (scoring).
#   2. The regional-stats table must have been built with
#      scripts/slurm/submit_regional_stats.sh (this script checks for it).
#
# After scoring finishes, inspect results in the notebook:
#   notebooks/qc_RR_secondary_ml.ipynb
#
# The scoring stage streams the fail rows, so it runs as a single job. If the
# fail set ever grows too large for one job it can be sharded by file_id with
# the same --start-file-id/--end-file-id args the score script accepts.
#
# Requires: sbatch on PATH, PDIR set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

mkdir -p "${SECONDARY_QC_ROOT}" "${SLURM_LOG_DIR}"

# The regional-stats table is a required input (built by submit_regional_stats.sh)
# -- it holds the neighbour-statistic features for every located station-day.
REGIONAL_DIR="${PDIR}/regional_stats_parquet/regional_daily_stats"
if ! ls "${REGIONAL_DIR}"/session_meta*_qc*.parquet >/dev/null 2>&1; then
    echo "ERROR: regional-stats table not found under:" >&2
    echo "  ${REGIONAL_DIR}" >&2
    echo "Build it first with:  scripts/slurm/submit_regional_stats.sh" >&2
    exit 1
fi

EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR}"
EXPORTS="${EXPORTS},SECONDARY_QC_ROOT=${SECONDARY_QC_ROOT}"
EXPORTS="${EXPORTS},SECONDARY_MAX_TRAIN_ROWS=${SECONDARY_MAX_TRAIN_ROWS}"
EXPORTS="${EXPORTS},SECONDARY_COVERAGE_TARGET=${SECONDARY_COVERAGE_TARGET}"
EXPORTS="${EXPORTS},SECONDARY_SEED=${SECONDARY_SEED}"
EXPORTS="${EXPORTS},PDIR=${PDIR}"

TRAIN_RES="--qos=${SLURM_QOS} --ntasks=${SECONDARY_TRAIN_CORES} --ntasks-per-core=1 --mem=${SECONDARY_TRAIN_MEM_MB} --time=${SECONDARY_TRAIN_TIME_MIN}"
SCORE_RES="--qos=${SLURM_QOS} --ntasks=${SECONDARY_SCORE_CORES} --ntasks-per-core=1 --mem=${SECONDARY_SCORE_MEM_MB} --time=${SECONDARY_SCORE_TIME_MIN}"

TRAIN_ID=$(sbatch --parsable \
    ${TRAIN_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/secondary_qc_train.sbatch")
echo "Submitted secondary-QC training: ${TRAIN_ID}"

SCORE_ID=$(sbatch --parsable \
    --dependency=afterok:${TRAIN_ID} \
    ${SCORE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/secondary_qc_score.sbatch")
echo "Submitted secondary-QC scoring: ${SCORE_ID}"

echo
echo "Pipeline submitted. Track with:  squeue -u \$USER"
echo "Models land in:  ${SECONDARY_QC_ROOT}/models"
echo "Results land in: ${SECONDARY_QC_ROOT}/secondary_qc_status"
