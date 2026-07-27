#!/bin/bash
# Submit the SEF export (share located, QC'd daily rainfall in Station Exchange
# Format) as a single SLURM array stage:
#   sef_export_array   (SEF_NUM_SHARDS array tasks, each a disjoint file_id slice)
#
# There is NO merge stage: each shard writes its own station-year .tsv files into
# year-partitioned subfolders of SEF_OUTPUT_ROOT/tsv, so the shard outputs never
# collide.
#
# Usage:
#   scripts/slurm/submit_sef_export.sh
#   SEF_NUM_SHARDS=200 scripts/slurm/submit_sef_export.sh
#   SEF_TOTAL_FILE_IDS=680000 scripts/slurm/submit_sef_export.sh
#
# Prerequisites (both are inputs to the export join):
#   1. The daily-consensus table (submit_daily_consensus.sh) -- the daily totals.
#   2. Located ensemble metadata (assign_ensemble_metadata) -- station lat/lon.
#   QC verdicts (daily_qc_status, secondary_qc_status) are joined when present;
#   the secondary (qc2) verdict is reported as NA where a day was not re-examined.
#
# Requires: sbatch on PATH, PDIR set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

mkdir -p "${SEF_OUTPUT_ROOT}" "${SLURM_LOG_DIR}"

# The daily-consensus table is a required input (the daily rainfall totals).
CONSENSUS_FILE="${CONSENSUS_ROOT}/daily_consensus/daily_consensus.parquet"
if [[ ! -f "${CONSENSUS_FILE}" ]]; then
    echo "ERROR: daily-consensus table not found at:" >&2
    echo "  ${CONSENSUS_FILE}" >&2
    echo "Build it first with:  scripts/slurm/submit_daily_consensus.sh" >&2
    exit 1
fi

# Located ensemble metadata is the other required input (station coordinates).
if ! ls "${COMPARISON_PARQUET_ROOT}/ensemble_metadata/"*.parquet >/dev/null 2>&1; then
    echo "ERROR: ensemble metadata not found under:" >&2
    echo "  ${COMPARISON_PARQUET_ROOT}/ensemble_metadata/" >&2
    echo "Assign it first (assign_ensemble_metadata)." >&2
    exit 1
fi

ARRAY_MAX=$(( SEF_NUM_SHARDS - 1 ))

EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR}"
EXPORTS="${EXPORTS},SEF_NUM_SHARDS=${SEF_NUM_SHARDS}"
EXPORTS="${EXPORTS},SEF_TOTAL_FILE_IDS=${SEF_TOTAL_FILE_IDS}"
EXPORTS="${EXPORTS},SEF_OUTPUT_ROOT=${SEF_OUTPUT_ROOT}"
EXPORTS="${EXPORTS},SEF_SOURCE=${SEF_SOURCE}"
EXPORTS="${EXPORTS},SEF_LINK=${SEF_LINK}"
EXPORTS="${EXPORTS},SEF_OBS_HOUR=${SEF_OBS_HOUR}"
EXPORTS="${EXPORTS},PDIR=${PDIR}"

ARRAY_RES="--qos=${SLURM_QOS} --ntasks=${SEF_CORES} --ntasks-per-core=1 --mem=${SEF_MEM_MB} --time=${SEF_TIME_MIN}"

ARRAY_ID=$(sbatch --parsable \
    ${ARRAY_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/sef_export_array.sbatch")
echo "Submitted SEF export array: ${ARRAY_ID} (0-${ARRAY_MAX}, ${SEF_NUM_SHARDS} shards)"

echo
echo "Pipeline submitted. Track with:  squeue -u \$USER"
echo "SEF files land in: ${SEF_OUTPUT_ROOT}/tsv/<year>/<ID>.tsv"
echo "Per-shard manifests land in: ${SEF_OUTPUT_ROOT}/manifests"
