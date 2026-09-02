#!/bin/bash
# Submit the SEF analysis-dataset build (Parquet summary tables from the SEF
# output only) as a single SLURM array stage:
#   sef_analysis_array   (one array task per SEF year)
#
# There is NO merge stage: each task parses one year of SEF .tsv files and writes
# disjoint observations/year=<Y>.parquet + daily_national/year=<Y>.parquet under
# SEFSTATS_ROOT, so the task outputs never collide. The array size is set here
# from the number of SEF years discovered under SEFSTATS_SEF_ROOT/tsv.
#
# Usage:
#   scripts/slurm/submit_sef_analysis.sh
#   SEFSTATS_SEF_ROOT=/some/sef_export scripts/slurm/submit_sef_analysis.sh
#
# Prerequisite: the SEF export itself (submit_sef_export.sh).
#
# Requires: sbatch on PATH, PDIR set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

mkdir -p "${SEFSTATS_ROOT}" "${SLURM_LOG_DIR}"

# The SEF export is the required input.
TSV_ROOT="${SEFSTATS_SEF_ROOT}/tsv"
if [[ ! -d "${TSV_ROOT}" ]]; then
    echo "ERROR: SEF export not found at:" >&2
    echo "  ${TSV_ROOT}" >&2
    echo "Build it first with:  scripts/slurm/submit_sef_export.sh" >&2
    exit 1
fi

# Count the SEF year subdirectories; one array task handles each year.
NUM_YEARS=$(find "${TSV_ROOT}" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' \
    | grep -E '^[0-9]+$' | wc -l)
if [[ "${NUM_YEARS}" -eq 0 ]]; then
    echo "ERROR: no year subdirectories found under ${TSV_ROOT}" >&2
    exit 1
fi
ARRAY_MAX=$(( NUM_YEARS - 1 ))

# Dense SEF years (e.g. 1908-era outputs) can be large enough to blow the old
# 8 GB default when a single year's observations are materialised in memory.
# Keep the safer default explicit here, while still allowing an override at the
# environment invocation site.
export SEFSTATS_MEM_MB="${SEFSTATS_MEM_MB:-16000}"
export SEFSTATS_TIME_MIN="${SEFSTATS_TIME_MIN:-60}"

EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR}"
EXPORTS="${EXPORTS},SEFSTATS_ROOT=${SEFSTATS_ROOT}"
EXPORTS="${EXPORTS},SEFSTATS_SEF_ROOT=${SEFSTATS_SEF_ROOT}"
EXPORTS="${EXPORTS},SEFSTATS_MEM_MB=${SEFSTATS_MEM_MB}"
EXPORTS="${EXPORTS},SEFSTATS_TIME_MIN=${SEFSTATS_TIME_MIN}"
EXPORTS="${EXPORTS},PDIR=${PDIR}"

ARRAY_RES="--qos=${SLURM_QOS} --ntasks=${SEFSTATS_CORES} --ntasks-per-core=1 --mem=${SEFSTATS_MEM_MB} --time=${SEFSTATS_TIME_MIN}"

ARRAY_ID=$(sbatch --parsable \
    ${ARRAY_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/sef_analysis_array.sbatch")
echo "Submitted SEF analysis array: ${ARRAY_ID} (0-${ARRAY_MAX}, ${NUM_YEARS} years)"

echo
echo "Pipeline submitted. Track with:  squeue -u \$USER"
echo "Parquet lands in: ${SEFSTATS_ROOT}/observations and ${SEFSTATS_ROOT}/daily_national"
