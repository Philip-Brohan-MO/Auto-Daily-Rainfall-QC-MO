#!/bin/bash
# Submit the regional neighbour-statistics pipeline (QC check 2, stage 1) as two
# dependent SLURM stages:
#   1. regional_stats_array   (REGIONAL_NUM_SHARDS array tasks)
#   2. regional_stats_merge   (single job, starts after the whole array succeeds)
#
# Usage:
#   scripts/slurm/submit_regional_stats.sh
#   REGIONAL_NUM_SHARDS=400 scripts/slurm/submit_regional_stats.sh
#   REGIONAL_TOTAL_FILE_IDS=680000 scripts/slurm/submit_regional_stats.sh
#
# Prerequisites:
#   1. QC check 1 must have been run (daily_qc_status must exist) -- the
#      neighbour pool is drawn from station-days that passed that check.
#   2. The daily-consensus table must have been built once with
#      scripts/slurm/submit_daily_consensus.sh (this script checks for it).
#
# After the merge finishes, inspect results in the notebook:
#   notebooks/qc_RR_regional_stats.ipynb
#
# Each shard READS the precomputed daily_consensus table rather than recomputing
# a holistic median over its (nationally-scattered) neighbour pool -- that
# earlier inline median OOM-killed the array tasks. The remaining cost is the
# spatial self-join, which spills to disk under the DuckDB memory cap.
#
# Requires: sbatch on PATH, PDIR set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

mkdir -p "${REGIONAL_SHARD_DIR}" "${SLURM_LOG_DIR}"

# The daily-consensus table is a required input (built once by
# submit_daily_consensus.sh) -- reading it keeps each shard within memory
# instead of recomputing a national holistic median that OOM-kills the task.
CONSENSUS_FILE="${CONSENSUS_ROOT}/daily_consensus/daily_consensus.parquet"
if [[ ! -f "${CONSENSUS_FILE}" ]]; then
    echo "ERROR: daily-consensus table not found at:" >&2
    echo "  ${CONSENSUS_FILE}" >&2
    echo "Build it first with:  scripts/slurm/submit_daily_consensus.sh" >&2
    exit 1
fi

# Avoid mixing stale shard files from an earlier failed run with new output.
rm -f "${REGIONAL_SHARD_DIR}"/regional_shard_*.parquet

ARRAY_MAX=$(( REGIONAL_NUM_SHARDS - 1 ))

EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR}"
EXPORTS="${EXPORTS},REGIONAL_NUM_SHARDS=${REGIONAL_NUM_SHARDS}"
EXPORTS="${EXPORTS},REGIONAL_TOTAL_FILE_IDS=${REGIONAL_TOTAL_FILE_IDS}"
EXPORTS="${EXPORTS},REGIONAL_SHARD_DIR=${REGIONAL_SHARD_DIR}"
EXPORTS="${EXPORTS},CONSENSUS_ROOT=${CONSENSUS_ROOT}"
EXPORTS="${EXPORTS},PDIR=${PDIR}"

ARRAY_RES="--qos=${SLURM_QOS} --ntasks=${REGIONAL_CORES} --ntasks-per-core=1 --mem=${REGIONAL_MEM_MB} --time=${REGIONAL_TIME_MIN}"
MERGE_RES="--qos=${SLURM_QOS} --ntasks=${REGIONAL_MERGE_CORES} --ntasks-per-core=1 --mem=${REGIONAL_MERGE_MEM_MB} --time=${REGIONAL_MERGE_TIME_MIN}"

ARRAY_ID=$(sbatch --parsable \
    ${ARRAY_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/regional_stats_array.sbatch")
echo "Submitted regional-stats array: ${ARRAY_ID} (0-${ARRAY_MAX}, ${REGIONAL_NUM_SHARDS} shards)"

MERGE_ID=$(sbatch --parsable \
    --dependency=afterok:${ARRAY_ID} \
    ${MERGE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/regional_stats_merge.sbatch")
echo "Submitted regional-stats merge: ${MERGE_ID}"

echo
echo "Pipeline submitted. Track with:  squeue -u \$USER"
echo "Shards land in: ${REGIONAL_SHARD_DIR}"
echo "Results land in: ${PDIR}/regional_stats_parquet/regional_daily_stats"
