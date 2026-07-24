#!/bin/bash
# Precompute the daily consensus table (median rainfall per file/day) as two
# dependent SLURM stages:
#   1. daily_consensus_array   (CONSENSUS_NUM_SHARDS array tasks)
#   2. daily_consensus_merge   (single job, after the whole array succeeds)
#
# This is a ONE-TIME prerequisite for the regional-stats pipeline: it moves the
# expensive holistic median out of every regional shard (where a nationally-
# scattered neighbour pool OOM-kills the task) into cheap, low-memory shards
# partitioned by contiguous file_id range. Rerun only if ensemble_daily_values
# changes.
#
# Usage:
#   scripts/slurm/submit_daily_consensus.sh
#   CONSENSUS_TOTAL_FILE_IDS=584513 scripts/slurm/submit_daily_consensus.sh
#
# Requires: sbatch on PATH, PDIR set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

mkdir -p "${CONSENSUS_SHARD_DIR}" "${SLURM_LOG_DIR}"

# Avoid mixing stale shard files from an earlier failed run with new output.
rm -f "${CONSENSUS_SHARD_DIR}"/consensus_shard_*.parquet

ARRAY_MAX=$(( CONSENSUS_NUM_SHARDS - 1 ))

EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR}"
EXPORTS="${EXPORTS},CONSENSUS_NUM_SHARDS=${CONSENSUS_NUM_SHARDS}"
EXPORTS="${EXPORTS},CONSENSUS_TOTAL_FILE_IDS=${CONSENSUS_TOTAL_FILE_IDS}"
EXPORTS="${EXPORTS},CONSENSUS_SHARD_DIR=${CONSENSUS_SHARD_DIR}"
EXPORTS="${EXPORTS},CONSENSUS_ROOT=${CONSENSUS_ROOT}"
EXPORTS="${EXPORTS},PDIR=${PDIR}"

ARRAY_RES="--qos=${SLURM_QOS} --ntasks=${CONSENSUS_CORES} --ntasks-per-core=1 --mem=${CONSENSUS_MEM_MB} --time=${CONSENSUS_TIME_MIN}"
MERGE_RES="--qos=${SLURM_QOS} --ntasks=${CONSENSUS_MERGE_CORES} --ntasks-per-core=1 --mem=${CONSENSUS_MERGE_MEM_MB} --time=${CONSENSUS_MERGE_TIME_MIN}"

ARRAY_ID=$(sbatch --parsable \
    ${ARRAY_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/daily_consensus_array.sbatch")
echo "Submitted daily-consensus array: ${ARRAY_ID} (0-${ARRAY_MAX}, ${CONSENSUS_NUM_SHARDS} shards)"

MERGE_ID=$(sbatch --parsable \
    --dependency=afterok:${ARRAY_ID} \
    ${MERGE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/daily_consensus_merge.sbatch")
echo "Submitted daily-consensus merge: ${MERGE_ID}"

echo
echo "Pipeline submitted. Track with:  squeue -u \$USER"
echo "Consensus table lands in: ${CONSENSUS_ROOT}/daily_consensus/daily_consensus.parquet"
echo "Then run: scripts/slurm/submit_regional_stats.sh"
