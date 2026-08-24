#!/bin/bash -l
#SBATCH --job-name=rqc_as_match
#SBATCH --output=%x_%A_%a.out
#SBATCH --error=%x_%A_%a.err
# Default resource requests (overridden by submit_allsheets.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=2
#SBATCH --ntasks-per-core=1
#SBATCH --mem=6000
#SBATCH --time=60
# NOTE: the array range is set at submit time (see submit_allsheets.sh) to match
#       ALLSHEETS_NUM_SHARDS, e.g. --array=0-99. A default is provided below.
#SBATCH --array=0-99

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_allsheets.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${ALLSHEETS_SIMILARITY_SHARD_DIR}"

cd "${REPO_ROOT}"

echo "ALLSHEETS shard ${SLURM_ARRAY_TASK_ID}/${ALLSHEETS_NUM_SHARDS} matching residual queries"
run_py scripts/run_similarity_shard.py \
    --backend duckdb \
    --comparison-root "${ALLSHEETS_COMPARISON_PARQUET_ROOT}" \
    --shard-dir "${ALLSHEETS_SIMILARITY_SHARD_DIR}" \
    --num-shards "${ALLSHEETS_NUM_SHARDS}" \
    --shard-index "${SLURM_ARRAY_TASK_ID}" \
    --top-k "${TOP_K}" \
    --min-overlap "${MIN_OVERLAP}" \
    --uncertainty-weight "${UNCERTAINTY_WEIGHT}" \
    --batch-size "${BATCH_SIZE}" \
    --progress-interval "${PROGRESS_INTERVAL}"

echo "ALLSHEETS shard ${SLURM_ARRAY_TASK_ID} complete."
