#!/bin/bash -l
#SBATCH --job-name=rqc_match
#SBATCH --output=%x_%A_%a.out
#SBATCH --error=%x_%A_%a.err
# Default resource requests (overridden by submit_all.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=2
#SBATCH --ntasks-per-core=1
#SBATCH --mem=4000
#SBATCH --time=15
# NOTE: the array range is set at submit time (see submit_all.sh) to match
#       NUM_SHARDS, e.g. --array=0-99. A default is provided below.
#SBATCH --array=0-99

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_all.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${SHARD_DIR}"

cd "${REPO_ROOT}"

echo "Shard ${SLURM_ARRAY_TASK_ID}/${NUM_SHARDS} matching against all RR candidates"
run_py scripts/run_similarity_shard.py \
    --comparison-db-path "${COMPARISON_DB}" \
    --shard-dir "${SHARD_DIR}" \
    --num-shards "${NUM_SHARDS}" \
    --shard-index "${SLURM_ARRAY_TASK_ID}" \
    --top-k "${TOP_K}" \
    --min-overlap "${MIN_OVERLAP}" \
    --uncertainty-weight "${UNCERTAINTY_WEIGHT}" \
    --batch-size "${BATCH_SIZE}" \
    --progress-interval "${PROGRESS_INTERVAL}"

echo "Shard ${SLURM_ARRAY_TASK_ID} complete."
