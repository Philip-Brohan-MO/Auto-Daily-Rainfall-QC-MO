#!/bin/bash -l
#SBATCH --job-name=rqc_merge
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_all.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=4000
#SBATCH --time=20

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_all.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

cd "${REPO_ROOT}"

echo "Merging ${NUM_SHARDS} shards from ${SHARD_DIR} into ${COMPARISON_DB}"
run_py scripts/merge_similarity_shards.py \
    --comparison-db-path "${COMPARISON_DB}" \
    --shard-dir "${SHARD_DIR}" \
    --top-k "${TOP_K}" \
    --min-overlap "${MIN_OVERLAP}" \
    --uncertainty-weight "${UNCERTAINTY_WEIGHT}" \
    --expected-shards "${NUM_SHARDS}"

echo "Merge complete."
