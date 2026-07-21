#!/bin/bash -l
#SBATCH --job-name=rqc_emerge
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_ensemble_ingest.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=8000
#SBATCH --time=30

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# the submit driver exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

cd "${REPO_ROOT}"

echo "Merging ${ENSEMBLE_NUM_SHARDS} ensemble shards from ${ENSEMBLE_SHARD_DIR} into ${ENSEMBLE_DB}"
run_py scripts/merge_ensemble_shards.py \
    --ensemble-db-path "${ENSEMBLE_DB}" \
    --shard-dir "${ENSEMBLE_SHARD_DIR}" \
    ${ENSEMBLE_ROOT:+--ensemble-root "${ENSEMBLE_ROOT}"} \
    --expected-shards "${ENSEMBLE_NUM_SHARDS}"

echo "Ensemble merge complete."
