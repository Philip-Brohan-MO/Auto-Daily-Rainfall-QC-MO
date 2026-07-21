#!/bin/bash -l
#SBATCH --job-name=rqc_eingest
#SBATCH --output=%x_%A_%a.out
#SBATCH --error=%x_%A_%a.err
# Default resource requests (overridden by submit_ensemble_ingest.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=4000
#SBATCH --time=30
# NOTE: the array range is set at submit time (see submit_ensemble_ingest.sh) to
#       match ENSEMBLE_NUM_SHARDS, e.g. --array=0-99. A default is provided below.
#SBATCH --array=0-99

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# the submit driver exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${ENSEMBLE_SHARD_DIR}"

cd "${REPO_ROOT}"

echo "Ensemble shard ${SLURM_ARRAY_TASK_ID}/${ENSEMBLE_NUM_SHARDS} ingesting its JSON slice"
run_py scripts/run_ensemble_ingest_shard.py \
    --shard-dir "${ENSEMBLE_SHARD_DIR}" \
    --num-shards "${ENSEMBLE_NUM_SHARDS}" \
    --shard-index "${SLURM_ARRAY_TASK_ID}" \
    ${ENSEMBLE_ROOT:+--ensemble-root "${ENSEMBLE_ROOT}"} \
    ${ENSEMBLE_MAX_FILES:+--max-files "${ENSEMBLE_MAX_FILES}"}

echo "Ensemble shard ${SLURM_ARRAY_TASK_ID} complete."
