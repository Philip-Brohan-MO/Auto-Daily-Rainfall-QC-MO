#!/bin/bash -l
#SBATCH --job-name=rqc_anim_render
#SBATCH --output=%x_%A_%a.out
#SBATCH --error=%x_%A_%a.err
# Default resource requests (overridden by submit_animation.sh via CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=4000
#SBATCH --time=30
# NOTE: the array range is set at submit time to match RENDER_NUM_SHARDS.
#SBATCH --array=0-99

set -euo pipefail

SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${RENDER_FRAME_DIR}"

cd "${REPO_ROOT}"

echo "Render shard ${SLURM_ARRAY_TASK_ID}/${RENDER_NUM_SHARDS}"
run_py scripts/render_animation_shard.py \
    --manifest-path "${RENDER_MANIFEST}" \
    --ensemble-db "${ENSEMBLE_DB}" \
    --shard-index "${SLURM_ARRAY_TASK_ID}"

echo "Render shard ${SLURM_ARRAY_TASK_ID} complete."
