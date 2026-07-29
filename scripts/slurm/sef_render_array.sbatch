#!/bin/bash -l
#SBATCH --job-name=rqc_sefanim_render
#SBATCH --output=%x_%A_%a.out
#SBATCH --error=%x_%A_%a.err
# Default resource requests (overridden by submit_sef_animation.sh via CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=8000
#SBATCH --time=30
# NOTE: the array range is set at submit time to match SEFANIM_NUM_SHARDS.
#SBATCH --array=0-99

set -euo pipefail

SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${SEFANIM_FRAME_DIR}"

cd "${REPO_ROOT}"

echo "SEF render shard ${SLURM_ARRAY_TASK_ID}/${SEFANIM_NUM_SHARDS}"
run_py scripts/render_sef_animation_shard.py \
    --manifest-path "${SEFANIM_MANIFEST}" \
    --shard-index "${SLURM_ARRAY_TASK_ID}"

echo "SEF render shard ${SLURM_ARRAY_TASK_ID} complete."
