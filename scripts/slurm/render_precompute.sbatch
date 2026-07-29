#!/bin/bash -l
#SBATCH --job-name=rqc_anim_precompute
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_animation.sh via CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=2000
#SBATCH --time=10

set -euo pipefail

SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${ANIMATION_DIR}" "${RENDER_FRAME_DIR}" "${SLURM_LOG_DIR}"

cd "${REPO_ROOT}"

echo "Building animation manifest -> ${RENDER_MANIFEST}"
run_py scripts/render_animation_manifest.py \
    --date-start "${RENDER_DATE_START}" \
    --date-end "${RENDER_DATE_END}" \
    --frames-per-day "${RENDER_FRAMES_PER_DAY}" \
    --num-shards "${RENDER_NUM_SHARDS}" \
    --fps "${RENDER_FPS}" \
    --cmap "${RENDER_CMAP}" \
    --vmax "${RENDER_VMAX}" \
    --marker-size "${RENDER_MARKER_SIZE}" \
    --manifest-path "${RENDER_MANIFEST}" \
    --frame-dir "${RENDER_FRAME_DIR}"

echo "Precompute complete."
