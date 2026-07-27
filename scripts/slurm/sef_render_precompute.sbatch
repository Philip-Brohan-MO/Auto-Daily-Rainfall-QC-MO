#!/bin/bash -l
#SBATCH --job-name=rqc_sefanim_precompute
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_sef_animation.sh via CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --ntasks-per-core=1
#SBATCH --mem=2000
#SBATCH --time=10

set -euo pipefail

SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${SEFANIM_DIR}" "${SEFANIM_FRAME_DIR}" "${SLURM_LOG_DIR}"

cd "${REPO_ROOT}"

echo "Building SEF-animation manifest -> ${SEFANIM_MANIFEST}"
run_py scripts/render_sef_animation_manifest.py \
    --sef-root "${SEFANIM_SEF_ROOT}" \
    --date-start "${SEFANIM_DATE_START}" \
    --date-end "${SEFANIM_DATE_END}" \
    --frames-per-day "${SEFANIM_FRAMES_PER_DAY}" \
    --num-shards "${SEFANIM_NUM_SHARDS}" \
    --fps "${SEFANIM_FPS}" \
    --cmap "${SEFANIM_CMAP}" \
    --vmax "${SEFANIM_VMAX}" \
    --marker-size "${SEFANIM_MARKER_SIZE}" \
    --error-color "${SEFANIM_ERROR_COLOR}" \
    --manifest-path "${SEFANIM_MANIFEST}" \
    --frame-dir "${SEFANIM_FRAME_DIR}"

echo "Precompute complete."
