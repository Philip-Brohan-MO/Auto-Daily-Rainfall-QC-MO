#!/bin/bash -l
#SBATCH --job-name=rqc_sefanim_encode
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_sef_animation.sh via CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=4000
#SBATCH --time=30

set -euo pipefail

SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

cd "${REPO_ROOT}"

# The encode stage is data-source agnostic: it ffmpegs the manifest's frames.
echo "Encoding frames to MP4 from ${SEFANIM_MANIFEST}"
if [[ "${SEFANIM_KEEP_FRAMES}" == "1" ]]; then
    run_py scripts/render_animation_encode.py \
        --manifest-path "${SEFANIM_MANIFEST}" \
        --keep-frames
else
    run_py scripts/render_animation_encode.py \
        --manifest-path "${SEFANIM_MANIFEST}"
fi

echo "Encode complete."
