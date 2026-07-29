#!/bin/bash -l
#SBATCH --job-name=rqc_sefanim_validate
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

cd "${REPO_ROOT}"

# The validate stage is data-source agnostic: it just checks every frame exists.
echo "Validating rendered frames against ${SEFANIM_MANIFEST}"
run_py scripts/render_animation_validate.py \
    --manifest-path "${SEFANIM_MANIFEST}"

echo "Validation passed."
