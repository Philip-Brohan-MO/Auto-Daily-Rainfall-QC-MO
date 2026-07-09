#!/bin/bash -l
#SBATCH --job-name=rqc_build
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
# Default resource requests (overridden by submit_all.sh via sbatch CLI flags).
#SBATCH --qos=normal
#SBATCH --ntasks=2
#SBATCH --ntasks-per-core=1
#SBATCH --mem=8000
#SBATCH --time=20

set -euo pipefail

# sbatch copies this script to a spool dir, so BASH_SOURCE can't find config.sh;
# submit_all.sh exports RQC_SLURM_DIR. Fall back to BASH_SOURCE for direct runs.
SLURM_DIR="${RQC_SLURM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
source "${SLURM_DIR}/config.sh"
set_thread_env

mkdir -p "${SHARD_DIR}" "${SLURM_LOG_DIR}"

cd "${REPO_ROOT}"

echo "Building comparison vectors -> ${COMPARISON_DB}"
run_py scripts/build_similarity_vectors.py \
    --comparison-db-path "${COMPARISON_DB}"

echo "Build complete."
