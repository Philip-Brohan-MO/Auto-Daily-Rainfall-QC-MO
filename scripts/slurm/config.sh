# Shared configuration for the SLURM similarity-matching workflow.
# Source this from the sbatch scripts and the submit driver.

# --- Paths ---------------------------------------------------------------
export REPO_ROOT="/home/users/philip.brohan/Projects/Auto-Daily-Rainfall-QC-MO"
export CONDA_ENV_PREFIX="/data/users/philip.brohan/conda/environments/ADRQ"

# PDIR holds the SQLite databases and shard outputs (shared disc).
export PDIR="${PDIR:-/data/scratch/philip.brohan/ADRQ}"

export COMPARISON_DB="${PDIR}/monthly_similarity.sqlite"
export SHARD_DIR="${PDIR}/similarity_shards"
export SLURM_LOG_DIR="${PDIR}/slurm_logs"

# --- Sharding / matching parameters -------------------------------------
export NUM_SHARDS="${NUM_SHARDS:-100}"
export TOP_K="${TOP_K:-10}"
export MIN_OVERLAP="${MIN_OVERLAP:-10}"
export UNCERTAINTY_WEIGHT="${UNCERTAINTY_WEIGHT:-0.15}"
export BATCH_SIZE="${BATCH_SIZE:-8192}"
export PROGRESS_INTERVAL="${PROGRESS_INTERVAL:-50}"

# --- SLURM resource requests (qos / cores / RAM / time) -----------------
# These are passed to sbatch by submit_all.sh, so CLI flags override the
# #SBATCH defaults baked into each *.sbatch file. QOS: high | normal | low.
# Cores are requested via --ntasks (SPICE convention); RAM is in MB; time in
# minutes.
export SLURM_QOS="${SLURM_QOS:-normal}"

export BUILD_CORES="${BUILD_CORES:-2}"
export BUILD_MEM_MB="${BUILD_MEM_MB:-8000}"
export BUILD_TIME_MIN="${BUILD_TIME_MIN:-20}"

export MATCH_CORES="${MATCH_CORES:-2}"
export MATCH_MEM_MB="${MATCH_MEM_MB:-4000}"
export MATCH_TIME_MIN="${MATCH_TIME_MIN:-15}"

export MERGE_CORES="${MERGE_CORES:-1}"
export MERGE_MEM_MB="${MERGE_MEM_MB:-4000}"
export MERGE_TIME_MIN="${MERGE_TIME_MIN:-20}"

# --- Python runner -------------------------------------------------------
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH}"

# Pin BLAS/OpenMP thread counts to the cores SLURM actually gave this job so a
# single NumPy process uses exactly its allocation (call inside each sbatch).
set_thread_env() {
    local cores="${SLURM_NTASKS:-${SLURM_CPUS_PER_TASK:-1}}"
    export OMP_NUM_THREADS="${cores}"
    export OPENBLAS_NUM_THREADS="${cores}"
    export MKL_NUM_THREADS="${cores}"
    export VECLIB_MAXIMUM_THREADS="${cores}"
    export NUMEXPR_NUM_THREADS="${cores}"
}

# Helper to run a repo script inside the conda env.
run_py() {
    conda run -p "${CONDA_ENV_PREFIX}" --no-capture-output python "$@"
}
