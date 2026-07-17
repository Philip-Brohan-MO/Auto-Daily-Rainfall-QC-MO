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

# Ensemble transcription ingest (sharded JSON -> ensemble_transcriptions.sqlite).
export ENSEMBLE_DB="${PDIR}/ensemble_transcriptions.sqlite"
export ENSEMBLE_SHARD_DIR="${PDIR}/ensemble_shards"

# --- Sharding / matching parameters -------------------------------------
export NUM_SHARDS="${NUM_SHARDS:-100}"
export TOP_K="${TOP_K:-10}"
export MIN_OVERLAP="${MIN_OVERLAP:-10}"
export UNCERTAINTY_WEIGHT="${UNCERTAINTY_WEIGHT:-0.15}"
export BATCH_SIZE="${BATCH_SIZE:-8192}"
export PROGRESS_INTERVAL="${PROGRESS_INTERVAL:-50}"

# --- Ensemble-ingest sharding parameters --------------------------------
export ENSEMBLE_NUM_SHARDS="${ENSEMBLE_NUM_SHARDS:-100}"
# Empty = ingest all discovered JSON files; set to a small number for testing.
export ENSEMBLE_MAX_FILES="${ENSEMBLE_MAX_FILES:-}"

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

# Ensemble-ingest stages (JSON parsing is single-threaded and I/O bound).
export EINGEST_CORES="${EINGEST_CORES:-1}"
export EINGEST_MEM_MB="${EINGEST_MEM_MB:-4000}"
export EINGEST_TIME_MIN="${EINGEST_TIME_MIN:-30}"

export EMERGE_CORES="${EMERGE_CORES:-1}"
export EMERGE_MEM_MB="${EMERGE_MEM_MB:-8000}"
export EMERGE_TIME_MIN="${EMERGE_TIME_MIN:-30}"

# --- Rainfall animation pipeline ----------------------------------------
# Interpolated daily-rainfall map animation (precompute -> render array ->
# validate -> encode). Frames are rendered in parallel across RENDER_NUM_SHARDS
# array tasks, published to RENDER_FRAME_DIR, then encoded once to MP4.
export ANIMATION_DIR="${ANIMATION_DIR:-${PDIR}/animation}"
export RENDER_MANIFEST="${RENDER_MANIFEST:-${ANIMATION_DIR}/manifest.json}"
export RENDER_FRAME_DIR="${RENDER_FRAME_DIR:-${ANIMATION_DIR}/frames}"

# Date range and interpolation density. Defaults target the 1931 test range;
# override for the full available span.
export RENDER_DATE_START="${RENDER_DATE_START:-1931-01-01}"
export RENDER_DATE_END="${RENDER_DATE_END:-1931-12-31}"
# In-between frames per day step (higher = smoother, more frames/output).
export RENDER_FRAMES_PER_DAY="${RENDER_FRAMES_PER_DAY:-6}"
export RENDER_FPS="${RENDER_FPS:-30}"
export RENDER_CMAP="${RENDER_CMAP:-YlGnBu}"
export RENDER_VMAX="${RENDER_VMAX:-2.0}"
export RENDER_MARKER_SIZE="${RENDER_MARKER_SIZE:-9.0}"
export RENDER_KEEP_FRAMES="${RENDER_KEEP_FRAMES:-0}"

# Parallelism: number of render array tasks.
export RENDER_NUM_SHARDS="${RENDER_NUM_SHARDS:-100}"

# Per-stage resource requests (cores via --ntasks, RAM in MB, time in minutes).
export RPRECOMPUTE_CORES="${RPRECOMPUTE_CORES:-1}"
export RPRECOMPUTE_MEM_MB="${RPRECOMPUTE_MEM_MB:-2000}"
export RPRECOMPUTE_TIME_MIN="${RPRECOMPUTE_TIME_MIN:-10}"

export RRENDER_CORES="${RRENDER_CORES:-1}"
export RRENDER_MEM_MB="${RRENDER_MEM_MB:-4000}"
export RRENDER_TIME_MIN="${RRENDER_TIME_MIN:-30}"

export RVALIDATE_CORES="${RVALIDATE_CORES:-1}"
export RVALIDATE_MEM_MB="${RVALIDATE_MEM_MB:-2000}"
export RVALIDATE_TIME_MIN="${RVALIDATE_TIME_MIN:-10}"

export RENCODE_CORES="${RENCODE_CORES:-16}"
export RENCODE_MEM_MB="${RENCODE_MEM_MB:-32000}"
export RENCODE_TIME_MIN="${RENCODE_TIME_MIN:-240}"

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
