# Shared configuration for the SLURM similarity-matching workflow.
# Source this from the sbatch scripts and the submit driver.

# --- Paths ---------------------------------------------------------------
export REPO_ROOT="/home/users/philip.brohan/Projects/Auto-Daily-Rainfall-QC-MO"
export CONDA_ENV_PREFIX="/data/users/philip.brohan/conda/environments/ADRQ"

# PDIR holds the SQLite databases and shard outputs (shared disc).
export PDIR="${PDIR:-/data/scratch/philip.brohan/ADRQ}"

export COMPARISON_DB="${PDIR}/monthly_similarity.sqlite"
export SHARD_DIR="${PDIR}/similarity_shards"
export COMPARISON_PARQUET_ROOT="${PDIR}/monthly_similarity_parquet"
export SIMILARITY_SHARD_DIR="${PDIR}/similarity_shards_parquet"
export SLURM_LOG_DIR="${PDIR}/slurm_logs"

# Ensemble transcription ingest (sharded JSON -> ensemble_transcriptions.sqlite).
export ENSEMBLE_DB="${PDIR}/ensemble_transcriptions.sqlite"
export ENSEMBLE_SHARD_DIR="${PDIR}/ensemble_shards"
export ENSEMBLE_PARQUET_ROOT="${PDIR}/ensemble_transcriptions_parquet"

# Root of the ensemble transcription JSON tree to ingest. Set this (or the
# legacy ENSEMBLE_TRANSCRIPTIONS_ROOT) to point at the full dataset; if left
# empty the Python package default (the operational sample) is used. This value
# is passed explicitly to every job via --ensemble-root, so it never relies on
# environment propagation.
export ENSEMBLE_ROOT="${ENSEMBLE_ROOT:-${ENSEMBLE_TRANSCRIPTIONS_ROOT:-}}"

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
export BUILD_MEM_MB="${BUILD_MEM_MB:-12000}"  # build streams vectors to Parquet; DuckDB memory_limit is capped to this allocation in build_vectors.sbatch
export BUILD_TIME_MIN="${BUILD_TIME_MIN:-120}"  # full dataset: ~34M member rows aggregated in DuckDB

export MATCH_CORES="${MATCH_CORES:-2}"
export MATCH_MEM_MB="${MATCH_MEM_MB:-4000}"  # RR candidates fixed + queries streamed -> memory flat
export MATCH_TIME_MIN="${MATCH_TIME_MIN:-60}"  # full dataset: ~12.7x queries/shard (sample ~85s -> ~15-18min)

export MERGE_CORES="${MERGE_CORES:-1}"
export MERGE_MEM_MB="${MERGE_MEM_MB:-8000}"  # merge streams via DuckDB COPY; sort of merged rows spills to node-local scratch (see merge_shards.sbatch)
export MERGE_TIME_MIN="${MERGE_TIME_MIN:-40}"  # full dataset: ~12.7x shard rows to combine

# Ensemble-ingest stages (JSON parsing is single-threaded and I/O bound).
# Sized for the full operational dataset (~584k JSON files -> ~1.1e9 rows). With
# 100 shards each array task parses ~5.8k files (~50 min) and writes ~11M rows;
# the merge then combines ~1.1e9 rows and builds the indexes at the end.
# For a quick ENSEMBLE_MAX_FILES smoke run these can be lowered to speed queuing.
export EINGEST_CORES="${EINGEST_CORES:-1}"
export EINGEST_MEM_MB="${EINGEST_MEM_MB:-8000}"
export EINGEST_TIME_MIN="${EINGEST_TIME_MIN:-120}"

export EMERGE_CORES="${EMERGE_CORES:-1}"
export EMERGE_MEM_MB="${EMERGE_MEM_MB:-24000}"
export EMERGE_TIME_MIN="${EMERGE_TIME_MIN:-360}"  # cpu partition max wall (6 h)

# Node-local scratch reserved per job via --gres=tmp:N (N in MB; nodes advertise
# ~1.7e6 MB / ~1.66 TB). Every SQLite DB is built on this scratch and copied to
# shared disc at the end. The merge holds the entire combined DB (~77 GB for the
# full dataset) plus index-sort temp files, so it needs a large reservation; each
# ingest shard only writes its own slice (< 1 GB for the full dataset).
export EINGEST_TMP_MB="${EINGEST_TMP_MB:-16000}"
export EMERGE_TMP_MB="${EMERGE_TMP_MB:-300000}"

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

# --- QC pipeline (exact monthly consistency check) ----------------------
# Shard count, output dir, and parameters. 100 shards over ~514k file_ids
# → ~5140 file_ids per shard (~9 min each based on smoke-test timing).
export QC_NUM_SHARDS="${QC_NUM_SHARDS:-100}"
export QC_SHARD_DIR="${PDIR}/qc_shards"
export QC_TOLERANCE="${QC_TOLERANCE:-0.01}"
# Total file_ids in the ensemble DB. Passed to each shard so it can compute
# its start/end file_id slice.  Run:
#   sqlite3 $PDIR/ensemble_transcriptions.sqlite \
#       'SELECT MAX(file_id) FROM ensemble_files'
# to refresh this number after a new ingest.
export QC_TOTAL_FILE_IDS="${QC_TOTAL_FILE_IDS:-514000}"

export QC_CORES="${QC_CORES:-1}"
export QC_MEM_MB="${QC_MEM_MB:-8000}"
export QC_TIME_MIN="${QC_TIME_MIN:-30}"

export QC_MERGE_CORES="${QC_MERGE_CORES:-1}"
export QC_MERGE_MEM_MB="${QC_MERGE_MEM_MB:-24000}"
export QC_MERGE_TIME_MIN="${QC_MERGE_TIME_MIN:-180}"

# --- Daily-consensus precompute (prerequisite for regional stats) ---------
# Compute median(rainfall) per (file_id, month, day_of_month) ONCE, sharded by
# CONTIGUOUS file_id range. Contiguous ranges let DuckDB prune ensemble_daily_
# values row groups, so each shard's holistic median only buffers its own slice
# and stays small. Regional stats then reads this table instead of recomputing
# the median over a nationally-scattered neighbour pool (which OOM-kills tasks).
export CONSENSUS_NUM_SHARDS="${CONSENSUS_NUM_SHARDS:-100}"
export CONSENSUS_SHARD_DIR="${PDIR}/daily_consensus_shards"
export CONSENSUS_ROOT="${PDIR}/daily_consensus_parquet"
export CONSENSUS_TOTAL_FILE_IDS="${CONSENSUS_TOTAL_FILE_IDS:-680000}"
export CONSENSUS_CORES="${CONSENSUS_CORES:-1}"
export CONSENSUS_MEM_MB="${CONSENSUS_MEM_MB:-12000}"
export CONSENSUS_TIME_MIN="${CONSENSUS_TIME_MIN:-30}"
export CONSENSUS_MERGE_CORES="${CONSENSUS_MERGE_CORES:-1}"
export CONSENSUS_MERGE_MEM_MB="${CONSENSUS_MERGE_MEM_MB:-24000}"
export CONSENSUS_MERGE_TIME_MIN="${CONSENSUS_MERGE_TIME_MIN:-120}"

# --- Regional-stats pipeline (QC check 2, stage 1: neighbour statistics) --
# For every located station-day, compute neighbour count / median / MAD at
# 20 km and 50 km from station-days that passed QC check 1. Sharded by target
# file_id; each shard scopes its own neighbour pool (same year, target bounding
# box + 50 km). The daily consensus is READ from the precomputed CONSENSUS_ROOT
# table (build it first with submit_daily_consensus.sh), which keeps memory
# bounded -- recomputing the median inline OOM-kills nationally-spread shards.
export REGIONAL_NUM_SHARDS="${REGIONAL_NUM_SHARDS:-200}"
export REGIONAL_SHARD_DIR="${PDIR}/regional_stats_shards"
# Total file_ids (targets are the located subset). Passed to each shard to
# compute its start/end slice. Refresh with the max file_id from the ensemble
# dataset (see the "Max file_id" cell in notebooks/qc_RR_monthly_total.ipynb).
export REGIONAL_TOTAL_FILE_IDS="${REGIONAL_TOTAL_FILE_IDS:-680000}"

export REGIONAL_CORES="${REGIONAL_CORES:-1}"
export REGIONAL_MEM_MB="${REGIONAL_MEM_MB:-12000}"
export REGIONAL_TIME_MIN="${REGIONAL_TIME_MIN:-90}"

export REGIONAL_MERGE_CORES="${REGIONAL_MERGE_CORES:-1}"
export REGIONAL_MERGE_MEM_MB="${REGIONAL_MERGE_MEM_MB:-24000}"
export REGIONAL_MERGE_TIME_MIN="${REGIONAL_MERGE_TIME_MIN:-120}"

# --- Python runner -------------------------------------------------------
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH}"

# Pin BLAS/OpenMP thread counts to the cores SLURM actually gave this job so a
# single NumPy process uses exactly its allocation (call inside each sbatch).
set_thread_env() {
    local cores="${SLURM_CPUS_PER_TASK:-${SLURM_NTASKS:-1}}"
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
