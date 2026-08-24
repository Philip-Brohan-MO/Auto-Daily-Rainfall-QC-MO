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

# --- Transcription-source QC (bad sources + duplicate detection) ---------
# Two dependent stages (array -> merge), independent of the RR match / metadata:
#   1. array : each shard computes per-file QC metrics (rainfall-day counts +
#      the 372-value daily consensus vector) for its CONTIGUOUS file_id slice
#      and writes tqc_shard_XXXXX.parquet. Sharding by file_id lets DuckDB prune
#      row groups so the heavy daily aggregation stays memory-bounded per task.
#   2. merge : flags bad sources (nonzero_days < TQC_MIN_NONZERO_DAYS) and
#      detects duplicate sources purely from content -- LSH banding over the
#      rounded monthly sums to block candidates, then day-level agreement over
#      the full 372 daily values -- then writes the sessioned outputs under
#      TQC_ROOT.
# Submit with submit_transcription_qc.sh.
export TQC_ROOT="${TQC_ROOT:-${PDIR}/transcription_qc_parquet}"
export TQC_SHARD_DIR="${TQC_SHARD_DIR:-${PDIR}/transcription_qc_shards}"
export TQC_NUM_SHARDS="${TQC_NUM_SHARDS:-100}"
# Upper bound on file_id (used to compute each shard's slice). Refresh with the
# max file_id from the ensemble dataset after a new ingest.
export TQC_TOTAL_FILE_IDS="${TQC_TOTAL_FILE_IDS:-680000}"

# Bad-source flag: a source is bad when fewer than this many of its 372 day-cells
# hold real rainfall (non-null and > 0). Pick from the nonzero_days distribution
# in Daily_transcriptions_ingest.
export TQC_MIN_NONZERO_DAYS="${TQC_MIN_NONZERO_DAYS:-20}"

# Duplicate-detection tunables (content-only, day-level scoring).
export TQC_ROUND_DECIMALS="${TQC_ROUND_DECIMALS:-1}"
export TQC_MATCH_TOL="${TQC_MATCH_TOL:-0.2}"
export TQC_MIN_OVERLAP_DAYS="${TQC_MIN_OVERLAP_DAYS:-60}"
export TQC_MIN_AGREEMENT="${TQC_MIN_AGREEMENT:-0.9}"
export TQC_MAX_BLOCK="${TQC_MAX_BLOCK:-2000}"

export TQC_CORES="${TQC_CORES:-1}"
export TQC_MEM_MB="${TQC_MEM_MB:-8000}"
export TQC_TIME_MIN="${TQC_TIME_MIN:-30}"

export TQC_MERGE_CORES="${TQC_MERGE_CORES:-2}"
export TQC_MERGE_MEM_MB="${TQC_MERGE_MEM_MB:-24000}"
export TQC_MERGE_TIME_MIN="${TQC_MERGE_TIME_MIN:-120}"

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

# --- Secondary-QC models (QC check 2, stage 2: XGBoost expectation test) --
# Two dependent single-node jobs:
#   1. secondary_qc_train  -- fit model 1 (predict a station's consensus from its
#      regional neighbour stats) and model 2 (predict model 1's absolute error)
#      on the QC1-pass rows; calibrate the range multiplier k; persist the models.
#   2. secondary_qc_score  -- apply the models to the QC1-fail rows and flag each
#      pass / fail / indeterminate.
# Training reads a month-stratified sample (SECONDARY_MAX_TRAIN_ROWS) so it fits
# in memory; scoring streams the fail rows so its memory stays flat.
export SECONDARY_QC_ROOT="${SECONDARY_QC_ROOT:-${PDIR}/secondary_qc_parquet}"
export SECONDARY_MAX_TRAIN_ROWS="${SECONDARY_MAX_TRAIN_ROWS:-5000000}"
export SECONDARY_COVERAGE_TARGET="${SECONDARY_COVERAGE_TARGET:-0.99}"
export SECONDARY_SEED="${SECONDARY_SEED:-0}"

export SECONDARY_TRAIN_CORES="${SECONDARY_TRAIN_CORES:-8}"
export SECONDARY_TRAIN_MEM_MB="${SECONDARY_TRAIN_MEM_MB:-32000}"
export SECONDARY_TRAIN_TIME_MIN="${SECONDARY_TRAIN_TIME_MIN:-120}"

export SECONDARY_SCORE_CORES="${SECONDARY_SCORE_CORES:-4}"
export SECONDARY_SCORE_MEM_MB="${SECONDARY_SCORE_MEM_MB:-16000}"
export SECONDARY_SCORE_TIME_MIN="${SECONDARY_SCORE_TIME_MIN:-60}"

# --- SEF export (share located, QC'd daily rainfall in Station Exchange Format)
# Write one SEF .tsv per REAL station-year, carrying every day's consensus daily
# total (converted inches -> mm) plus its QC verdicts (qc1 / qc2 in each
# observation's Meta). Duplicate exact-match transcriptions of the same
# station-year are merged (QC-aware, best value per day) into a single file, so
# the export is sharded by CONTIGUOUS matched-year range (keeping a station's
# duplicates together) -- each array task streams a disjoint set of years and
# writes its own year-partitioned .tsv files, so no merge stage is needed.
# Refresh SEF_MIN_YEAR / SEF_MAX_YEAR with the min/max matched_year in
# ensemble_metadata (safe defaults cover the Rainfall Rescue coverage span).
export SEF_OUTPUT_ROOT="${SEF_OUTPUT_ROOT:-${PDIR}/sef_export}"
export SEF_NUM_SHARDS="${SEF_NUM_SHARDS:-100}"
export SEF_MIN_YEAR="${SEF_MIN_YEAR:-1677}"
export SEF_MAX_YEAR="${SEF_MAX_YEAR:-1980}"
# SEF header provenance and the daily observation hour (UK rainfall day ends 09:00).
export SEF_SOURCE="${SEF_SOURCE:-RainfallRescue}"
export SEF_LINK="${SEF_LINK:-NA}"
export SEF_OBS_HOUR="${SEF_OBS_HOUR:-9}"

export SEF_CORES="${SEF_CORES:-1}"
export SEF_MEM_MB="${SEF_MEM_MB:-8000}"
export SEF_TIME_MIN="${SEF_TIME_MIN:-30}"

# --- SEF rainfall animation pipeline ------------------------------------
# Interpolated daily-rainfall map animation built from the exported SEF .tsv
# files ONLY (precompute -> render array -> validate -> encode). Stations that
# passed either QC check are drawn with their millimetre value; stations that
# failed BOTH checks are drawn as red error crosses. Reads SEF_OUTPUT_ROOT (build
# it first with submit_sef_export.sh). Frames are rendered in parallel across
# SEFANIM_NUM_SHARDS array tasks, then encoded once to MP4.
export SEFANIM_DIR="${SEFANIM_DIR:-${PDIR}/sef_animation}"
export SEFANIM_MANIFEST="${SEFANIM_MANIFEST:-${SEFANIM_DIR}/manifest.json}"
export SEFANIM_FRAME_DIR="${SEFANIM_FRAME_DIR:-${SEFANIM_DIR}/frames}"
export SEFANIM_SEF_ROOT="${SEFANIM_SEF_ROOT:-${SEF_OUTPUT_ROOT}}"

# Date range and interpolation density. Defaults target the 1931 test range;
# override for the full available span.
export SEFANIM_DATE_START="${SEFANIM_DATE_START:-1931-01-01}"
export SEFANIM_DATE_END="${SEFANIM_DATE_END:-1931-12-31}"
export SEFANIM_FRAMES_PER_DAY="${SEFANIM_FRAMES_PER_DAY:-6}"
export SEFANIM_FPS="${SEFANIM_FPS:-30}"
export SEFANIM_CMAP="${SEFANIM_CMAP:-YlGnBu}"
# SEF values are millimetres, so the colour scale maxes out much higher than the
# inch-scaled ensemble animation (2 in ~= 50 mm).
export SEFANIM_VMAX="${SEFANIM_VMAX:-50.0}"
export SEFANIM_MARKER_SIZE="${SEFANIM_MARKER_SIZE:-9.0}"
export SEFANIM_ERROR_COLOR="${SEFANIM_ERROR_COLOR:-#d62728}"
export SEFANIM_KEEP_FRAMES="${SEFANIM_KEEP_FRAMES:-0}"

# Parallelism: number of render array tasks.
export SEFANIM_NUM_SHARDS="${SEFANIM_NUM_SHARDS:-100}"

# Per-stage resource requests (cores via --ntasks, RAM in MB, time in minutes).
export SEFANIM_PRECOMPUTE_CORES="${SEFANIM_PRECOMPUTE_CORES:-1}"
export SEFANIM_PRECOMPUTE_MEM_MB="${SEFANIM_PRECOMPUTE_MEM_MB:-2000}"
export SEFANIM_PRECOMPUTE_TIME_MIN="${SEFANIM_PRECOMPUTE_TIME_MIN:-10}"

# Rendering a shard reads a whole SEF year into memory, so give it more RAM than
# the SQLite animation's per-day cache needed.
export SEFANIM_RENDER_CORES="${SEFANIM_RENDER_CORES:-1}"
export SEFANIM_RENDER_MEM_MB="${SEFANIM_RENDER_MEM_MB:-8000}"
export SEFANIM_RENDER_TIME_MIN="${SEFANIM_RENDER_TIME_MIN:-30}"

export SEFANIM_VALIDATE_CORES="${SEFANIM_VALIDATE_CORES:-1}"
export SEFANIM_VALIDATE_MEM_MB="${SEFANIM_VALIDATE_MEM_MB:-2000}"
export SEFANIM_VALIDATE_TIME_MIN="${SEFANIM_VALIDATE_TIME_MIN:-10}"

export SEFANIM_ENCODE_CORES="${SEFANIM_ENCODE_CORES:-16}"
export SEFANIM_ENCODE_MEM_MB="${SEFANIM_ENCODE_MEM_MB:-32000}"
export SEFANIM_ENCODE_TIME_MIN="${SEFANIM_ENCODE_TIME_MIN:-240}"

# --- SEF analysis dataset (summary figures from the SEF output only) ------
# Convert the exported SEF .tsv tree into a compact Parquet analysis set that the
# analyse_sef_output.ipynb notebook queries with DuckDB. Sharded ONE YEAR PER
# ARRAY TASK: each task parses <SEFSTATS_SEF_ROOT>/tsv/<year>/ and writes disjoint
# observations/year=<Y>.parquet + daily_national/year=<Y>.parquet, so there is NO
# merge stage. The array size is set at submit time from the number of SEF years
# (submit_sef_analysis.sh counts them). Reads the SEF export, so build it first
# with submit_sef_export.sh.
export SEFSTATS_ROOT="${SEFSTATS_ROOT:-${PDIR}/sef_analysis}"
export SEFSTATS_SEF_ROOT="${SEFSTATS_SEF_ROOT:-${SEF_OUTPUT_ROOT}}"

# Per-task resources. A task holds one year of observations in memory before
# writing; the busiest year (~9k station files) is the sizing case.
export SEFSTATS_CORES="${SEFSTATS_CORES:-1}"
export SEFSTATS_MEM_MB="${SEFSTATS_MEM_MB:-8000}"
export SEFSTATS_TIME_MIN="${SEFSTATS_TIME_MIN:-30}"

# --- ALLSHEETS residual matching (second matching pass) ------------------
# After the DATA pipeline (submit_all.sh) and its metadata assignment, every
# ensemble record WITHOUT an exact DATA match is re-run through the same matching
# algorithm against the ALLSHEETS transcriptions (the individual source sheets,
# ingested to their own Parquet store). ALLSHEETS sheets carry no coordinates, so
# a match supplies location name + year only (tagged match_type=exact_allsheets).
# Stages mirror the DATA pipeline: build residual vectors -> match array ->
# merge -> finalise (assign + combine into a new final ensemble_metadata session
# under COMPARISON_PARQUET_ROOT). Submit with submit_allsheets.sh.
export ALLSHEETS_PARQUET_ROOT="${ALLSHEETS_PARQUET_ROOT:-${PDIR}/Rainfall-Rescue/rainfall_rescue_allsheets_parquet}"
export ALLSHEETS_COMPARISON_PARQUET_ROOT="${ALLSHEETS_COMPARISON_PARQUET_ROOT:-${PDIR}/monthly_similarity_allsheets_parquet}"
export ALLSHEETS_SIMILARITY_SHARD_DIR="${ALLSHEETS_SIMILARITY_SHARD_DIR:-${PDIR}/similarity_shards_allsheets_parquet}"
# DATA metadata parquet consumed as the residual filter + combine source.
# Empty = auto-resolve the latest ensemble_metadata/session_*.parquet in
# COMPARISON_PARQUET_ROOT (produced by the DATA metadata assignment).
export ALLSHEETS_DATA_METADATA="${ALLSHEETS_DATA_METADATA:-}"
export ALLSHEETS_MATCH_TYPE="${ALLSHEETS_MATCH_TYPE:-exact_allsheets}"
# CSV used to fill lat/lon/elevation for ALLSHEETS name matches so those records
# become SEF-exportable. Empty = auto-resolve LeftOverSites.csv under the
# Rainfall Rescue root (${PDIR}/Rainfall-Rescue).
export ALLSHEETS_LEFTOVER_CSV="${ALLSHEETS_LEFTOVER_CSV:-}"

# Number of ALLSHEETS match array shards (falls back to NUM_SHARDS).
export ALLSHEETS_NUM_SHARDS="${ALLSHEETS_NUM_SHARDS:-${NUM_SHARDS}}"

# Per-stage resources. Build rebuilds ~449k candidate vectors from the ALLSHEETS
# dataset and copies the residual member values via DuckDB; match holds the
# larger ALLSHEETS candidate matrix in RAM; finalise streams every ensemble file.
export ALLSHEETS_BUILD_CORES="${ALLSHEETS_BUILD_CORES:-2}"
export ALLSHEETS_BUILD_MEM_MB="${ALLSHEETS_BUILD_MEM_MB:-16000}"
export ALLSHEETS_BUILD_TIME_MIN="${ALLSHEETS_BUILD_TIME_MIN:-120}"

export ALLSHEETS_MATCH_CORES="${ALLSHEETS_MATCH_CORES:-2}"
export ALLSHEETS_MATCH_MEM_MB="${ALLSHEETS_MATCH_MEM_MB:-6000}"
export ALLSHEETS_MATCH_TIME_MIN="${ALLSHEETS_MATCH_TIME_MIN:-60}"

export ALLSHEETS_MERGE_CORES="${ALLSHEETS_MERGE_CORES:-1}"
export ALLSHEETS_MERGE_MEM_MB="${ALLSHEETS_MERGE_MEM_MB:-8000}"
export ALLSHEETS_MERGE_TIME_MIN="${ALLSHEETS_MERGE_TIME_MIN:-40}"

export ALLSHEETS_FINAL_CORES="${ALLSHEETS_FINAL_CORES:-2}"
export ALLSHEETS_FINAL_MEM_MB="${ALLSHEETS_FINAL_MEM_MB:-16000}"
export ALLSHEETS_FINAL_TIME_MIN="${ALLSHEETS_FINAL_TIME_MIN:-60}"

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
