#!/bin/bash
# Registry of local pipeline stage sequences for scripts/local/submit_local.sh.
#
# Written as a case-based function (not an associative array) because macOS
# ships bash 3.2, which has no `declare -A`.
#
# Must be sourced AFTER scripts/slurm/config.sh and scripts/local/config_local.sh
# so the config variables referenced in stage tokens below are already set.
#
# Each pipeline maps to a newline-separated list of stage tokens:
#   single:<sbatch_file>              -- run once, directly, via bash
#   array:<sbatch_file>:<VAR_NAME>    -- run via run_array_local.sh; shard count
#                                         comes from the named config.sh var
#   array:<sbatch_file>:fn:<func>     -- shard count comes from calling <func>
#   check:<path>:<hint>               -- fail with <hint> if <path> is missing
#
# Adding local support for a new pipeline added on main is one case arm here --
# no new script to write or keep in sync.

PIPELINE_NAMES="transcription_qc ensemble_ingest qc daily_consensus regional_stats similarity allsheets sef_export animation sef_animation sef_analysis secondary_qc"

count_sef_years() {
    find "${SEFSTATS_SEF_ROOT}/tsv" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' | wc -l | tr -d ' '
}

pipeline_stages() {
    case "$1" in
        transcription_qc)
            cat <<EOF
array:transcription_qc_array.sbatch:TQC_NUM_SHARDS
single:transcription_qc_merge.sbatch
EOF
            ;;
        ensemble_ingest)
            cat <<EOF
array:ingest_ensemble_array.sbatch:ENSEMBLE_NUM_SHARDS
single:merge_ensemble_shards.sbatch
EOF
            ;;
        qc)
            cat <<EOF
array:qc_array.sbatch:QC_NUM_SHARDS
single:qc_merge.sbatch
EOF
            ;;
        daily_consensus)
            cat <<EOF
array:daily_consensus_array.sbatch:CONSENSUS_NUM_SHARDS
single:daily_consensus_merge.sbatch
EOF
            ;;
        regional_stats)
            cat <<EOF
check:${CONSENSUS_ROOT}/daily_consensus/daily_consensus.parquet:build it first with: scripts/local/submit_local.sh daily_consensus
array:regional_stats_array.sbatch:REGIONAL_NUM_SHARDS
single:regional_stats_merge.sbatch
EOF
            ;;
        similarity)
            cat <<EOF
single:build_vectors.sbatch
array:match_array.sbatch:NUM_SHARDS
single:merge_shards.sbatch
EOF
            ;;
        allsheets)
            cat <<EOF
single:build_allsheets_vectors.sbatch
array:match_allsheets_array.sbatch:ALLSHEETS_NUM_SHARDS
single:merge_allsheets_shards.sbatch
single:finalize_allsheets_metadata.sbatch
EOF
            ;;
        sef_export)
            cat <<EOF
check:${CONSENSUS_ROOT}/daily_consensus/daily_consensus.parquet:build it first with: scripts/local/submit_local.sh daily_consensus
array:sef_export_array.sbatch:SEF_NUM_SHARDS
EOF
            ;;
        animation)
            cat <<EOF
single:render_precompute.sbatch
array:render_array.sbatch:RENDER_NUM_SHARDS
single:render_validate.sbatch
single:render_encode.sbatch
EOF
            ;;
        sef_animation)
            cat <<EOF
check:${SEFANIM_SEF_ROOT}/tsv:build it first with: scripts/local/submit_local.sh sef_export
single:sef_render_precompute.sbatch
array:sef_render_array.sbatch:SEFANIM_NUM_SHARDS
single:sef_render_validate.sbatch
single:sef_render_encode.sbatch
EOF
            ;;
        sef_analysis)
            cat <<EOF
check:${SEFSTATS_SEF_ROOT}/tsv:build it first with: scripts/local/submit_local.sh sef_export
array:sef_analysis_array.sbatch:fn:count_sef_years
EOF
            ;;
        secondary_qc)
            cat <<EOF
single:secondary_qc_train.sbatch
single:secondary_qc_score.sbatch
EOF
            ;;
        *)
            return 1
            ;;
    esac
}
