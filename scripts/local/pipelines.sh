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

PIPELINE_NAMES="match_metadata main transcription_qc ensemble_ingest qc daily_consensus regional_stats similarity allsheets sef_export animation sef_animation sef_analysis secondary_qc"

count_sef_years() {
    find "${SEFSTATS_SEF_ROOT}/tsv" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' | wc -l | tr -d ' '
}

pipeline_stages() {
    case "$1" in
        match_metadata)
            cat <<EOF
check_glob:${MATCH_ENSEMBLE_PARQUET_ROOT}/ensemble_files/*.parquet:missing MATCH_ENSEMBLE_PARQUET_ROOT ensemble_files; run scripts/local/submit_local.sh ensemble_ingest then scripts/local/submit_local.sh transcription_qc, or point MATCH_ENSEMBLE_PARQUET_ROOT at a valid parquet dataset
check_glob:${MATCH_ENSEMBLE_PARQUET_ROOT}/ensemble_monthly_totals/*.parquet:missing MATCH_ENSEMBLE_PARQUET_ROOT ensemble_monthly_totals; run scripts/local/submit_local.sh ensemble_ingest then scripts/local/submit_local.sh transcription_qc
check_glob:${ALLSHEETS_PARQUET_ROOT}/monthly_rainfall/*.parquet:missing ALLSHEETS monthly_rainfall parquet; rerun ALLSHEETS ingest (RR_data_ingest notebook) or set ALLSHEETS_PARQUET_ROOT to a valid ALLSHEETS parquet root
single:build_vectors.sbatch
array:match_array.sbatch:NUM_SHARDS
single:merge_shards.sbatch
single:assign_metadata.sbatch
single:build_allsheets_vectors.sbatch
array:match_allsheets_array.sbatch:ALLSHEETS_NUM_SHARDS
single:merge_allsheets_shards.sbatch
single:finalize_allsheets_metadata.sbatch
EOF
            ;;
        main)
            # Backward-compatible alias; prefer 'match_metadata'.
            pipeline_stages match_metadata
            ;;
        transcription_qc)
            cat <<EOF
check_glob:${ENSEMBLE_PARQUET_ROOT}/ensemble_files/*.parquet:missing ensemble_files parquet; run scripts/local/submit_local.sh ensemble_ingest first
check_glob:${ENSEMBLE_PARQUET_ROOT}/ensemble_daily_values/*.parquet:missing ensemble_daily_values parquet; run scripts/local/submit_local.sh ensemble_ingest first
array:transcription_qc_array.sbatch:TQC_NUM_SHARDS
single:transcription_qc_merge.sbatch
EOF
            ;;
        ensemble_ingest)
            cat <<EOF
check:${ENSEMBLE_ROOT}:missing ENSEMBLE_ROOT input tree; set ENSEMBLE_ROOT to the ensemble_transcriptions JSON root
array:ingest_ensemble_array.sbatch:ENSEMBLE_NUM_SHARDS
single:merge_ensemble_shards.sbatch
EOF
            ;;
        qc)
            cat <<EOF
check_glob:${ENSEMBLE_PARQUET_ROOT}/ensemble_files/*.parquet:missing ensemble_files parquet; run scripts/local/submit_local.sh ensemble_ingest first
check_glob:${ENSEMBLE_PARQUET_ROOT}/ensemble_daily_values/*.parquet:missing ensemble_daily_values parquet; run scripts/local/submit_local.sh ensemble_ingest first
check_glob:${COMPARISON_PARQUET_ROOT}/ensemble_metadata/session_*.parquet:missing metadata session parquet; run metadata assignment/matching first
array:qc_array.sbatch:QC_NUM_SHARDS
single:qc_merge.sbatch
EOF
            ;;
        daily_consensus)
            cat <<EOF
check_glob:${ENSEMBLE_PARQUET_ROOT}/ensemble_daily_values/*.parquet:missing ensemble_daily_values parquet; run scripts/local/submit_local.sh ensemble_ingest first
array:daily_consensus_array.sbatch:CONSENSUS_NUM_SHARDS
single:daily_consensus_merge.sbatch
EOF
            ;;
        regional_stats)
            cat <<EOF
check:${CONSENSUS_ROOT}/daily_consensus/daily_consensus.parquet:build it first with: scripts/local/submit_local.sh daily_consensus
check_glob:${PDIR}/qc_parquet/daily_qc_status/*.parquet:missing qc_parquet daily_qc_status; build it first with: scripts/local/submit_local.sh qc
check_glob:${COMPARISON_PARQUET_ROOT}/ensemble_metadata/session_*.parquet:missing metadata session parquet; run metadata assignment/matching first
array:regional_stats_array.sbatch:REGIONAL_NUM_SHARDS
single:regional_stats_merge.sbatch
EOF
            ;;
        similarity)
            cat <<EOF
check_glob:${MATCH_ENSEMBLE_PARQUET_ROOT}/ensemble_files/*.parquet:missing MATCH_ENSEMBLE_PARQUET_ROOT ensemble_files; run scripts/local/submit_local.sh ensemble_ingest then scripts/local/submit_local.sh transcription_qc
check_glob:${MATCH_ENSEMBLE_PARQUET_ROOT}/ensemble_monthly_totals/*.parquet:missing MATCH_ENSEMBLE_PARQUET_ROOT ensemble_monthly_totals; run scripts/local/submit_local.sh ensemble_ingest then scripts/local/submit_local.sh transcription_qc
single:build_vectors.sbatch
array:match_array.sbatch:NUM_SHARDS
single:merge_shards.sbatch
EOF
            ;;
        allsheets)
            cat <<EOF
check_glob:${ALLSHEETS_PARQUET_ROOT}/monthly_rainfall/*.parquet:missing ALLSHEETS monthly_rainfall parquet; rerun ALLSHEETS ingest (RR_data_ingest notebook) or set ALLSHEETS_PARQUET_ROOT to a valid ALLSHEETS parquet root
check_glob:${COMPARISON_PARQUET_ROOT}/ensemble_metadata/session_*.parquet:missing DATA metadata session under COMPARISON_PARQUET_ROOT; create one first before allsheets residual pass
single:build_allsheets_vectors.sbatch
array:match_allsheets_array.sbatch:ALLSHEETS_NUM_SHARDS
single:merge_allsheets_shards.sbatch
single:finalize_allsheets_metadata.sbatch
EOF
            ;;
        sef_export)
            cat <<EOF
check:${CONSENSUS_ROOT}/daily_consensus/daily_consensus.parquet:build it first with: scripts/local/submit_local.sh daily_consensus
check_glob:${COMPARISON_PARQUET_ROOT}/ensemble_metadata/session_*.parquet:missing metadata session parquet; run metadata assignment/matching first
array:sef_export_array.sbatch:SEF_NUM_SHARDS
EOF
            ;;
        animation)
            cat <<EOF
check:${ENSEMBLE_DB}:missing ensemble SQLite DB; build it first with: scripts/local/submit_local.sh ensemble_ingest
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
check_glob:${PDIR}/regional_stats_parquet/regional_daily_stats/*.parquet:missing regional stats parquet; build it first with: scripts/local/submit_local.sh regional_stats
check_glob:${PDIR}/qc_parquet/daily_qc_status/*.parquet:missing qc_parquet daily_qc_status; build it first with: scripts/local/submit_local.sh qc
single:secondary_qc_train.sbatch
check_glob:${SECONDARY_QC_ROOT}/models/train_*/metadata.json:missing trained secondary-QC models; training stage did not produce model artifacts
single:secondary_qc_score.sbatch
EOF
            ;;
        *)
            return 1
            ;;
    esac
}
