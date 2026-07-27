#!/bin/bash
# Submit the SEF rainfall animation pipeline as four dependent stages:
#   1. sef_render_precompute  (single job)  -> writes the frame manifest
#   2. sef_render_array       (SEFANIM_NUM_SHARDS array tasks) -> renders frames
#   3. sef_render_validate    (single job)  -> checks every frame exists
#   4. sef_render_encode      (single job)  -> ffmpeg frames -> MP4
#
# Each stage starts only after the previous stage succeeds (afterok).
#
# The animation is built from the exported SEF .tsv files ONLY: stations that
# passed either QC check are drawn with their millimetre value; stations that
# failed both checks are drawn as red error crosses. Build the SEF files first
# with submit_sef_export.sh.
#
# Usage:
#   scripts/slurm/submit_sef_animation.sh
#   SEFANIM_DATE_START=1931-01-01 SEFANIM_DATE_END=1931-12-31 \
#       SEFANIM_NUM_SHARDS=200 scripts/slurm/submit_sef_animation.sh
#   scripts/slurm/submit_sef_animation.sh --skip-precompute  # reuse manifest
#
# Requires: sbatch on PATH; ffmpeg available in the conda env for the encode stage.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

SKIP_PRECOMPUTE=0
for arg in "$@"; do
    case "${arg}" in
        --skip-precompute) SKIP_PRECOMPUTE=1 ;;
        *) echo "Unknown option: ${arg}" >&2; exit 2 ;;
    esac
done

# Guard: the SEF export must exist before we can animate it.
if [[ ! -d "${SEFANIM_SEF_ROOT}/tsv" ]]; then
    echo "ERROR: no SEF files under ${SEFANIM_SEF_ROOT}/tsv" >&2
    echo "       run scripts/slurm/submit_sef_export.sh first." >&2
    exit 1
fi

mkdir -p "${SEFANIM_DIR}" "${SEFANIM_FRAME_DIR}" "${SLURM_LOG_DIR}"

# Forward the animation config to every stage so the values chosen here are
# honoured inside each job. RQC_SLURM_DIR lets the jobs locate config.sh.
EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR},PDIR=${PDIR}"
EXPORTS="${EXPORTS},SEFANIM_DIR=${SEFANIM_DIR},SEFANIM_MANIFEST=${SEFANIM_MANIFEST}"
EXPORTS="${EXPORTS},SEFANIM_FRAME_DIR=${SEFANIM_FRAME_DIR},SEFANIM_SEF_ROOT=${SEFANIM_SEF_ROOT}"
EXPORTS="${EXPORTS},SEFANIM_DATE_START=${SEFANIM_DATE_START},SEFANIM_DATE_END=${SEFANIM_DATE_END}"
EXPORTS="${EXPORTS},SEFANIM_FRAMES_PER_DAY=${SEFANIM_FRAMES_PER_DAY},SEFANIM_FPS=${SEFANIM_FPS}"
EXPORTS="${EXPORTS},SEFANIM_CMAP=${SEFANIM_CMAP},SEFANIM_VMAX=${SEFANIM_VMAX}"
EXPORTS="${EXPORTS},SEFANIM_MARKER_SIZE=${SEFANIM_MARKER_SIZE},SEFANIM_ERROR_COLOR=${SEFANIM_ERROR_COLOR}"
EXPORTS="${EXPORTS},SEFANIM_NUM_SHARDS=${SEFANIM_NUM_SHARDS},SEFANIM_KEEP_FRAMES=${SEFANIM_KEEP_FRAMES}"

ARRAY_MAX=$(( SEFANIM_NUM_SHARDS - 1 ))

# Resource requests per stage.
PRECOMPUTE_RES="--qos=${SLURM_QOS} --ntasks=${SEFANIM_PRECOMPUTE_CORES} --ntasks-per-core=1 --mem=${SEFANIM_PRECOMPUTE_MEM_MB} --time=${SEFANIM_PRECOMPUTE_TIME_MIN}"
RENDER_RES="--qos=${SLURM_QOS} --ntasks=${SEFANIM_RENDER_CORES} --ntasks-per-core=1 --mem=${SEFANIM_RENDER_MEM_MB} --time=${SEFANIM_RENDER_TIME_MIN}"
VALIDATE_RES="--qos=${SLURM_QOS} --ntasks=${SEFANIM_VALIDATE_CORES} --ntasks-per-core=1 --mem=${SEFANIM_VALIDATE_MEM_MB} --time=${SEFANIM_VALIDATE_TIME_MIN}"
ENCODE_RES="--qos=${SLURM_QOS} --ntasks=1 --cpus-per-task=${SEFANIM_ENCODE_CORES} --mem=${SEFANIM_ENCODE_MEM_MB} --time=${SEFANIM_ENCODE_TIME_MIN}"

PRECOMPUTE_DEP=""
if [[ "${SKIP_PRECOMPUTE}" -eq 0 ]]; then
    PRECOMPUTE_ID=$(sbatch --parsable \
        ${PRECOMPUTE_RES} \
        --chdir="${SLURM_LOG_DIR}" \
        --export="${EXPORTS}" \
        "${SCRIPT_DIR}/sef_render_precompute.sbatch")
    echo "Submitted precompute job: ${PRECOMPUTE_ID}"
    PRECOMPUTE_DEP="--dependency=afterok:${PRECOMPUTE_ID}"
else
    echo "Skipping precompute; reusing ${SEFANIM_MANIFEST}"
fi

ARRAY_ID=$(sbatch --parsable \
    ${PRECOMPUTE_DEP} \
    ${RENDER_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/sef_render_array.sbatch")
echo "Submitted render array: ${ARRAY_ID} (0-${ARRAY_MAX})"

VALIDATE_ID=$(sbatch --parsable \
    --dependency=afterok:${ARRAY_ID} \
    ${VALIDATE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/sef_render_validate.sbatch")
echo "Submitted validate job: ${VALIDATE_ID}"

ENCODE_ID=$(sbatch --parsable \
    --dependency=afterok:${VALIDATE_ID} \
    ${ENCODE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/sef_render_encode.sbatch")
echo "Submitted encode job: ${ENCODE_ID}"

echo
echo "SEF animation pipeline submitted. Track with:  squeue -u \$USER"
echo "Final video lands in: ${SEFANIM_DIR}/ (see manifest output_path)"
