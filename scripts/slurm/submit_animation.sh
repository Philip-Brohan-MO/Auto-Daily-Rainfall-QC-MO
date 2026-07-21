#!/bin/bash
# Submit the interpolated rainfall animation pipeline as four dependent stages:
#   1. render_precompute  (single job)  -> writes the frame manifest
#   2. render_array       (RENDER_NUM_SHARDS array tasks) -> renders frames
#   3. render_validate    (single job)  -> checks every frame exists
#   4. render_encode      (single job)  -> ffmpeg frames -> MP4
#
# Each stage starts only after the previous stage succeeds (afterok).
#
# Usage:
#   scripts/slurm/submit_animation.sh
#   RENDER_DATE_START=1931-01-01 RENDER_DATE_END=1931-12-31 \
#       RENDER_NUM_SHARDS=200 scripts/slurm/submit_animation.sh
#   scripts/slurm/submit_animation.sh --skip-precompute  # reuse manifest
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

mkdir -p "${ANIMATION_DIR}" "${RENDER_FRAME_DIR}" "${SLURM_LOG_DIR}"

# Forward the animation config to every stage so the values chosen here are
# honoured inside each job. RQC_SLURM_DIR lets the jobs locate config.sh.
EXPORTS="ALL,RQC_SLURM_DIR=${SCRIPT_DIR},PDIR=${PDIR}"
EXPORTS="${EXPORTS},ANIMATION_DIR=${ANIMATION_DIR},RENDER_MANIFEST=${RENDER_MANIFEST}"
EXPORTS="${EXPORTS},RENDER_FRAME_DIR=${RENDER_FRAME_DIR},ENSEMBLE_DB=${ENSEMBLE_DB}"
EXPORTS="${EXPORTS},RENDER_DATE_START=${RENDER_DATE_START},RENDER_DATE_END=${RENDER_DATE_END}"
EXPORTS="${EXPORTS},RENDER_FRAMES_PER_DAY=${RENDER_FRAMES_PER_DAY},RENDER_FPS=${RENDER_FPS}"
EXPORTS="${EXPORTS},RENDER_CMAP=${RENDER_CMAP},RENDER_VMAX=${RENDER_VMAX}"
EXPORTS="${EXPORTS},RENDER_MARKER_SIZE=${RENDER_MARKER_SIZE},RENDER_NUM_SHARDS=${RENDER_NUM_SHARDS}"
EXPORTS="${EXPORTS},RENDER_KEEP_FRAMES=${RENDER_KEEP_FRAMES}"

ARRAY_MAX=$(( RENDER_NUM_SHARDS - 1 ))

# Resource requests per stage.
PRECOMPUTE_RES="--qos=${SLURM_QOS} --ntasks=${RPRECOMPUTE_CORES} --ntasks-per-core=1 --mem=${RPRECOMPUTE_MEM_MB} --time=${RPRECOMPUTE_TIME_MIN}"
RENDER_RES="--qos=${SLURM_QOS} --ntasks=${RRENDER_CORES} --ntasks-per-core=1 --mem=${RRENDER_MEM_MB} --time=${RRENDER_TIME_MIN}"
VALIDATE_RES="--qos=${SLURM_QOS} --ntasks=${RVALIDATE_CORES} --ntasks-per-core=1 --mem=${RVALIDATE_MEM_MB} --time=${RVALIDATE_TIME_MIN}"
ENCODE_RES="--qos=${SLURM_QOS} --ntasks=1 --cpus-per-task=${RENCODE_CORES} --mem=${RENCODE_MEM_MB} --time=${RENCODE_TIME_MIN}"

PRECOMPUTE_DEP=""
if [[ "${SKIP_PRECOMPUTE}" -eq 0 ]]; then
    PRECOMPUTE_ID=$(sbatch --parsable \
        ${PRECOMPUTE_RES} \
        --chdir="${SLURM_LOG_DIR}" \
        --export="${EXPORTS}" \
        "${SCRIPT_DIR}/render_precompute.sbatch")
    echo "Submitted precompute job: ${PRECOMPUTE_ID}"
    PRECOMPUTE_DEP="--dependency=afterok:${PRECOMPUTE_ID}"
else
    echo "Skipping precompute; reusing ${RENDER_MANIFEST}"
fi

ARRAY_ID=$(sbatch --parsable \
    ${PRECOMPUTE_DEP} \
    ${RENDER_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    --array="0-${ARRAY_MAX}" \
    "${SCRIPT_DIR}/render_array.sbatch")
echo "Submitted render array: ${ARRAY_ID} (0-${ARRAY_MAX})"

VALIDATE_ID=$(sbatch --parsable \
    --dependency=afterok:${ARRAY_ID} \
    ${VALIDATE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/render_validate.sbatch")
echo "Submitted validate job: ${VALIDATE_ID}"

ENCODE_ID=$(sbatch --parsable \
    --dependency=afterok:${VALIDATE_ID} \
    ${ENCODE_RES} \
    --chdir="${SLURM_LOG_DIR}" \
    --export="${EXPORTS}" \
    "${SCRIPT_DIR}/render_encode.sbatch")
echo "Submitted encode job: ${ENCODE_ID}"

echo
echo "Animation pipeline submitted. Track with:  squeue -u \$USER"
echo "Final video lands in: ${ANIMATION_DIR}/ (see manifest output_path)"
