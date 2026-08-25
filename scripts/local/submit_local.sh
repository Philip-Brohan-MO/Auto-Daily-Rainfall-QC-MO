#!/bin/bash
# Generic local launcher for any registered pipeline (see pipelines.sh).
#
# Local (non-SLURM) equivalent of the scripts/slurm/submit_*.sh drivers: runs
# the same array/merge/build/finalize stages, in the same order, directly on
# this machine -- array stages fan out across LOCAL_WORKERS concurrent
# processes (see run_array_local.sh) instead of a SLURM array job.
#
# Usage:
#   scripts/local/submit_local.sh <pipeline-name>
#   scripts/local/submit_local.sh                      # lists registered pipelines
#   TQC_NUM_SHARDS=8 scripts/local/submit_local.sh transcription_qc
#   LOCAL_WORKERS=12 scripts/local/submit_local.sh regional_stats
#
# Requires: PDIR set, ADRQ conda env installed (see docs/installation.md).

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SLURM_DIR="${SCRIPT_DIR}/../slurm"
source "${SLURM_DIR}/config.sh"
source "${SCRIPT_DIR}/config_local.sh"
source "${SCRIPT_DIR}/pipelines.sh"

PIPELINE="${1:-}"
STAGES="$(pipeline_stages "${PIPELINE}" 2>/dev/null)"
if [[ -z "${PIPELINE}" || -z "${STAGES}" ]]; then
    echo "Usage: $0 <pipeline-name>" >&2
    echo "Registered pipelines:" >&2
    for name in ${PIPELINE_NAMES}; do echo "  ${name}"; done >&2
    exit 1
fi

mkdir -p "${LOCAL_LOG_DIR}"

while IFS= read -r stage; do
    [[ -z "${stage}" ]] && continue
    kind="${stage%%:*}"
    rest="${stage#*:}"
    case "${kind}" in
        single)
            echo ">>> [${PIPELINE}] single stage: ${rest}"
            bash "${SLURM_DIR}/${rest}" || { echo "ERROR: stage ${rest} failed" >&2; exit 1; }
            ;;
        array)
            file="${rest%%:*}"
            spec="${rest#*:}"
            if [[ "${spec}" == fn:* ]]; then
                num_shards="$("${spec#fn:}")"
            else
                num_shards="${!spec}"
            fi
            echo ">>> [${PIPELINE}] array stage: ${file} (${num_shards} shards)"
            "${SCRIPT_DIR}/run_array_local.sh" "${SLURM_DIR}/${file}" "${num_shards}" \
                || { echo "ERROR: array stage ${file} failed" >&2; exit 1; }
            ;;
        check)
            path="${rest%%:*}"
            hint="${rest#*:}"
            if [[ ! -e "${path}" ]]; then
                echo "ERROR: required input not found: ${path}" >&2
                echo "  ${hint}" >&2
                exit 1
            fi
            ;;
        *)
            echo "ERROR: unknown stage kind '${kind}' in pipeline '${PIPELINE}'" >&2
            exit 1
            ;;
    esac
done <<< "${STAGES}"

echo "Pipeline '${PIPELINE}' completed."
