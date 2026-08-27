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
#   scripts/local/submit_local.sh match_metadata       # one-command metadata matching flow
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
                num_shards="$(${spec#fn:})"
            else
                num_shards="${!spec}"
            fi
            case "${file}" in
                transcription_qc_array.sbatch)
                    rm -f "${TQC_SHARD_DIR:-}"/tqc_shard_*.parquet 2>/dev/null || true
                    ;;
                qc_array.sbatch)
                    rm -f "${QC_SHARD_DIR:-}"/qc_shard_*.parquet 2>/dev/null || true
                    ;;
                daily_consensus_array.sbatch)
                    rm -f "${CONSENSUS_SHARD_DIR:-}"/consensus_shard_*.parquet 2>/dev/null || true
                    ;;
                regional_stats_array.sbatch)
                    rm -f "${REGIONAL_SHARD_DIR:-}"/regional_stats_*.parquet 2>/dev/null || true
                    ;;
                similarity_array.sbatch)
                    rm -f "${SIMILARITY_SHARD_DIR:-}"/similarity_shard_*.parquet 2>/dev/null || true
                    ;;
                ensemble_ingest_array.sbatch)
                    rm -f "${ENSEMBLE_SHARD_DIR:-}"/ensemble_shard_*.parquet 2>/dev/null || true
                    ;;
            esac
            echo ">>> [${PIPELINE}] array stage: ${file} (${num_shards} shards)"
            "${SCRIPT_DIR}/run_array_local.sh" "${SLURM_DIR}/${file}" "${num_shards}" \
                || { echo "ERROR: array stage ${file} failed" >&2; exit 1; }
            ;;
        check)
            path="${rest%%:*}"
            hint="${rest#*:}"
            if [[ ! -e "${path}" ]]; then
                echo "ERROR: missing required input for pipeline '${PIPELINE}': ${path}" >&2
                echo "HINT: ${hint}" >&2
                exit 1
            fi
            ;;
        check_glob)
            pattern="${rest%%:*}"
            hint="${rest#*:}"
            if ! compgen -G "${pattern}" > /dev/null; then
                echo "ERROR: missing required input files for pipeline '${PIPELINE}': ${pattern}" >&2
                echo "HINT: ${hint}" >&2
                exit 1
            fi
            ;;
        *)
            echo "ERROR: unknown stage kind '${kind}' in pipeline '${PIPELINE}'" >&2
            exit 1
            ;;
    esac
done <<< "${STAGES}"

if [[ "${PIPELINE}" == "main" ]]; then
    echo "WARNING: pipeline name 'main' is deprecated; use 'match_metadata'" >&2
fi

if [[ "${PIPELINE}" == "match_metadata" || "${PIPELINE}" == "main" ]]; then
    echo ">>> [${PIPELINE}] validating published manifest"
    run_py scripts/local/verify_match_metadata_manifest.py \
        --comparison-root "${COMPARISON_PARQUET_ROOT}" \
        || { echo "ERROR: match_metadata manifest validation failed" >&2; exit 1; }
fi

echo "Pipeline '${PIPELINE}' completed."
