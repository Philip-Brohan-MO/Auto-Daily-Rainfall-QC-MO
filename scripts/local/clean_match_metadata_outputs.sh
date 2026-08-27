#!/bin/bash
# Remove match-metadata pipeline artifacts under PDIR so the next run starts clean.
#
# Default mode is dry-run (prints what would be removed).
# Use --apply to actually delete.
#
# Usage:
#   scripts/local/clean_match_metadata_outputs.sh
#   scripts/local/clean_match_metadata_outputs.sh --apply
#   scripts/local/clean_match_metadata_outputs.sh --apply --yes
#   scripts/local/clean_match_metadata_outputs.sh --apply --yes --include-inputs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SLURM_DIR="${SCRIPT_DIR}/../slurm"

source "${SLURM_DIR}/config.sh"
source "${SCRIPT_DIR}/config_local.sh"

if [[ -z "${PDIR:-}" ]]; then
    echo "ERROR: PDIR is not set." >&2
    exit 1
fi

APPLY=0
ASSUME_YES=0
INCLUDE_INPUTS=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply)
            APPLY=1
            shift
            ;;
        --yes)
            ASSUME_YES=1
            shift
            ;;
        --include-inputs)
            INCLUDE_INPUTS=1
            shift
            ;;
        -h|--help)
            sed -n '1,28p' "$0"
            exit 0
            ;;
        *)
            echo "ERROR: Unknown argument: $1" >&2
            echo "Use --help for usage." >&2
            exit 1
            ;;
    esac
done

# Directories/files to reset for a clean local rerun.
TARGETS=(
    "${COMPARISON_DB}"
    "${SHARD_DIR}"
    "${COMPARISON_PARQUET_ROOT}"
    "${SIMILARITY_SHARD_DIR}"
    "${SLURM_LOG_DIR}"
    "${LOCAL_LOG_DIR}"

    "${ALLSHEETS_COMPARISON_PARQUET_ROOT}"
    "${ALLSHEETS_SIMILARITY_SHARD_DIR}"
)

# Optional destructive mode: also remove upstream input datasets.
if [[ "$INCLUDE_INPUTS" -eq 1 ]]; then
    TARGETS+=(
        "${ENSEMBLE_DB}"
        "${ENSEMBLE_SHARD_DIR}"
        "${ENSEMBLE_PARQUET_ROOT}"
        "${QC_SHARD_DIR}"
        "${TQC_ROOT}"
        "${TQC_SHARD_DIR}"
        "${TQC_GOOD_ROOT}"
        "${CONSENSUS_SHARD_DIR}"
        "${CONSENSUS_ROOT}"
        "${REGIONAL_SHARD_DIR}"
        "${SECONDARY_QC_ROOT}"
        "${SEF_OUTPUT_ROOT}"
        "${SEFANIM_DIR}"
        "${SEFSTATS_ROOT}"
        "${ANIMATION_DIR}"
    )
fi

# Deduplicate while preserving order (bash 3.2 compatible: no associative arrays).
UNIQ_TARGETS=()
for t in "${TARGETS[@]}"; do
    [[ -n "$t" ]] || continue
    seen=0
    for u in ${UNIQ_TARGETS[@]+"${UNIQ_TARGETS[@]}"}; do
        if [[ "$u" == "$t" ]]; then
            seen=1
            break
        fi
    done
    if [[ "$seen" -eq 0 ]]; then
        UNIQ_TARGETS+=("$t")
    fi
done

is_safe_target() {
    local path="$1"
    [[ -n "$path" ]] || return 1
    [[ "$path" != "/" ]] || return 1
    [[ "$path" != "$PDIR" ]] || return 1
    [[ "$path" == "$PDIR"/* ]] || return 1
    return 0
}

echo "PDIR: ${PDIR}"
if [[ "$INCLUDE_INPUTS" -eq 0 ]]; then
    echo "Mode: outputs only (input datasets preserved)"
else
    echo "Mode: outputs + inputs (destructive)"
fi
echo ""
echo "Configured cleanup targets:"
for t in "${UNIQ_TARGETS[@]}"; do
    if is_safe_target "$t"; then
        if [[ -e "$t" ]]; then
            echo "  [exists]  $t"
        else
            echo "  [missing] $t"
        fi
    else
        echo "  [SKIP-UNSAFE] $t"
    fi
done

if [[ "$APPLY" -eq 0 ]]; then
    echo ""
    echo "Dry-run complete. Re-run with --apply to delete existing targets." 
    exit 0
fi

if [[ "$ASSUME_YES" -ne 1 ]]; then
    echo ""
    read -r -p "Delete ALL existing safe targets listed above? [y/N] " answer
    case "$answer" in
        y|Y|yes|YES)
            ;;
        *)
            echo "Cancelled."
            exit 1
            ;;
    esac
fi

DELETED=0
for t in "${UNIQ_TARGETS[@]}"; do
    if ! is_safe_target "$t"; then
        continue
    fi
    if [[ -d "$t" ]]; then
        rm -rf "$t"
        echo "deleted dir  $t"
        DELETED=$((DELETED + 1))
    elif [[ -f "$t" ]]; then
        rm -f "$t"
        echo "deleted file $t"
        DELETED=$((DELETED + 1))
    fi
done

echo ""
echo "Cleanup complete. Deleted ${DELETED} target(s)."
