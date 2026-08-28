#!/bin/bash
# Deprecated compatibility shim. Use clean_match_metadata_outputs.sh instead.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "WARNING: scripts/local/clean_outputs.sh is deprecated; use scripts/local/clean_match_metadata_outputs.sh" >&2
exec "${SCRIPT_DIR}/clean_match_metadata_outputs.sh" "$@"
