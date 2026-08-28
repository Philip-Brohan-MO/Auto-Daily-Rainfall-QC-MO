#!/bin/bash
# Deprecated compatibility shim. Use verify_match_metadata_manifest.py instead.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "WARNING: scripts/local/verify_main_manifest.py is deprecated; use scripts/local/verify_match_metadata_manifest.py" >&2
exec python3 "${SCRIPT_DIR}/verify_match_metadata_manifest.py" "$@"
