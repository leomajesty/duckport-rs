#!/bin/bash
# Build and package airport for all platforms: macOS, Linux x86_64, Linux ARM64

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

"$SCRIPT_DIR/package-macos.sh"
"$SCRIPT_DIR/package-linux.sh"
