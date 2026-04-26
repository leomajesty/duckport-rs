#!/bin/bash
# Build and package airport for macOS (Apple Silicon)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PKG_DIR="$PROJECT_DIR/packages"
BIN_NAME="airport-macos"

echo "=== Building airport for macOS ==="

cd "$PROJECT_DIR"

# Build for macOS ARM64
cargo build --release --bin airport

# Copy binary
mkdir -p "$PKG_DIR"
cp "$PROJECT_DIR/target/release/airport" "$PKG_DIR/$BIN_NAME"

echo "=== Done ==="
echo "Binary: $PKG_DIR/$BIN_NAME"
echo "Size: $(ls -lh "$PKG_DIR/$BIN_NAME" | awk '{print $5}')"
