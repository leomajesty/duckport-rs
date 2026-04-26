#!/bin/bash
# Build and package airport for Linux (x86_64 and ARM64)
# Requires: cargo-zigbuild, zig, musl-cross (brew install filosottile/musl-cross/musl-cross)
# The .cargo/config.toml is already configured with the correct ar/linker for zigbuild.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PKG_DIR="$PROJECT_DIR/packages"

echo "=== Building airport for Linux ==="

cd "$PROJECT_DIR"

# Build for Linux x86_64
echo "--- Building x86_64 ---"
cargo build --release --target x86_64-unknown-linux-gnu --bin airport
mkdir -p "$PKG_DIR"
cp "$PROJECT_DIR/target/x86_64-unknown-linux-gnu/release/airport" "$PKG_DIR/airport-x86_64"

# Build for Linux ARM64
echo "--- Building ARM64 ---"
cargo build --release --target aarch64-unknown-linux-gnu --bin airport
cp "$PROJECT_DIR/target/aarch64-unknown-linux-gnu/release/airport" "$PKG_DIR/airport-aarch64"

# Create tarballs
echo "--- Creating tarballs ---"
tar -C "$PKG_DIR" -czf "$PKG_DIR/airport-x86_64.tar.gz" airport-x86_64
tar -C "$PKG_DIR" -czf "$PKG_DIR/airport-aarch64.tar.gz" airport-aarch64

echo "=== Done ==="
echo "Packages:"
ls -lh "$PKG_DIR"/*.tar.gz "$PKG_DIR"/airport-{x86_64,aarch64,macos} 2>/dev/null || true
