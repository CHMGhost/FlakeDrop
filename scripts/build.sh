#!/bin/bash

# Build script for FlakeDrop
# This script builds executables for multiple platforms

set -e

echo "Building FlakeDrop executables..."

# Create build directory
mkdir -p build

# Get version from git tag or use development
VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Build flags for smaller binaries
LDFLAGS="-s -w -X main.Version=${VERSION} -X main.BuildTime=${BUILD_TIME}"

# Build for different platforms
platforms=(
    "darwin/amd64"
    "darwin/arm64"
    "linux/amd64"
    "linux/arm64"
    "windows/amd64"
)

for platform in "${platforms[@]}"; do
    IFS='/' read -r -a parts <<< "$platform"
    GOOS="${parts[0]}"
    GOARCH="${parts[1]}"
    
    output_name="flakedrop-${GOOS}-${GOARCH}"
    if [ "$GOOS" = "windows" ]; then
        output_name="${output_name}.exe"
    fi
    
    echo "Building $output_name..."
    GOOS="$GOOS" GOARCH="$GOARCH" go build -ldflags="$LDFLAGS" -o "build/${output_name}" main.go
done

# Create universal macOS binary
if command -v lipo &> /dev/null; then
    echo "Creating universal macOS binary..."
    lipo -create -output build/flakedrop-darwin-universal \
        build/flakedrop-darwin-amd64 \
        build/flakedrop-darwin-arm64
fi

# Create archives
cd build
echo "Creating archives..."

# macOS
tar -czf flakedrop-darwin-universal.tar.gz flakedrop-darwin-universal 2>/dev/null || \
tar -czf flakedrop-darwin-amd64.tar.gz flakedrop-darwin-amd64

# Linux
tar -czf flakedrop-linux-amd64.tar.gz flakedrop-linux-amd64
tar -czf flakedrop-linux-arm64.tar.gz flakedrop-linux-arm64

# Windows
zip flakedrop-windows-amd64.zip flakedrop-windows-amd64.exe

# Generate checksums
echo "Generating checksums..."
shasum -a 256 *.tar.gz *.zip > checksums.txt

cd ..

echo "Build complete! Files are in the build/ directory."
echo "Version: $VERSION"
ls -lh build/