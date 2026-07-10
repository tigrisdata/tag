#!/bin/bash
#
# Install TAG binary for the current platform
#

set -euo pipefail

TAG_VERSION="${TAG_VERSION:-v1.9.4}"
TAG_RELEASES_URL="https://tag-releases.t3.storage.dev"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

# Detect OS and architecture
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "${ARCH}" in
    x86_64)  ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
esac

BINARY_NAME="tag-${OS}-${ARCH}"
DOWNLOAD_URL="${TAG_RELEASES_URL}/${TAG_VERSION}/${BINARY_NAME}"
DEST="${INSTALL_DIR}/tag"

echo "Installing TAG ${TAG_VERSION} (${OS}/${ARCH})..."

if ! command -v curl >/dev/null 2>&1; then
    echo "Error: curl is required but not installed"
    exit 1
fi

# Create install directory if it doesn't exist
if [ ! -d "${INSTALL_DIR}" ]; then
    echo "Creating ${INSTALL_DIR} (may require sudo)..."
    mkdir -p "${INSTALL_DIR}" 2>/dev/null || sudo mkdir -p "${INSTALL_DIR}"
fi

# Download binary to a temporary file first, then move to destination
echo "Downloading from ${DOWNLOAD_URL}..."
TMPFILE="$(mktemp)"
TMPCONFIG=""
trap 'rm -f "${TMPFILE}" "${TMPCONFIG}"' EXIT

if ! curl -fsSL "${DOWNLOAD_URL}" -o "${TMPFILE}"; then
    echo "Error: Failed to download TAG from ${DOWNLOAD_URL}"
    exit 1
fi

# Move to destination, using sudo only if needed for permissions
if ! mv "${TMPFILE}" "${DEST}" 2>/dev/null; then
    sudo mv "${TMPFILE}" "${DEST}"
fi

chmod +x "${DEST}" 2>/dev/null || sudo chmod +x "${DEST}"

# Install default configuration file
CONFIG_DIR="/etc/tag"
CONFIG_FILE="${CONFIG_DIR}/config.yaml"
# Config ships in the same versioned release directory as the binary, so it is
# always matched to the installed version and needs no GitHub access.
CONFIG_URL="${TAG_RELEASES_URL}/${TAG_VERSION}/config.yaml"

if [ ! -d "${CONFIG_DIR}" ]; then
    echo "Creating ${CONFIG_DIR} (may require sudo)..."
    mkdir -p "${CONFIG_DIR}" 2>/dev/null || sudo mkdir -p "${CONFIG_DIR}"
fi

if [ ! -f "${CONFIG_FILE}" ]; then
    echo "Installing default config to ${CONFIG_FILE}..."
    TMPCONFIG="$(mktemp)"
    if curl -fsSL "${CONFIG_URL}" -o "${TMPCONFIG}"; then
        cp "${TMPCONFIG}" "${CONFIG_FILE}" 2>/dev/null || sudo cp "${TMPCONFIG}" "${CONFIG_FILE}"
        chmod 644 "${CONFIG_FILE}" 2>/dev/null || sudo chmod 644 "${CONFIG_FILE}"
    else
        echo "Warning: no config found at ${CONFIG_URL}; skipping." \
             "TAG can also be configured entirely via environment variables."
    fi
else
    echo "Config already exists at ${CONFIG_FILE}, skipping"
fi

echo "TAG ${TAG_VERSION} installed to ${DEST}"
echo ""
echo "Quick start:"
echo "  export AWS_ACCESS_KEY_ID=your_access_key"
echo "  export AWS_SECRET_ACCESS_KEY=your_secret_key"
echo "  tag --config /etc/tag/config.yaml"
echo ""
echo "Verify with:"
echo "  tag --version"
