#!/bin/bash
# Setup script for running kickoff.py inside a K8s pod.
# Installs Python3, pip, venv, and dependencies.
#
# Usage:
#   chmod +x setup.sh && ./setup.sh

set -euo pipefail

echo "=== Installing Python3 + venv ==="

# Detect package manager
if command -v apk &>/dev/null; then
    # Alpine (most likely — connector uses Dockerfile.alpine)
    apk add --no-cache python3 py3-pip py3-virtualenv
elif command -v apt-get &>/dev/null; then
    # Debian/Ubuntu
    apt-get update && apt-get install -y python3 python3-pip python3-venv
elif command -v dnf &>/dev/null; then
    # Fedora/RHEL
    dnf install -y python3 python3-pip python3-virtualenv
else
    echo "ERROR: Unknown package manager. Install python3 + pip manually."
    exit 1
fi

echo ""
echo "=== Creating virtualenv ==="
python3 -m venv .venv
source .venv/bin/activate

echo ""
echo "=== Installing Python dependencies ==="
pip install --no-cache-dir grpcio protobuf requests nats-py

# Install proto_py package in editable mode
pip install --no-cache-dir -e ./proto_py

echo ""
echo "=== Setup complete ==="
echo "To activate the venv:  source .venv/bin/activate"
echo "To run kickoff:        python kickoff.py --help"
