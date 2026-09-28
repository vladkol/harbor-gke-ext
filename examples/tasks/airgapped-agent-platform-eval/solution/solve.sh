#!/bin/bash
set -euo pipefail

mkdir -p /logs/artifacts

# Attempt to reach github.com and record output
curl -I https://github.com --connect-timeout 5 > /logs/artifacts/github_test.txt 2>&1 || true

# Write reversed word OLLEH
echo "OLLEH" > /logs/artifacts/answer.txt
