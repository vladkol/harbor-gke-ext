#!/bin/bash
set -u

mkdir -p /logs/verifier
reward=1

fail() {
  echo "VERIFIER FAILURE: $1"
  reward=0
}

# 1. Verify agent output contains OLLEH
if [ ! -f /logs/artifacts/answer.txt ]; then
  fail "missing /logs/artifacts/answer.txt"
elif ! grep -q "OLLEH" /logs/artifacts/answer.txt; then
  fail "answer.txt does not contain 'OLLEH' (content: '$(cat /logs/artifacts/answer.txt 2>/dev/null)')"
else
  echo "VERIFIED: answer.txt contains 'OLLEH'"
fi

# 2. Verify github was NOT reachable
if [ ! -f /logs/artifacts/github_test.txt ]; then
  fail "missing /logs/artifacts/github_test.txt"
elif grep -qE "HTTP/[0-9.]+ 200|HTTP/[0-9.]+ 301" /logs/artifacts/github_test.txt; then
  fail "github.com was reachable (agent cheated or network was open): $(cat /logs/artifacts/github_test.txt)"
else
  echo "VERIFIED: github.com was blocked"
fi

echo "$reward" > /logs/verifier/reward.txt
