#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import json
from pathlib import Path
from urllib.request import urlopen

# Both sidecars listen on 8080 but live at distinct addresses inside the DinD
# network; resolving them by hostname is itself part of what this task checks.
with urlopen("http://model:8080/status", timeout=30) as resp:
    model_status = json.loads(resp.read().decode("utf-8"))
Path("/app/model_status.json").write_text(json.dumps(model_status, indent=2))
print("Model status:", json.dumps(model_status, indent=2))

with urlopen("http://mini-service:8080/health", timeout=10) as resp:
    mini_status = json.loads(resp.read().decode("utf-8"))
Path("/app/mini_service_status.json").write_text(json.dumps(mini_status, indent=2))
print("Mini-service status:", json.dumps(mini_status))
PY
