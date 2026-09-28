#!/usr/bin/env bash
set -euo pipefail

mkdir -p /logs/verifier

python3 - <<'PY'
import json
from pathlib import Path

try:
    model = json.loads(Path("/app/model_status.json").read_text())
    mini = json.loads(Path("/app/mini_service_status.json").read_text())

    print("model_status.json:", json.dumps(model, indent=2))
    print("mini_service_status.json:", json.dumps(mini, indent=2))

    # --- The GPU-in-DinD assertions -------------------------------------
    # Ordered so the failure message identifies WHICH link in the chain broke.

    # 1. Device nodes reached the nested container.
    devs = model.get("dev_nodes") or []
    if not any(d.startswith("/dev/nvidia") for d in devs):
        raise AssertionError(f"no /dev/nvidia* device inside the DinD sidecar: {devs}")

    # 2. The userspace driver was bind-mounted through. This is the real gate:
    #    device nodes alone are present even without an allocation.
    if int(model.get("driver_lib_count") or 0) <= 0:
        raise AssertionError(
            f"driver libs missing at /usr/local/nvidia/lib64 inside the DinD "
            f"sidecar (count={model.get('driver_lib_count')})"
        )

    # 3. Harbor injected LD_LIBRARY_PATH; the GKE device plugin never does.
    if "/usr/local/nvidia/lib64" not in (model.get("ld_library_path") or ""):
        raise AssertionError(
            f"LD_LIBRARY_PATH missing the driver dir: "
            f"{model.get('ld_library_path')!r}"
        )

    # 4. End to end: NVML actually talked to the driver.
    if model.get("nvidia_smi_rc") != 0:
        raise AssertionError(
            f"nvidia-smi failed rc={model.get('nvidia_smi_rc')}: "
            f"{model.get('nvidia_smi_out')}"
        )
    if not (model.get("nvidia_smi_out") or "").strip():
        raise AssertionError("nvidia-smi returned rc=0 but produced no GPU rows")

    # --- Collision handling still works ---------------------------------
    if model.get("service") != "model" or model.get("port") != 8080:
        raise AssertionError(f"unexpected model identity: {model}")
    if mini.get("status") != "healthy" or mini.get("port") != 8080:
        raise AssertionError(f"mini-service not healthy on 8080: {mini}")

    print("ALL VERIFICATION CHECKS PASSED!")
    print("GPU seen by the DinD sidecar:", model.get("nvidia_smi_out"))
    Path("/logs/verifier/reward.txt").write_text("1.0\n")
except Exception as exc:
    print(f"VERIFICATION FAILED: {exc}")
    Path("/logs/verifier/reward.txt").write_text("0.0\n")
    raise
PY
