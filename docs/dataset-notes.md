# Dataset notes

This guide documents dataset- and task-specific operational requirements, known upstream task defects, and recommended CLI configurations for running public benchmarks on GKE with `-e harbor_gke_ext:GKEEnvironment` and `--plugin harbor_gke_ext:CloudBuildPlugin`.

## How to use this guide

- **Separate upstream dataset defects from environment behavior**: Many public benchmarks ship tasks whose reference solution (`-a oracle`) or verifier script fails regardless of where the container runs (for example, due to unpinned pip/conda dependencies, truncated test IDs in `config.json`, or missing reference scripts). Calibrate each dataset with the oracle agent before comparing model scores.
- **Match node pool shape to workload characteristics**: Datasets vary from 1-CPU Python unit tests to 16-CPU C++/Go builds, AVX2 EDA flows, 300 GiB Docker-in-Docker snapshots, and L4/A100/H100 GPU kernels. For cluster-wide node pool design, see [Cluster setup](cluster-setup.md) and [Task sizing and placement](task-sizing-and-placement.md).

## Calibrate a dataset before scoring agents

Before running an agent evaluation on a new dataset or cluster, run the `oracle` agent across multiple attempts (`-k 3`) on the same cluster configuration:

```bash
uv run harbor run \
  -d <org>/<dataset> \
  -a oracle \
  -k 3 \
  -n 500 \
  -e harbor_gke_ext:GKEEnvironment \
  --plugin harbor_gke_ext:CloudBuildPlugin \
  --ek project_id="${PROJECT_ID}" \
  --ek region="${REGION}" \
  --ek cluster_name="${CLUSTER_NAME}" \
  --ek registry_location="${REGION}"
```

Classify the resulting tasks into three groups before scoring agents:

| Group | Criterion | How to handle in agent evaluations |
| --- | --- | --- |
| **Stable** | Oracle scores `1.0` in all attempts | Score normally. |
| **Flaky** | Oracle scores `1.0` in some attempts and `< 1.0` in others | Report separately or evaluate with `pass@k`; a single failure on a timing-sensitive task is not reliable signal. |
| **Broken** | Oracle scores `0.0` (or fails verification) in every attempt | Exclude with `-x <task>` and record the exclusion list alongside reported metrics. |

## General recommendations

The following best practices apply across all datasets unless noted otherwise:

- **Prebuild task images in parallel** with `--plugin harbor_gke_ext:CloudBuildPlugin`. See [Images and builds](images.md).
- **Set explicit Node Auto-Provisioning (NAP) disk defaults**: Without explicit `diskSizeGb` and `diskType` in your NAP profile, auto-provisioned nodes receive `100 GB` `pd-balanced` boot disks (~`43.8 GiB` allocatable ephemeral storage and `168 MiB/s` sustained throughput). See [Cluster setup](cluster-setup.md).
- **Use capped resources on static-CPU nodes for reproducible scoring**:
  - `--cpus auto --memory auto` currently resolves to `guarantee`, which sets `requests == limits == declared budget` to match Harbor's local Docker `--cpus` and `--memory` caps. (Whether capped-by-default remains the permanent `auto` mode is under final benchmark validation; see [Open questions](design-decisions.md#open-questions). Pass `--cpus guarantee --memory guarantee` to lock in strict caps explicitly, or `--cpus request --memory request` for uncapped Burstable execution. Request mode removes limits only on direct single-container Pods; Compose Pods always keep a Pod-level limit at least equal to the request.)
  - On shared GKE Standard nodes, a CPU limit enforces a Linux CFS quota while leaving all host cores visible in `/proc/cpuinfo`. Toolchains that size worker pools from visible logical CPUs (`nproc`, `os.cpu_count()`, `os.cpus()`, or Go `< 1.25`) spawn too many workers for their 1–2 CPU quota and memory cap. Running capped trials on a static-CPU node pool (`cpuManagerPolicy: static`, such as the `harbor-static-cpu` ComputeClass) assigns exclusive physical cores so the container sees only its allocated CPUs. See [Task sizing and placement](task-sizing-and-placement.md).
- **Use `--max-retries 2` to recover from transient node or network preemption**:
  - Harbor's `--max-retries` option retries transient infrastructure exceptions such as `TrialContainerLostError` and `GKEExecStreamClosedError`.
  - A Pod lost to preemption, eviction, or node loss before the trial starts is replaced by its Job without a retry. `--max-retries` covers losses after that.
  - By default, Harbor excludes evaluation and model errors from retry (`AgentTimeoutError`, `VerifierTimeoutError`, `RewardFileNotFoundError`, `RewardFileEmptyError`, `VerifierOutputParseError`, `ApiUsageLimitError`, `AgentSafetyRefusalError`, `AgentAuthenticationError`, and `ModelNotFoundError`), and completed trials that return `reward = 0.0` are never retried.

---

## `scale-ai/swe-bench-pro`

### Dataset profile

| Property | Value |
| --- | --- |
| **Tasks** | 731 |
| **Pod shape** | Shape A (single native container, no Compose) |
| **Declared `[environment]` budget** | `cpus = 1`, `memory_mb = 4096`, `storage_mb = 10240`, `allow_internet = true` |
| **`[agent]` / `[verifier]` `timeout_sec`** | `3000` / `3000` |
| **Images** | Prebuilt `jefzda/sweap-images:*` (`linux/amd64`), one image per task |
| **Verifier** | `tests/test.sh` runs `run_script.sh`, then `parser.py` matches stdout test IDs against a required list in `tests/config.json` |

### Operational guidance

1. **Run capped (`--cpus guarantee --memory guarantee`) on static-CPU nodes (`harbor-static-cpu`)**:
   - Each 1-CPU trial receives an exclusive core (`GOMAXPROCS=1`, `nproc=1`), eliminating both neighbor CPU/disk contention and over-parallelized worker spawns.
2. **Exclude the 24 upstream Known Issues tasks (`707` valid tasks)**:
   - The Harbor `swebenchpro` adapter documents [24 upstream tasks to exclude](https://github.com/harbor-framework/harbor/tree/main/adapters/swebenchpro#known-issues): 15 tasks whose gold patches fail verification upstream (`ansible` ×2, `element-web` ×2, `nodebb` ×1, `vuls` ×1, `navidrome` ×1, `gravitational/teleport` ×8) and 9 long-running tasks that time out (`tutao/tutanota` ×7, `gravitational/teleport` ×2).
3. **Run `gravitational/teleport` (`instance_gravitational__teleport-*`) as a separate pass with `--verifier-timeout-multiplier 1.5`**:
   - The `teleport` task images ship Go `1.23.1` (prior to Go 1.25's cgroup-aware `GOMAXPROCS`). On a 16-vCPU shared node with a 1-CPU / 4-GiB limit, `go test` spawns 16 parallel compiler/linker processes, exhausting the 4 GiB memory cap (`TrialContainerLostError`, exit code 137) or stalling under CFS throttling.
   - On static-CPU nodes, `go test` sees 1 CPU and stays comfortably under 4 GiB, but serial compilation of the largest packages (`lib/web`, `lib/services/local`) takes up to ~3,100 seconds against the 3,000-second verifier timeout. Passing `--verifier-timeout-multiplier 1.5` (4,500s) avoids verifier timeouts on those tasks.

```bash
# The 24 tasks documented in adapters/swebenchpro/README.md#known-issues
X_UPSTREAM=(
  # 15 tasks with invalid gold patches
  -x 'scale-ai/instance_ansible__ansible-cd473dfb2fdbc97acf3293c134b21cbbcfa89ec3-vba6da65a0f3baefda7a058ebbd0a8dcafb8512f5'
  -x 'scale-ai/instance_element-hq__element-web-880428ab94c6ea98d3d18dcaeb17e8767adcb461-vnan'
  -x 'scale-ai/instance_nodebb__nodebb-00c70ce7b0541cfc94afe567921d7668cdc8f4ac-vnan'
  -x 'scale-ai/instance_ansible__ansible-de5858f48dc9e1ce9117034e0d7e76806f420ca8-v1055803c3a812189a1133297f7f5468579283f86'
  -x 'scale-ai/instance_future-architect__vuls-bff6b7552370b55ff76d474860eead4ab5de785a-v1151a6325649aaf997cd541ebe533b53fddf1b07'
  -x 'scale-ai/instance_element-hq__element-web-aec454dd6feeb93000380523cbb0b3681c0275fd-vnan'
  -x 'scale-ai/instance_navidrome__navidrome-ee21f3957e0de91624427e93c62b8ee390de72e3'
  -x 'scale-ai/instance_gravitational__teleport-53814a2d600ccd74c1e9810a567563432b98386e-vce94f93ad1030e3136852817f2423c1b3ac37bc4'
  -x 'scale-ai/instance_gravitational__teleport-d6ffe82aaf2af1057b69c61bf9df777f5ab5635a-vee9b09fb20c43af7e520f57e9239bbcf46b7113d'
  -x 'scale-ai/instance_gravitational__teleport-baeb2697c4e4870c9850ff0cd5c7a2d08e1401c9-vee9b09fb20c43af7e520f57e9239bbcf46b7113d'
  -x 'scale-ai/instance_gravitational__teleport-e6d86299a855687b21970504fbf06f52a8f80c74-vce94f93ad1030e3136852817f2423c1b3ac37bc4'
  -x 'scale-ai/instance_gravitational__teleport-bb562408da4adeae16e025be65e170959d1ec492-vee9b09fb20c43af7e520f57e9239bbcf46b7113d'
  -x 'scale-ai/instance_gravitational__teleport-87a593518b6ce94624f6c28516ce38cc30cbea5a'
  -x 'scale-ai/instance_gravitational__teleport-2b15263e49da5625922581569834eec4838a9257-vee9b09fb20c43af7e520f57e9239bbcf46b7113d'
  -x 'scale-ai/instance_gravitational__teleport-02d1efb8560a1aa1c72cfb1c08edd8b84a9511b4-vce94f93ad1030e3136852817f2423c1b3ac37bc4'
  # 9 tasks that time out
  -x 'scale-ai/instance_tutao__tutanota-09c2776c0fce3db5c6e18da92b5a45dce9f013aa-vbc0d9ba8f0071fbe982809910959a6ff8884dbbf'
  -x 'scale-ai/instance_tutao__tutanota-12a6cbaa4f8b43c2f85caca0787ab55501539955-vc4e41fd0029957297843cb9dec4a25c7c756f029'
  -x 'scale-ai/instance_tutao__tutanota-1ff82aa365763cee2d609c9d19360ad87fdf2ec7-vc4e41fd0029957297843cb9dec4a25c7c756f029'
  -x 'scale-ai/instance_tutao__tutanota-db90ac26ab78addf72a8efaff3c7acc0fbd6d000-vbc0d9ba8f0071fbe982809910959a6ff8884dbbf'
  -x 'scale-ai/instance_tutao__tutanota-fb32e5f9d9fc152a00144d56dd0af01760a2d4dc-vc4e41fd0029957297843cb9dec4a25c7c756f029'
  -x 'scale-ai/instance_tutao__tutanota-d1aa0ecec288bfc800cfb9133b087c4f81ad8b38-vbc0d9ba8f0071fbe982809910959a6ff8884dbbf'
  -x 'scale-ai/instance_gravitational__teleport-0ecf31de0e98b272a6a2610abe1bbedd379a38a3-vce94f93ad1030e3136852817f2423c1b3ac37bc4'
  -x 'scale-ai/instance_tutao__tutanota-f3ffe17af6e8ab007e8d461355057ad237846d9d-vbc0d9ba8f0071fbe982809910959a6ff8884dbbf'
  -x 'scale-ai/instance_gravitational__teleport-e6895d8934f6e484341034869901145fbc025e72-vce94f93ad1030e3136852817f2423c1b3ac37bc4'
)

COMMON=(-d scale-ai/swe-bench-pro -a <agent> -n 500 --max-retries 2
  -e harbor_gke_ext:GKEEnvironment --plugin harbor_gke_ext:CloudBuildPlugin
  --ek project_id="${PROJECT_ID}" --ek location="${LOCATION}"
  --ek cluster_name="${CLUSTER_NAME}" --ek compute_class=harbor-static-cpu
  "${X_UPSTREAM[@]}")

# Pass 1: the 66 remaining gravitational/teleport tasks, with 1.5x verifier timeout for serial Go 1.23 builds
uv run harbor run "${COMMON[@]}" -i 'scale-ai/instance_gravitational__teleport-*' \
  --verifier-timeout-multiplier 1.5 --job-name swebenchpro-teleport

# Pass 2: the remaining 641 tasks at default timeouts
uv run harbor run "${COMMON[@]}" -x 'scale-ai/instance_gravitational__teleport-*' \
  --job-name swebenchpro-main
```

The same split is available as layered job config files in
[`examples/configs/swebenchpro/`](../examples/configs/swebenchpro/): `swebenchpro-common.yaml`
(environment, capped resources, `harbor-static-cpu`, retries, agent) plus one group file,
`swebenchpro-teleport.yaml` (66 tasks) or `swebenchpro-main.yaml` (641 tasks). Harbor appends
`datasets` lists across `-c` layers. On the command line, `-i` and `-x` are valid only together
with `-d` or `-p`, and that combination replaces the `datasets` list, so each group file carries
its complete dataset entry, including the 24 exclusions. Check the resolved task count with
`--dry-run` before launching.

### Calibration comparison across resource modes

Comparing 3-attempt (`-k 3`) oracle calibrations at `-n 500` across the full 731-task dataset and the 707-task subset (excluding the 24 Known Issues tasks):

| Configuration | Mean passing tasks (of 731) | Mean passing tasks (of 707) | Infrastructure exceptions |
| --- | --- | --- | --- |
| **Uncapped on shared nodes** (`--cpus request --memory request`) | 714.3 | 696.0 | 0 |
| **Capped on shared nodes** (`--cpus guarantee --memory guarantee`) | 701.0 | 687.0 | 12 (all `teleport`: 8 OOM/lost containers, 3 verifier timeouts, 1 other) |
| **Capped on static-CPU nodes** (`--cpus guarantee --memory guarantee`, `harbor-static-cpu`, `--max-retries 2`) | 723.7 | 704.3 | 0 in final results (4 transient node interruptions automatically retried) |

On static-CPU nodes (`-k 3`, 2,193 trials total), 701 of the 707 tasks passed in all 3 attempts. For a detailed breakdown of neighbor contention and CFS throttling on shared nodes, see [Why tasks fail for resource reasons](task-sizing-and-placement.md#why-tasks-fail-for-resource-reasons).

### Broken tasks: upstream verifier defects

In the following tasks (all included in the 24 Known Issues exclusions), the underlying test suite succeeds, but the required test ID list in `tests/config.json` contains malformed strings that `parser.py` can never match:

| Task prefix | Root cause |
| --- | --- |
| `instance_nodebb__nodebb-00c70ce7…` | Mocha reports `"failures": 0`, but required IDs in `config.json` are truncated (missing a closing `"` in `ACP default "off`) or strip a trailing space (`(length > 100) `), preventing an exact string match. |
| `instance_future-architect__vuls-bff6b755…` | All Go tests pass, but a required test ID in `config.json` is truncated (`"amazonlinux` with no closing quote). |
| `instance_ansible__ansible-de5858f4…` | Pytest reports `59 passed`, but 1 of the 58 required test IDs in `config.json` does not match any test emitted by the suite (`Required tests that passed: 57`). |

### Flaky and timing-sensitive tasks

Several tasks exhibit non-deterministic behavior due to race conditions or short hardcoded wall-clock timeouts in the upstream test suites:

| Task prefix | Uncapped (shared) | Capped (static CPU) | Local Docker (`1` CPU / `4 GiB`) | Failure mechanism |
| --- | --- | --- | --- | --- |
| `instance_qutebrowser__qutebrowser-bf045f7e…` | 1 / 16 | 3 / 3 | 1 / 1 | All 26 pytest tests pass, but `run_script.sh` merges stderr into stdout (`2>&1`). When Chromium's renderer logs `Failed to adjust OOM score of renderer … Permission denied (13)` between a test name and `PASSED`, `parser.py` fails to match the line. Not in the 24 Known Issues exclusions; expect occasional zero rewards. |
| `instance_element-hq__element-web-aec454dd…` | 0 / 14 | 0 / 3 | 0 / 1 | 2–3 `InviteDialog` tests fail with `recursive use of an object detected which would lead to unsafe aliasing in rust` from the `@matrix-org/matrix-wysiwyg` WASM module. Included in the 24 Known Issues exclusions. |
| `instance_element-hq__element-web-41dfec20…` | 6 / 13 | 2 / 3 | 1 / 1 | `SendWysiwygComposer › Should render WysiwygComposer when isRichTextEnabled is at true` intermittently times out waiting for `[data-testid="WysiwygComposer"]` while the WASM editor initializes under Jest (`--maxWorkers=1`). |
| `instance_tutao__tutanota-51818218…` | 8 / 14 | 3 / 3 | 1 / 1 | Fails intermittently when the background test build server has not finished emitting `/app/test/build/bootstrapTests-api.js` or when an `ospec` test exceeds its hardcoded `200ms` async timeout. |
| `instance_future-architect__vuls-83bcca6e…` | 3 / 3 | 3 / 4 | – | `Test_detectScanDest/multi-addr` compares an un-sorted slice whose iteration order varies (`[192.168.1.1:22 127.0.0.1:22]` vs `[127.0.0.1:22 192.168.1.1:22]`). |

Across the broader JavaScript/TypeScript repositories in `swe-bench-pro`, hardcoded test-runner timeouts are the primary source of run-to-run variance under CPU contention:

| Repository | Upstream wall-clock timeout |
| --- | --- |
| `nodebb` | Mocha hook `Timeout of 30000ms exceeded` (`ERR_MOCHA_TIMEOUT`) |
| `tutao/tutanota` | `ospec` async timeouts of `200ms`–`500ms`, or test build server startup race |
| `element-hq/element-web` | React Testing Library query timeouts waiting on WASM initialization (`--maxWorkers=1`) |
| `protonmail/webclients` | Jest `Exceeded timeout of 5000 ms for a test` (e.g., `webclients-814270`) |

---

## `swe-bench/swe-bench-verified`

### Dataset profile

| Property | Value |
| --- | --- |
| **Tasks** | 500 |
| **Pod shape** | Shape A (single native container, no Compose) |
| **Declared `[environment]` budget** | `cpus = 1`, `memory_mb = 4096`, `storage_mb = 10240` |
| **Architecture** | `linux/amd64` (`x86_64`) |
| **Calibration (`-k 3`, `harbor-static-cpu`, `496` tasks)** | **494 / 496** tasks pass `3 / 3` (`1,482 / 1,488` trials `1.0`), **0** infrastructure exceptions |

### Broken and upstream-harness tasks

Six tasks in `swe-bench/swe-bench-verified` fail unconditionally due to unpinned transitive dependencies in the upstream `swebench/sweb.eval.x86_64.*` base images (4 documented in the [SWE-bench adapter Known Issues](https://github.com/harbor-framework/harbor/tree/main/adapters/swebench#known-issues-failing-oracle-tasks) and 2 `pylint` tasks affected by `tomlkit >= 0.13`):

| Category | Task IDs | Root cause |
| --- | --- | --- |
| **Intrinsically broken (`setuptools`)** | `astropy__astropy-7606`<br>`astropy__astropy-8707`<br>`astropy__astropy-8872` | Upstream environment setup did not pin `setuptools`, installing a newer `setuptools` that removed `setuptools.dep_util` (`ModuleNotFoundError: No module named 'setuptools.dep_util'`). |
| **Intrinsically broken (`asgiref`)** | `django__django-10097` | Upstream environment setup installed `asgiref >= 3.6` (which uses Python 3.6+ variable type annotations) into a Python 3.5 conda environment, failing with `SyntaxError`. |
| **Intrinsically broken (`tomlkit`)** | `pylint-dev__pylint-6528`<br>`pylint-dev__pylint-7277` | Upstream image installs unpinned `tomlkit==0.13.2` (`tomlkit>=0.10.1`), which raises `tomlkit.exceptions.TOMLKitError: Can't add a table to a dotted key` in `PASS_TO_PASS` tests `TestCallbackOptions.test_generate_toml_config*`. |
| **Upstream SWE-bench harness fix (`PR #475`, 2 tasks)** | `sphinx-doc__sphinx-8595`<br>`sphinx-doc__sphinx-9711` | Passed `3 / 3` in `swe-bench/swe-bench-verified@sha256:b934b0cc...`. On older prebuilt images prior to `SWE-bench` PR `#475`, `packaging >= 22.0` caused `ImportError: cannot import name 'LegacyVersion' from 'packaging.version'`. |
| **CPU-bound single-core verifier (1 task)** | `scikit-learn__scikit-learn-14710` | Runs single-threaded `HistGradientBoostingClassifier` numerical tests (`test_gradient_boosting.py`, `test_warm_start.py`, `test_splitting.py`). On slow or throttled `e2` cores, verification can approach the verifier timeout; run on static-CPU nodes (`harbor-static-cpu`) with `--verifier-timeout-multiplier 1.5` (passes `3 / 3`). |

Exclude the 6 broken tasks when scoring agents:

```bash
uv run harbor run \
  -d swe-bench/swe-bench-verified \
  -a <agent> \
  --max-retries 2 \
  -e harbor_gke_ext:GKEEnvironment \
  --plugin harbor_gke_ext:CloudBuildPlugin \
  -x '*astropy__astropy-7606*' \
  -x '*astropy__astropy-8707*' \
  -x '*astropy__astropy-8872*' \
  -x '*django__django-10097*' \
  -x '*pylint-dev__pylint-6528*' \
  -x '*pylint-dev__pylint-7277*' \
  --verifier-timeout-multiplier 1.5 \
  --ek project_id="${PROJECT_ID}" \
  --ek location="${LOCATION}" \
  --ek cluster_name="${CLUSTER_NAME}" \
  --ek compute_class=harbor-static-cpu
```

---

## `terminal-bench` (`terminal-bench/terminal-bench@4`), `long-horizon-terminal-bench/lhtb`, and `ryanmarten/tb4-preview`

### `terminal-bench/terminal-bench@4` calibration summary

In a 3-attempt (`-k 3`) oracle calibration across all 66 tasks of `terminal-bench/terminal-bench@4` on GKE Standard (`harbor-static-cpu` for CPU tasks, `--max-retries 2`):

| Subset | Tasks | Oracle result (`-k 3`) | Infrastructure exceptions | Notes |
| --- | --- | --- | --- | --- |
| **CPU tasks** (`harbor-static-cpu`) | 63 | **61 / 63** pass `3 / 3` (`185 / 189` trials `1.0`) | 0 | All 8 multi-container Compose tasks (`ctr-optimization`, `heat-pump-warranty`, `intrastat-meldung`, `kv-live-surgery`, `legacy-utility-triage`, `live-database-cutover`, `medical-claims-processing`, `payments-pipeline-fix`) pass `3 / 3`. |
| **Flaky CPU task** (`uefi-bootkit`) | 1 (of 63) | `2 / 3` pass | 0 | Boots a QEMU guest over a serial console inside the container; occasional QEMU serial boot timing variance can cause a single attempt to return `0.0`. |
| **Broken CPU task** (`cad-model`) | 1 (of 63) | `0 / 3` pass | 0 | Upstream dependency defect: `solve.sh` runs `pip install build123d` unpinned, pulling `ocp_gordon` which fails with `ImportError: libOpenGL.so.0: cannot open shared object file`. Exclude with `-x '*cad-model*'`. |
| **GPU task compatible with L4** (`math-eval-grader`) | 1 (of 3 GPU) | **3 / 3** pass with `--ek gpu_override=nvidia-l4` | 0 | Declares `gpu_types = ["A100"]` (`cpus = 8`, `memory_mb = 32768`), fits within 24 GiB L4 VRAM. |
| **Hopper H100-only GPU tasks** (`fp8-rmsnorm-gemm`, `jax-speedrun-gpu`) | 2 (of 3 GPU) | Require `H100` (`a3-highgpu-1g`) | 0 | Declare `gpu_types = ["H100"]` (`cpus = 16`, `memory_mb = 65536`). `fp8-rmsnorm-gemm` uses Hopper `sm_90a` FP8 PTX instructions and `jax-speedrun-gpu` targets H100 VRAM/throughput; both return `0.0` if remapped to L4. |

### 1. `apex-openroad-ibex-signoff`: `x86_64` AVX2 requirement and machine-type pinning

`apex-openroad-ibex-signoff` runs the OpenROAD ASIC synthesis and static timing analysis toolchain, whose prebuilt binaries use `x86_64` instructions not available on every host behind `e2` machine types. When scheduled onto such a node, OpenROAD crashes with `SIGILL: child killed: illegal instruction`.

Harbor does not pin this task automatically. Pin it to a fixed-ISA family such as `n2`:

```bash
--ek task_machine_types=apex-openroad-ibex-signoff=n2-standard-4
```

This adds `nodeSelector: {"cloud.google.com/machine-family": "n2"}` to the Pod. With node auto-provisioning the node is created on demand. Without it, the cluster needs an `n2` pool with at least 4 vCPUs; if no qualifying `n2` pool exists, the trial fails immediately with `UnsatisfiableMachineTypeError`.

Alternatively, route the task to a ComputeClass (`--ek task_compute_classes=apex-openroad-ibex-signoff=<class>`) or a dedicated node pool (`--ek task_node_pools=apex-openroad-ibex-signoff=worker-pool`) without setting `task_machine_types` (combining `task_machine_types` with a ComputeClass or a pool from a different machine family raises `PlacementConflictError`).

### 2. GPU tasks and substituting L4 GPUs for scarce A100 capacity

Several Terminal-Bench tasks declare hardware accelerators in `task.toml`:
- Tasks in `terminal-bench` and `terminal-bench-science` declare `gpu_types = ["A100"]`, `gpu_types = ["H100"]`, or `gpu_types = ["L4"]`.
- In `terminal-bench/terminal-bench@4`, `math-eval-grader` declares `gpu_types = ["A100"]` (`cpus = 8`, `memory_mb = 32768`), while `fp8-rmsnorm-gemm` and `jax-speedrun-gpu` declare `gpu_types = ["H100"]` (`cpus = 16`, `memory_mb = 65536`, requiring an `a3-highgpu-1g` node pool or `nvidia-h100-80gb` NAP limits).

When `a2-highgpu-1g` (`nvidia-tesla-a100`) capacity is scarce in your region and the workload fits within 24 GiB of VRAM (such as `math-eval-grader`), pre-create an L4 GPU node pool (`g2-standard-8` or `g2-standard-12` with `--accelerator type=nvidia-l4,count=1`) and pass `--ek gpu_override=nvidia-l4` to remap A100 tasks onto L4 GPUs without editing `task.toml`:

```bash
--ek gpu_override=nvidia-l4
```

See [Accelerators: Pre-creating an L4 GPU node pool and substituting it for A100](accelerators.md#pre-creating-an-l4-gpu-node-pool-and-substituting-it-for-a100) for the node pool setup command.

### 3. Upstream task defects, open `"task fix"` issues, and continuous scoring

- **Dummy API keys for tasks invoking LLM SDKs in tests**: Some tasks validate client configurations or invoke SDK initializers during verification. When running with `-a oracle` (or without live provider keys), pass placeholder keys so verifier scripts that check for key presence do not fail early:
  ```bash
  --ae OPENAI_API_KEY=fakekey123 --ve OPENAI_API_KEY=fakekey123 \
  --ae ANTHROPIC_API_KEY=fakekey123 --ve ANTHROPIC_API_KEY=fakekey123
  ```
- **Upstream `harbor-framework/terminal-bench` open `"task fix"` issues** ([issue tracker](https://github.com/harbor-framework/terminal-bench/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22task%20fix%22)):
  - `freecad-impeller`, `freecad-platform-drawing`, and `freecad-spring-clip` ([#2002](https://github.com/harbor-framework/terminal-bench/issues/2002), [#1570](https://github.com/harbor-framework/terminal-bench/issues/1570)): In earlier `terminal-bench@4` revisions, the separate verifier image (`tests/Dockerfile`) failed to build (`ERROR: Cannot uninstall vtk 9.2.6` / `uninstall-distutils-installed-package`) due to a conda/pip `vtk` ownership conflict (resolved in `terminal-bench@sha256:39d9f44b...`, where all three pass `3 / 3`).
  - `fp8-rmsnorm-gemm` and `vf2-speedup-networkx` ([#2003](https://github.com/harbor-framework/terminal-bench/issues/2003)): CRLF (`\r`) line endings in runtime/solution scripts caused Oracle execution to fail in earlier revisions (`vf2-speedup-networkx` is fixed in `terminal-bench@sha256:39d9f44b...`).
  - `distributed-dedup` ([#1755](https://github.com/harbor-framework/terminal-bench/issues/1755), [#1638](https://github.com/harbor-framework/terminal-bench/issues/1638)): Reference `solution/SubmissionDedup.scala` imported `tb.dedup.task.Hashing`, which existed only in the verifier container (fixed in `terminal-bench@sha256:39d9f44b...`).
  - `html-js-filter` ([#1975](https://github.com/harbor-framework/terminal-bench/issues/1975)): Python 3.12 patch version mismatch between agent (`3.12.14`, supporting `HTMLParser(scripting=True)`) and verifier (`3.12.3`) in earlier revisions.
  - `atrx-vep-crispr` ([#1647](https://github.com/harbor-framework/terminal-bench/issues/1647)): Environment `Dockerfile` runs Ensembl `INSTALL.pl` against unauthenticated `api.github.com` (60 req/hr/IP limit) and can hang if rate-limited during build.
  - `fix-uautomizer-soundness` ([#1601](https://github.com/harbor-framework/terminal-bench/issues/1601), [#1541](https://github.com/harbor-framework/terminal-bench/issues/1541)): Silently unsolvable on `arm64` hosts (all verdicts `UNKNOWN`); schedule on `amd64` worker pools.
  - `embedding-drift-monitor` ([#1574](https://github.com/harbor-framework/terminal-bench/issues/1574), [#1636](https://github.com/harbor-framework/terminal-bench/issues/1636)) and `glycan-ms2-elucidation` ([#1569](https://github.com/harbor-framework/terminal-bench/issues/1569), [#1562](https://github.com/harbor-framework/terminal-bench/issues/1562)): Verifier pipe handling and overly strict assertions.
  - `music-harmony` ([#1332](https://github.com/harbor-framework/terminal-bench/issues/1332)), `gsea-proteomics` ([#1331](https://github.com/harbor-framework/terminal-bench/issues/1331)), and `ontology-kg-querying` ([#1450](https://github.com/harbor-framework/terminal-bench/issues/1450)): Task specification / strict verifier issues tracked upstream.
  - Verifier hardening issues ([#1632](https://github.com/harbor-framework/terminal-bench/issues/1632)–[#1639](https://github.com/harbor-framework/terminal-bench/issues/1639)): `batched-eval-parity`, `ks-solver-cpp`, `math-eval-grader`, `interleaved-vigenere`, and `load_model.py` JAX verifier isolation.
- **`vector-db-iterative-build` (`long-horizon-terminal-bench/lhtb`) verifier timeout**: The separate verifier benchmark suite takes ~32 minutes (`1921`s on `e2-standard-16` / `n2-standard-16`), just exceeding the `1800`s `verifier.timeout_sec` declared in `task.toml`. Pass `--verifier-timeout-multiplier 2.0` so verification completes cleanly (`reward = 0.8182` with `-a oracle`).
- **`sudoku-recovery` (`long-horizon-terminal-bench/lhtb`)**: The reference oracle (`solution/ref_solver.py`) fails with `ModuleNotFoundError: No module named 'sudoku_oracle'` because `sudoku_oracle` is packaged only under `tests/` and `environment/harness/private/`.
- **Continuous rewards in `long-horizon-terminal-bench/lhtb`**: Twelve tasks emit continuous verifier scores in the range `(0, 1)` rather than binary `{0, 1}` rewards. Compare agents on this dataset using mean reward rather than binary pass rate.
- **`cad-model` (`terminal-bench/terminal-bench@4` and `ryanmarten/tb4-preview`)**: Fails upstream because `solve.sh` runs `pip install build123d` unpinned, which pulls `ocp_gordon` and fails at import time with `ImportError: libOpenGL.so.0: cannot open shared object file` (or `ModuleNotFoundError: No module named 'OCP.collections'` in `ryanmarten/tb4-preview`). Exclude with `-x '*cad-model*'`.

---

## `orca-bench/orca-bench-verified`

### DinD Shape C execution

Every task in `orca-bench/orca-bench-verified` runs `main` with `privileged: true` and a `/var/run/docker.sock` bind mount, so the environment classifies the entire dataset into **Shape C (Main-in-DinD)**. See [Docker in Docker](docker-in-docker.md).

### Sizing storage and timeouts for task `701e915cf705cdf6` (301 GiB hardlink tree)

Task `701e915cf705cdf6` packages 1,623 Prometheus TSDB snapshots and OpenSearch indices under `/app/data` inside a `25.6 GiB` compressed image. In the OCI layer tarballs, identical immutable blocks across snapshots are deduplicated via hardlinks (~`55 GiB` unpacked when hardlinks are preserved, or **301 GiB** if every hardlink is expanded into an independent file).

- **When `dind-cache-main` pulls via `docker pull` (requires registry egress and, for private Artifact Registry images, `--ek allow_metadata_server=true`; see [Image delivery under restricted network modes](cluster-setup.md#image-delivery-under-restricted-network-modes))**: `dockerd` unpacks the compressed OCI layers directly into `/var/lib/docker/overlay2`, preserving hardlinks and keeping the disk footprint at ~`55 GiB`.
- **When `dind-cache-main` falls back to `tar -cf - / | docker import` (for example, if registry or metadata access is blocked)**: GKE Image Streaming (`gcfs`) reports `st_nlink = 1` for every file, preventing `tar` from deduplicating hardlinks and writing all **301 GiB** into `/var/lib/docker`. At ~`60 MiB/s` sustained disk write throughput, writing 301 GiB takes ~85 minutes.

When running `orca-bench/orca-bench-verified` (and specifically `701e915cf705cdf6`), configure the following environment kwargs so the Pod does not get evicted for ephemeral-storage pressure or time out during DinD initialization:

- `--ek task_dind_storage_mb=701e915cf705cdf6=65536` when `dind-cache-main` uses `docker pull` on a node pool with enough boot-disk or Local SSD capacity for the ~`55 GiB` footprint.
- For the 301 GiB fallback path, either:
  - Set `--ek task_dind_storage_mb=701e915cf705cdf6=308224` (301 GiB) and run on nodes whose allocatable ephemeral storage can hold it.
  - Add `--ek scratch_volume_size=350Gi` to back `/var/lib/docker` with a per-Pod Persistent Disk. This moves the data off the node boot disk, but the `dind-engine` container still reserves node ephemeral storage equal to the estimate or the `task_dind_storage_mb` value. The setting applies to every Compose trial in the job, and it also backs Compose named volumes.
- `--ek pod_ready_timeout=5400` (sets the Pod readiness wait to 90 minutes; the default is `max(1200, build_timeout_sec)` seconds).
- `--ek deadline_buffer_minutes=90` (raises the Kubernetes Job `activeDeadlineSeconds` buffer above the `15`-minute default so the Job controller does not terminate the trial before the agent starts).

---

## `luosuu/SWE-kokkos-bench`

- **Cloud Build C++ compilation OOM / `INTERNAL_ERROR`**:
  Three tasks (`kokkos__pykokkos-422`, `kokkos__pykokkos-424`, and `kokkos__kokkos-kernels-2864`) compile heavy C++ template instantiations in parallel during `docker build`. On Cloud Build's default worker (used when no machine type is set), parallel `g++`/`nvcc` processes exhaust memory and fail with `INTERNAL_ERROR`. Pass a larger Cloud Build machine type and, optionally, a larger retry budget (`build_attempts` counts total attempts per image; the default is `3`):
  ```bash
  --plugin harbor_gke_ext:CloudBuildPlugin \
  --pk cloud_build_machine_type=E2_HIGHCPU_32 \
  --pk build_attempts=5
  ```
  (If building on the fly via `GKEEnvironment` without the prebuild plugin, pass `--ek cloud_build_machine_type=E2_HIGHCPU_32`.)
- **L4 GPU requirement**:
  Two tasks declare `gpus = 1` and `gpu_types = ["L4"]`. Provision an L4 GPU node pool (`g2-standard-8` or `g2-standard-12`) or configure NAP limits for `nvidia-l4`. See [Accelerators](accelerators.md).

---

## `bespokelabs/autoresearch-exam`

None of the 29 tasks in `bespokelabs/autoresearch-exam` ships a reference `solution/solve.sh` script, so running `-a oracle` fails on every task with `FileNotFoundError`. Use `-a nop` to verify that environments start and verifiers execute cleanly, or evaluate with a real agent.

- **H100 requirement:** 7 tasks declare `gpus = 1` and `gpu_types = ["H100"]`. Without H100 quota and capacity (an `a3-highgpu-1g` pool or `nvidia-h100-80gb` NAP limits), their Pods stay `Pending`, and because these tasks set a large `build_timeout_sec`, the wait can last hours (`pod_ready_timeout` defaults to `max(1200, build_timeout_sec)`). Check H100 quota before the run. Otherwise exclude these tasks with `-x`, remap them with `--ek gpu_override=<label>` (the task may then fail on the smaller GPU), or lower `--ek pod_ready_timeout`.
- **Cloud Build memory:** `fastergcg-candidate-token-rank-ccc` loads a Vicuna-7B checkpoint shard with `torch.load` during `docker build`. On Cloud Build's default worker, that step is OOM-killed (exit code 137). Build with `--pk cloud_build_machine_type=E2_HIGHCPU_32` (or `--ek cloud_build_machine_type=E2_HIGHCPU_32` for inline builds).

---

## `zenml/zenml-bench`

Every task Dockerfile in `zenml/zenml-bench` begins with `FROM zenml-bench/base:0.96.4`, which is not published on Docker Hub (the upstream repository returns `404 Not Found`). Image builds will fail in every environment until you build `zenml-bench/base:0.96.4` from the upstream ZenML benchmark repository, push it to your Artifact Registry, and rewrite or tag the base image accordingly.
