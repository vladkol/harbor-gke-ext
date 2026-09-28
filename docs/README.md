# harbor-gke-ext documentation

Start with the [quick start](../README.md) if you have not run a task yet.

## By task

| You want to | Read |
| --- | --- |
| Provision a cluster and grant the right IAM | [Cluster setup](cluster-setup.md) |
| Look up an option, a default, or a `task.toml` field | [Configuration reference](configuration.md) |
| Diagnose a specific error message | [Troubleshooting](troubleshooting.md) |
| Speed up a large job | [Images and builds](images.md) |
| Tune timeouts, deadlines, and retries | [Runtime](runtime.md) |
| Request a GPU or TPU | [Accelerators](accelerators.md) |
| Restrict what a task can reach | [Networking and security](networking-and-security.md) |
| Run a specific dataset, or check whether a failing task is a known defect | [Dataset notes](dataset-notes.md) |
| Choose CPU, memory, and placement for a dataset, or understand resource-related failures | [Task sizing and placement](task-sizing-and-placement.md) |

## By subsystem

| Document | Contents |
| --- | --- |
| [Architecture](architecture.md) | The execution model, the three Pod shapes (A native, B hybrid DinD, C main-in-DinD), the module map, and the invariants that hold everywhere. Read this first. |
| [Compose translation](compose-translation.md) | How a Compose file becomes a Pod: normalization, the placement classifier (Shapes A, B, and C) and its reason codes, and per-key translation fidelity. |
| [Docker-in-Docker](docker-in-docker.md) | The DinD fallback planes (Shape B hybrid and Shape C main-in-DinD): container roster, `docker pull` image materialization, exec routing via `dind_services`, and Autopilot admissibility. |
| [Autopilot](autopilot.md) | GKE Autopilot resource rounding, `emptyDir` storage caps, `scratch_volume_size`, DinD allowlist, and gVisor. |
| [Networking and security](networking-and-security.md) | Network modes and the policy objects they emit, per-container security contexts, and the threat model. |
| [Accelerators](accelerators.md) | GPU and TPU requests, accelerators inside Compose, and the storage and ComputeClass interaction. |
| [Runtime](runtime.md) | Trial lifecycle, exec transports, deadline computation, retry budgets, concurrency, and cleanup. |
| [Images and builds](images.md) | Content-addressed images, lazy repository creation, the BuildKit cache, Cloud Build, and prebuilding a dataset. |

## Reference and history

| Document | Contents |
| --- | --- |
| [Configuration reference](configuration.md) | Every `--ek` key, plugin option, and `task.toml` field, with defaults and precedence. |
| [Cluster setup](cluster-setup.md) | Autopilot and Standard provisioning, optimizing clusters for benchmark datasets, node pools, ComputeClasses, and ISA/storage tuning. |
| [Troubleshooting](troubleshooting.md) | Runbook organized by literal error text. |
| [Known issues](known-issues.md) | Known v0.1.0 behavioral limitations and workarounds. |
| [Dataset notes](dataset-notes.md) | Per-dataset run recommendations, calibration procedure, and tasks known to be broken or unreliable on GKE. |
| [Design decisions](design-decisions.md) | Why the design is what it is, with the measurements behind each choice, current tradeoffs, and the roadmap. |
| [Task sizing and placement](task-sizing-and-placement.md) | Docker vs GKE resource semantics, why tasks fail for resource reasons, a deterministic sizing and placement procedure, and static-CPU recipes. |
