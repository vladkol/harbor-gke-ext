# Images and container registry

This document explains how `harbor-gke-ext` builds, caches, and pulls container images using Google Artifact Registry and Cloud Build.

## Image discovery and planning

The system plans image builds using the `ImageSpec` and `TaskImagePlan` data structures in `image_plan.py`. The pre-builder structurally analyzes the task directory to discover three distinct image roles:

- **`agent_env`**: The primary agent sandbox, built from the `<task>/environment` directory.
- **`verifier`**: The verification environment, built from `<task>/tests` or `<task>/steps/<name>/tests`. When a task uses a separate verifier environment, the pre-builder and the runtime resolve this to a different content digest than the agent image. This produces a second Artifact Registry tag.
- **`compose_sidecar`**: Background service containers, built from any non-`main` Docker Compose service that declares a `build:` section.

Discovery starts from the known role directories. A role directory is a build context when it contains its Dockerfile (`Dockerfile`, or the non-default name that the role specifies), unless a prebuilt `docker_image` from `task.toml` applies. When `task.toml` is missing or can't be parsed, the pre-builder builds every role context it finds. The pre-builder coordinates with the runtime using the `dedupe_specs()`, `record_planned_images()`, `was_image_planned()`, and `is_plan_published()` functions. This guarantees that exactly one build occurs per unique build context across an entire dataset job, preventing redundant inline builds.

## Task image resolution

The environment computes a content digest of the build context and builds the image URL in this format:

`{registry_location}-docker.pkg.dev/{project_id}/{registry_name}/task-{digest}:latest`

`{digest}` is 32 hex characters, or 64 hex characters when the build uses a non-default Dockerfile (see [Cache invalidation](#cache-invalidation)). At runtime, the environment also accepts an existing image under the legacy name `<environment_name>-<digest>:latest`.

### Cache invalidation

The environment calculates the content digest by computing a SHA-256 hash (truncated to 32 hex characters) over the sorted relative path, byte size, and raw contents of every regular file in the build context. It skips `.DS_Store`, `.git`, `__pycache__`, symlinks, and non-regular files. If the context contains no regular files, it hashes `docker_image`, or the directory name when no `docker_image` is set. If a non-default Dockerfile name is specified (for example, `Dockerfile.gpu`), the digest is the full 64-hex SHA-256 of `<32-hex digest>:dockerfile=<name>`.

| Change type | Produces a new digest? |
|---|---|
| File content modification | Yes |
| File rename or move | Yes |
| Adding or removing a file | Yes |
| File size change | Yes |
| Non-default Dockerfile name | Yes |
| File mode change (`chmod`) | No |

> [!WARNING]
> File permissions (modes) are not included in the hash. Changing a file's mode with `chmod` does not alter the digest, which causes the environment to reuse a stale cached image.

## BuildKit cache

When a build runs in Cloud Build, a generated step invokes Docker Buildx to export all intermediate layers (`mode=max`) to a sibling cache tag. For an image tagged `task-<digest>:latest`, the cache resides at `task-<digest>:cache`. Subsequent builds reference this cache tag to accelerate layer resolution.

## Pre-building and CloudBuildPlugin

The system builds images either during the trial (an inline build) or beforehand via a pre-build step. Inline builds make up to 3 attempts in total, with `wait_exponential(multiplier=2, min=5, max=60)` backoff (5 s, then 5 s). The inline retry has no exception filter, so it retries any exception, including a failed build.

The `CloudBuildPlugin` intercepts the trial plan and builds all required images concurrently in Cloud Build before any trials start. Its `on_job_end` method is an explicit no-op, so images remain in the registry after the job finishes.

### CloudBuildPlugin options

Pass plugin options via `--pk <key>=<value>` (`--plugin-kwarg`), or let the plugin inherit matching `--ek` environment kwargs from the job configuration. The plugin logs a warning (`Unrecognized CloudBuildPlugin kwargs (--pk) ignored: ...`) for `--pk` keys that it doesn't recognize, and ignores them:

| Option | Default | Description |
|---|---|---|
| `concurrency` | `25` | Maximum number of concurrent Cloud Build jobs. |
| `timeout_sec` | `10800` | Maximum duration (3 hours) per Cloud Build job. |
| `build_attempts` | `3` | Total Cloud Build attempts per image, including the first (minimum `1`). |
| `fail_on_incomplete` | `false` | When `true` (or when `--ek prebuild_fail_on_incomplete=true` is set), raises `RuntimeError` and aborts the job if any image fails to pre-build instead of falling back to inline builds. |
| `force_build` | `false` | Rebuild and push images even when the tag already exists in Artifact Registry. |
| `project_id` | Derived | The Google Cloud project ID for Cloud Build and Artifact Registry. |
| `registry_name` | `"harbor-tasks"` | The Artifact Registry repository name. |
| `registry_location` / `location` / `region` | Derived | The region hosting the Artifact Registry repository and Cloud Build jobs. |
| `private_pool` / `cloud_build_worker_pool` | `None` | Private Cloud Build worker pool resource name (`projects/.../locations/.../workerPools/...`). Mutually exclusive with `cloud_build_machine_type`. |
| `cloud_build_machine_type` | `None` | Cloud Build machine type (e.g., `E2_HIGHCPU_32`). Mutually exclusive with `private_pool`. |
| `cloud_build_disk_size_gb` | `None` | Requested disk size (in GB) for Cloud Build workers. |

### Entry points

You can execute the pre-build process as a Harbor plugin or using the standalone CLI.

**As a plugin:**
```bash
harbor run --dataset my-tasks \
  -e harbor_gke_ext:GKEEnvironment \
  --plugin harbor_gke_ext:CloudBuildPlugin \
  --pk concurrency=50 \
  --pk fail_on_incomplete=true \
  --pk private_pool=projects/my-proj/locations/us-central1/workerPools/my-pool
```

**As a standalone CLI:**
```bash
harbor-gke-ext-prebuild ./my-dataset-dir \
  --project-id my-proj \
  --location us-central1 \
  --registry-name harbor-tasks \
  --concurrency 50 \
  --timeout-sec 10800 \
  --worker-pool projects/my-proj/locations/us-central1/workerPools/my-pool
```

## Registry configuration and lazy repository creation

The environment accepts several `--ek` flags and task settings to control registry behavior, pre-build enforcement, and inline builds:

- **`registry_name`**: The Artifact Registry repository name (default `harbor-tasks`).
- **`registry_location`**: The region hosting the Artifact Registry repository.
- **`image_pull_secrets`**: Kubernetes Secrets attached to the Pod to authenticate against private registries.
- **`prebuild_fail_on_incomplete`**: When `true`, instructs `CloudBuildPlugin` to raise `RuntimeError` if any planned image fails pre-building rather than proceeding to trial execution with inline build fallbacks.
- **`cloud_build_machine_type`**: The machine type for inline Cloud Build jobs (e.g., `E2_HIGHCPU_32`).
- **`cloud_build_disk_size_gb`**: The requested disk size for inline Cloud Build jobs.
- **`cloud_build_worker_pool`** / **`private_pool`**: The private worker pool resource name for inline builds. Cannot be combined with `cloud_build_machine_type`.
- **Task `build_timeout_sec` (`task.toml` `[environment].build_timeout_sec`)**: Does not govern the Cloud Build job timeout; instead, at runtime it extends the default Pod readiness timeout when `build_timeout_sec > 1200` (`pod_ready_timeout = max(1200, build_timeout_sec)`) and sets the default `compose_up_timeout_sec` to `max(300, build_timeout_sec // 2)`.

### Lazy, gated repository creation

Artifact Registry repositories are touched **only when an image must actually be built**:

1. **Tasks with a prebuilt `docker_image`** (and no `--force-build`) never inspect or create an Artifact Registry repository — the kubelet pulls the image directly.
2. **Tasks that require a build** first check `check_image_exists_in_registry()` (coalesced per image URL via an in-process cache and lock). On a cache hit, the plugin's `build_task_image_if_missing()` and the runtime's `_image_exists()` both return without touching repository administration or Cloud Build. Inline runtime builds don't call `build_task_image_if_missing()`; on a cache miss they call `submit_cloud_build()` directly.
3. **Only on a cache miss** (or `--force-build`) does `submit_cloud_build()` call `ensure_artifact_registry_exists()`. Repository existence checks and creation (`gcloud artifacts repositories describe` followed by `gcloud artifacts repositories create --repository-format=docker`) are gated and coalesced per `(project_id, location, repository)` tuple via `_REPO_EXISTS_CACHE` and `_REPO_ENSURE_TASKS` so concurrent builds never race.
4. Both `CloudBuildPlugin` and inline runtime builds pass `reraise=True`: if the repository does not exist and creation fails (for example, missing `roles/artifactregistry.admin`), the image build fails immediately with the underlying error rather than proceeding to a guaranteed push failure.

## The ImageResolver invariant

For Compose Pods (built by `translate_compose()`), the module enforces a strict invariant: every container image string placed on the Pod's `initContainers`, `containers`, or `ephemeralContainers` must be issued by an `ImageResolver` instance. Single-container direct Pods don't go through `ImageResolver` and aren't checked.

Before a Compose Pod is created, `assert_pod_images_resolved()` verifies this condition and raises an `UnresolvedPodImageError` if an unregistered image string appears in the Pod specification. The resolver issues an `ImageRef` and classifies the image's `ImageOrigin` (such as `MAIN_BUILT`, `SIDECAR_BUILT`, `SIDECAR_EXTERNAL`, or `INFRA`).

For Compose Pods, this single chokepoint guarantees that all images are accounted for, audited for Image Streaming eligibility (`streaming_report()`), and ready for prefix rewriting when pull-through caching is enabled.

## GKE Image Streaming and external registries

Google Kubernetes Engine (GKE) Image Streaming accelerates Pod startup by mounting container layers over the network rather than downloading the full image tarball before container start. GKE Image Streaming supports:

- Google Artifact Registry (`*.pkg.dev`, both standard and remote repositories)
- Container Registry (`gcr.io`)
- Public Docker Hub (`docker.io`)

Task images built by `harbor-gke-ext` land in Artifact Registry and are always eligible for Image Streaming. Externally supplied images on `public.ecr.aws`, `ghcr.io`, or `mcr.microsoft.com` are pulled directly by the kubelet (or by the inner `dockerd` in DinD shapes) from their upstream registries.

Compose placement does not change which images are built. Both Shape B (hybrid DinD) and Shape C (main-in-DinD) replace each `build:` service with a pre-built Artifact Registry `image:` URL that `dind-cache-<svc>` pulls into `dind-engine`'s disk-backed `/var/lib/docker` (`harbor-dind-storage`, `overlay2`) via `docker pull` (with `tar -cf - / | docker import --change ...` as fallback).

### Roadmap: Pull-through caching via Artifact Registry remote repositories

Automatic mirroring of external images (`public.ecr.aws`, `ghcr.io`, `mcr.microsoft.com`) is planned as a first-class pull-through cache feature backed by **Artifact Registry remote repositories** (`--mode=remote-repository`) rather than synthetic Cloud Build rebuilds:

- **Zero-build manifest preservation**: Artifact Registry remote repositories act as a pull-through proxy that preserves upstream multi-arch manifest lists and exact `sha256` digests without running Cloud Build jobs.
- **Full GKE Image Streaming compatibility**: GKE Image Streaming natively supports Artifact Registry remote repositories, bringing network-streamed layer mounts to `public.ecr.aws` and `ghcr.io` images after the first pull.
- **Single-chokepoint rewriting in `ImageResolver`**: When upstream remote repositories are configured on the cluster/project, `ImageResolver.resolve()` will rewrite eligible external references (`<region>-docker.pkg.dev/<project>/<remote-repo>/<upstream-path>@sha256:...`) across `task.toml` `docker_image`, `Dockerfile FROM` lines, and Compose service `image:` entries alike.

For related setup and operational details, see the [Configuration reference](configuration.md), [Architecture](architecture.md), [Docker in Docker](docker-in-docker.md), [Cluster setup](cluster-setup.md), and [Troubleshooting](troubleshooting.md).
