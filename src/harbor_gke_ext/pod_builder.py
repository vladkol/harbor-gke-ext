from __future__ import annotations

from typing import TYPE_CHECKING

from harbor_gke_ext.constants import (
    _GKE_JOB_BACKOFF_LIMIT,
    GKE_NVIDIA_LDCONFIG_SNIPPET,
    _sanitize_kubernetes_resource_name,
    resolve_gpu_accelerator_label,
    resolve_tpu_accelerator_label,
)
from harbor.utils.logger import logger

try:
    from kubernetes import client as k8s_client
    from kubernetes.utils import parse_quantity

    _HAS_KUBERNETES = True
except ImportError:
    _HAS_KUBERNETES = False

if TYPE_CHECKING:
    from kubernetes import client as k8s_client
    from harbor.models.task.config import TpuSpec


def build_direct_pod(
    *,
    pod_name: str,
    namespace: str,
    environment_name: str,
    run_id: str | None,
    image_url: str,
    startup_env: dict[str, str],
    cpu_request: str | None = None,
    cpu_limit: str | None = None,
    memory_request: str | None = None,
    memory_limit: str | None = None,
    ephemeral_storage_request: str | None = None,
    machine_type: str | None = None,
    node_pool: str | None = None,
    effective_gpus: int = 0,
    gpu_types: list[str] | None = None,
    tpu: TpuSpec | None = None,
    active_deadline_seconds: int | None = None,
    workdir: str | None = None,
    service_account_name: str | None = None,
    compute_class: str | None = None,
    image_pull_secrets: list[str] | None = None,
    override_entrypoint: bool = False,
    runtime_class_name: str | None = None,
) -> k8s_client.V1Pod:
    """Build a Kubernetes V1Pod specification for direct single-container execution.

    ``budget_limits`` is populated only from explicitly supplied ``cpu_limit``
    and ``memory_limit`` arguments. Under ``GKEEnvironment``'s default
    ``ResourceMode.AUTO`` policy (``_GKE_DEFAULT_RESOURCE_AUTO_MODE =
    ResourceMode.GUARANTEE``), both ``requests`` and ``limits`` equal the
    task's declared ``cpus`` and ``memory_mb`` (matching Docker's capped
    default). Passing ``--cpus request --memory request`` omits the limits so
    a direct task can burst up to node capacity.

    **Where the CPU/memory budget lives.** When both CPU and memory are set
    with ``request == limit`` (a Guaranteed budget, produced by the default
    ``auto`` mode or ``--cpus guarantee --memory guarantee`` without limit
    multipliers), the budget goes on the ``main`` container so kubelet's
    static CPU Manager and Memory Manager can grant exclusive cores. Otherwise
    (when limits are omitted via ``request`` mode or raised via
    ``cpu_limit_multiplier`` / ``memory_limit_multiplier``), the budget goes on
    the Pod (``spec.resources``).
    """
    guaranteed = bool(
        cpu_request
        and memory_request
        and cpu_limit
        and memory_limit
        and parse_quantity(cpu_limit) == parse_quantity(cpu_request)
        and parse_quantity(memory_limit) == parse_quantity(memory_request)
    )

    budget_requests: dict[str, str] = {}
    if cpu_request:
        budget_requests["cpu"] = cpu_request
    if memory_request:
        budget_requests["memory"] = memory_request

    # Intentionally not defaulted to `budget_requests` -- see the docstring.
    budget_limits: dict[str, str] = {}
    if cpu_limit:
        budget_limits["cpu"] = cpu_limit
    if memory_limit:
        budget_limits["memory"] = memory_limit

    pod_resources = (
        k8s_client.V1ResourceRequirements(
            requests=budget_requests or None,
            limits=budget_limits or None,
        )
        if (budget_requests or budget_limits) and not guaranteed
        else None
    )

    requests: dict[str, str] = dict(budget_requests) if guaranteed else {}
    if ephemeral_storage_request:
        requests["ephemeral-storage"] = ephemeral_storage_request

    limits: dict[str, str] = dict(budget_limits) if guaranteed else {}

    node_selector: dict[str, str] = {}
    if machine_type and not compute_class:
        family = machine_type.split("-")[0]
        if family:
            node_selector["cloud.google.com/machine-family"] = family
    if node_pool:
        node_selector["cloud.google.com/gke-nodepool"] = node_pool
    if compute_class:
        node_selector["cloud.google.com/compute-class"] = compute_class

    tolerations: list[k8s_client.V1Toleration] = []

    # GPU configuration
    if effective_gpus > 0:
        gpu_str = str(effective_gpus)
        limits["nvidia.com/gpu"] = gpu_str
        requests["nvidia.com/gpu"] = gpu_str

        tolerations.append(
            k8s_client.V1Toleration(
                key="nvidia.com/gpu",
                operator="Exists",
                effect="NoSchedule",
            )
        )

        if gpu_types and not compute_class:
            if len(gpu_types) > 1:
                logger.debug(
                    "Multiple GPU types specified but GKE pods can only target "
                    "one accelerator type via nodeSelector. Using the first: "
                    f"{gpu_types[0]}"
                )
            node_selector["cloud.google.com/gke-accelerator"] = (
                resolve_gpu_accelerator_label(gpu_types[0])
            )

    # TPU configuration
    if tpu is not None:
        chip_str = str(tpu.chip_count)
        limits["google.com/tpu"] = chip_str
        requests["google.com/tpu"] = chip_str

        tolerations.append(
            k8s_client.V1Toleration(
                key="google.com/tpu",
                operator="Exists",
                effect="NoSchedule",
            )
        )

        node_selector["cloud.google.com/gke-tpu-accelerator"] = (
            resolve_tpu_accelerator_label(tpu.type)
        )
        node_selector["cloud.google.com/gke-tpu-topology"] = tpu.topology

    labels = {
        "app": "sandbox",
        "session": pod_name,
        "environment": _sanitize_kubernetes_resource_name(environment_name),
    }
    if run_id:
        labels["run"] = _sanitize_kubernetes_resource_name(run_id)

    # GPU pods must register the GKE-mounted NVIDIA driver with the dynamic
    # linker before anything runs; the device plugin mounts the libraries but
    # sets no environment, so libcuda.so.1 is otherwise unresolvable. See
    # GKE_NVIDIA_LDCONFIG_SNIPPET for why this is not done with an env var.
    # Non-GPU pods keep the plain exec form.
    if effective_gpus > 0:
        idle_command = f"{GKE_NVIDIA_LDCONFIG_SNIPPET} exec sleep infinity"
        container_command = ["sh", "-c", idle_command] if override_entrypoint else None
        container_args = None if override_entrypoint else ["sh", "-c", idle_command]
    else:
        container_command = ["sleep", "infinity"] if override_entrypoint else None
        container_args = None if override_entrypoint else ["sh", "-c", "sleep infinity"]

    container = k8s_client.V1Container(
        name="main",
        image=image_url,
        working_dir=workdir,
        command=container_command,
        args=container_args,
        stdin=not override_entrypoint,
        tty=not override_entrypoint,
        env=[
            k8s_client.V1EnvVar(name=key, value=value)
            for key, value in startup_env.items()
        ],
        security_context=k8s_client.V1SecurityContext(
            run_as_user=0,
            run_as_group=0,
        ),
        resources=k8s_client.V1ResourceRequirements(
            requests=requests or None,
            limits=limits or None,
        ),
        volume_mounts=[],
    )

    return k8s_client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=k8s_client.V1ObjectMeta(
            name=pod_name,
            namespace=namespace,
            labels=labels,
            annotations={
                "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
            },
        ),
        spec=k8s_client.V1PodSpec(
            active_deadline_seconds=active_deadline_seconds,
            automount_service_account_token=False,
            containers=[container],
            restart_policy="Never",
            runtime_class_name=runtime_class_name,
            service_account_name=service_account_name,
            node_selector=node_selector or None,
            tolerations=tolerations or None,
            image_pull_secrets=[
                k8s_client.V1LocalObjectReference(name=s) for s in image_pull_secrets
            ]
            if image_pull_secrets
            else None,
            resources=pod_resources,
        ),
    )


def build_job(
    job_name: str,
    namespace: str,
    pod_spec: k8s_client.V1PodSpec,
    labels: dict[str, str],
    ttl_seconds_after_finished: int = 120,
    annotations: dict[str, str] | None = None,
) -> k8s_client.V1Job:
    """Wrap a V1PodSpec into a Kubernetes batch/v1 Job with a clean-up TTL.

    The Job, not Harbor, replaces a Pod lost to infrastructure, up to
    `_GKE_JOB_BACKOFF_LIMIT` times. Kubernetes sets `DisruptionTarget` only for
    disruptions the Pod did not cause (preemption, eviction, node loss), never
    for exceeding its own limits. The rules are evaluated in order, and a
    disrupted Pod's containers exit non-zero as well, so the disruption rule
    must come first. Any other container failure fails the Job, as it would
    fail a `docker run`. Harbor follows the replacement only until `start()`
    returns (`GKEEnvironment._follow_job_replacement_pod`).
    """
    active_deadline = getattr(pod_spec, "active_deadline_seconds", None)
    merged_annotations = {
        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
    }
    if annotations:
        merged_annotations.update(annotations)
    return k8s_client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=k8s_client.V1ObjectMeta(
            name=_sanitize_kubernetes_resource_name(job_name),
            namespace=namespace,
            labels=labels,
        ),
        spec=k8s_client.V1JobSpec(
            ttl_seconds_after_finished=ttl_seconds_after_finished,
            active_deadline_seconds=active_deadline,
            backoff_limit=_GKE_JOB_BACKOFF_LIMIT,
            pod_failure_policy=k8s_client.V1PodFailurePolicy(
                rules=[
                    k8s_client.V1PodFailurePolicyRule(
                        action="Count",
                        on_pod_conditions=[
                            k8s_client.V1PodFailurePolicyOnPodConditionsPattern(
                                type="DisruptionTarget", status="True"
                            )
                        ],
                    ),
                    k8s_client.V1PodFailurePolicyRule(
                        action="FailJob",
                        on_exit_codes=k8s_client.V1PodFailurePolicyOnExitCodesRequirement(
                            operator="NotIn", values=[0]
                        ),
                    ),
                ]
            ),
            template=k8s_client.V1PodTemplateSpec(
                metadata=k8s_client.V1ObjectMeta(
                    labels=labels,
                    annotations=merged_annotations,
                ),
                spec=pod_spec,
            ),
        ),
    )
