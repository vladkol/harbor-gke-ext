from __future__ import annotations

import asyncio
import ipaddress
from collections.abc import Sequence
from typing import Any

from harbor.models.task.config import NetworkMode, NetworkPolicy
from harbor.utils.logger import logger
from harbor_gke_ext.constants import _sanitize_kubernetes_resource_name

try:
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    _HAS_KUBERNETES = True
except ImportError:
    _HAS_KUBERNETES = False


METADATA_SERVER_CIDR = "169.254.169.254/32"
GKE_METADATA_PROXY_CIDR = "169.254.169.252/32"
METADATA_IPV4_CIDRS: tuple[str, ...] = (
    METADATA_SERVER_CIDR,
    GKE_METADATA_PROXY_CIDR,
)


def _normalize_cidr(entry: str) -> str:
    """Normalize an IP address or CIDR string into canonical CIDR notation."""
    text = entry.strip()
    if "/" in text:
        return str(ipaddress.ip_network(text, strict=False))
    addr = ipaddress.ip_address(text)
    prefix = 32 if addr.version == 4 else 128
    return f"{addr}/{prefix}"


async def _delete_fqdn_network_policy(
    custom_api: k8s_client.CustomObjectsApi | None,
    namespace: str,
    fqdn_policy_name: str,
) -> None:
    """Delete a GKE FQDNNetworkPolicy custom object if it exists."""
    if custom_api is None:
        return
    try:
        await asyncio.to_thread(
            custom_api.delete_namespaced_custom_object,
            group="networking.gke.io",
            version="v1alpha1",
            namespace=namespace,
            plural="fqdnnetworkpolicies",
            name=fqdn_policy_name,
        )
    except ApiException as e:
        if e.status != 404:
            logger.debug(f"Failed to delete FQDNNetworkPolicy {fqdn_policy_name}: {e}")
    except Exception as e:
        logger.debug(f"Error deleting FQDNNetworkPolicy {fqdn_policy_name}: {e}")


async def apply_network_policy(
    networking_api: k8s_client.NetworkingV1Api,
    custom_api: k8s_client.CustomObjectsApi | None,
    namespace: str,
    pod_name: str,
    session_id: str,
    network_policy: NetworkPolicy,
    fqdn_supported: bool = False,
    allow_metadata_server: bool | None = False,
    pod_uid: str | None = None,
    pod_labels: dict[str, str] | None = None,
    policy_key: str | None = None,
    dns_egress_extra_cidrs: Sequence[str] | None = None,
    allow_pod_ingress: bool = False,
    kube_dns_cluster_ip: str | None = None,
) -> None:
    """Apply network policy using native Kubernetes NetworkPolicy and GKE FQDNNetworkPolicy.

    ``policy_key`` names the policy objects and must stay constant for the whole
    trial. Pass the Job name: it is assigned before the Pod exists and never
    changes. ``pod_name`` and ``pod_uid`` identify the Pod and are used only to
    build ownerReferences, which is why they must not name the objects -- the
    first call happens before the Pod exists, so a pod-derived name would create
    a second policy later instead of replacing the first. Two policies with the
    same podSelector union their rules, which makes narrowing egress mid-trial
    silently ineffective and leaks the first object past teardown.
    """
    naming_key = policy_key or pod_name
    session_label = (pod_labels or {}).get("session") or naming_key
    policy_name = _sanitize_kubernetes_resource_name(f"harbor-netpol-{naming_key}")
    fqdn_policy_name = _sanitize_kubernetes_resource_name(f"harbor-fqdn-{naming_key}")

    owner_references = (
        [
            k8s_client.V1OwnerReference(
                api_version="v1",
                kind="Pod",
                name=pod_name,
                uid=pod_uid,
                block_owner_deletion=True,
                controller=True,
            )
        ]
        if pod_uid
        else None
    )

    async def _create_or_replace_egress_policy(
        egress_rules: list[k8s_client.V1NetworkPolicyEgressRule],
    ) -> None:
        if allow_pod_ingress:
            policy_types = ["Egress"]
            ingress_rules: list[k8s_client.V1NetworkPolicyIngressRule] | None = None
        else:
            policy_types = ["Ingress", "Egress"]
            ingress_rules = []

        body = k8s_client.V1NetworkPolicy(
            metadata=k8s_client.V1ObjectMeta(
                name=policy_name,
                namespace=namespace,
                labels={
                    "harbor.eval/session": session_id,
                    "session": session_label,
                },
                owner_references=owner_references,
            ),
            spec=k8s_client.V1NetworkPolicySpec(
                pod_selector=k8s_client.V1LabelSelector(
                    match_labels={"session": session_label}
                ),
                policy_types=policy_types,
                ingress=ingress_rules,
                egress=egress_rules,
            ),
        )
        try:
            await asyncio.to_thread(
                networking_api.create_namespaced_network_policy,
                namespace=namespace,
                body=body,
            )
        except ApiException as e:
            if e.status != 409:
                raise
            existing = await asyncio.to_thread(
                networking_api.read_namespaced_network_policy,
                name=policy_name,
                namespace=namespace,
            )
            resource_version = getattr(
                getattr(existing, "metadata", None), "resource_version", None
            )
            if isinstance(resource_version, str) and resource_version:
                body.metadata.resource_version = resource_version
            await asyncio.to_thread(
                networking_api.replace_namespaced_network_policy,
                name=policy_name,
                namespace=namespace,
                body=body,
            )

    async def _create_or_replace_fqdn_policy(fqdn_body: dict[str, Any]) -> None:
        if custom_api is None:
            return
        try:
            await asyncio.to_thread(
                custom_api.create_namespaced_custom_object,
                group="networking.gke.io",
                version="v1alpha1",
                namespace=namespace,
                plural="fqdnnetworkpolicies",
                body=fqdn_body,
            )
        except ApiException as e:
            if e.status != 409:
                raise
            existing = await asyncio.to_thread(
                custom_api.get_namespaced_custom_object,
                group="networking.gke.io",
                version="v1alpha1",
                namespace=namespace,
                plural="fqdnnetworkpolicies",
                name=fqdn_policy_name,
            )
            resource_version = existing.get("metadata", {}).get("resourceVersion")
            if resource_version:
                fqdn_body.setdefault("metadata", {})["resourceVersion"] = (
                    resource_version
                )
            await asyncio.to_thread(
                custom_api.replace_namespaced_custom_object,
                group="networking.gke.io",
                version="v1alpha1",
                namespace=namespace,
                plural="fqdnnetworkpolicies",
                name=fqdn_policy_name,
                body=fqdn_body,
            )

    async def _delete_fqdn_policy() -> None:
        await _delete_fqdn_network_policy(custom_api, namespace, fqdn_policy_name)

    def _dns_egress_rule() -> k8s_client.V1NetworkPolicyEgressRule:
        ports = [
            k8s_client.V1NetworkPolicyPort(protocol="UDP", port=53),
            k8s_client.V1NetworkPolicyPort(protocol="TCP", port=53),
        ]
        dns_cidrs: list[str] = [
            "169.254.20.10/32",  # GKE NodeLocal DNSCache link-local IP
            "169.254.169.254/32",  # GKE Cloud DNS / metadata DNS resolver (port 53 only)
        ]
        if kube_dns_cluster_ip and kube_dns_cluster_ip.strip():
            norm_dns_ip = _normalize_cidr(kube_dns_cluster_ip)
            if norm_dns_ip not in dns_cidrs:
                dns_cidrs.append(norm_dns_ip)
        for raw_extra in dns_egress_extra_cidrs or ():
            if not raw_extra or not raw_extra.strip():
                continue
            norm_cidr = _normalize_cidr(raw_extra)
            if norm_cidr not in dns_cidrs:
                dns_cidrs.append(norm_cidr)

        peers: list[k8s_client.V1NetworkPolicyPeer] = [
            k8s_client.V1NetworkPolicyPeer(
                namespace_selector=k8s_client.V1LabelSelector(
                    match_labels={"kubernetes.io/metadata.name": "kube-system"}
                ),
                pod_selector=k8s_client.V1LabelSelector(
                    match_expressions=[
                        k8s_client.V1LabelSelectorRequirement(
                            key="k8s-app",
                            operator="In",
                            values=["kube-dns", "node-local-dns"],
                        )
                    ]
                ),
            ),
            *[
                k8s_client.V1NetworkPolicyPeer(ip_block=k8s_client.V1IPBlock(cidr=cidr))
                for cidr in dns_cidrs
            ],
        ]
        return k8s_client.V1NetworkPolicyEgressRule(to=peers, ports=ports)

    match network_policy.network_mode:
        case NetworkMode.NO_NETWORK:
            await _create_or_replace_egress_policy([])
            await _delete_fqdn_policy()

        case NetworkMode.ALLOWLIST:
            hostnames: list[str] = []
            cidrs: list[str] = []
            for host in network_policy.allowed_hosts:
                if "/" in host:
                    cidrs.append(host)
                else:
                    try:
                        ipaddress.ip_address(host)
                        cidrs.append(_normalize_cidr(host))
                    except ValueError:
                        hostnames.append(host)

            if hostnames and not fqdn_supported:
                raise RuntimeError(
                    "Task requires hostname-based network allowlisting, but the target GKE cluster "
                    "does not support FQDNNetworkPolicy (GKE Datapath V2 / Cilium is required)."
                )

            if hostnames and fqdn_supported and custom_api:
                matches = [
                    {"pattern": host} if "*" in host else {"name": host}
                    for host in hostnames
                ]
                fqdn_body = {
                    "apiVersion": "networking.gke.io/v1alpha1",
                    "kind": "FQDNNetworkPolicy",
                    "metadata": {
                        "name": fqdn_policy_name,
                        "namespace": namespace,
                        "labels": {
                            "harbor.eval/session": session_id,
                            "session": session_label,
                        },
                        **(
                            {
                                "ownerReferences": [
                                    {
                                        "apiVersion": "v1",
                                        "kind": "Pod",
                                        "name": pod_name,
                                        "uid": pod_uid,
                                        "blockOwnerDeletion": True,
                                        "controller": True,
                                    }
                                ]
                            }
                            if pod_uid
                            else {}
                        ),
                    },
                    "spec": {
                        "podSelector": {"matchLabels": {"session": session_label}},
                        "egress": [{"matches": matches}],
                    },
                }
                await _create_or_replace_fqdn_policy(fqdn_body)
            else:
                await _delete_fqdn_policy()

            egress_rules: list[k8s_client.V1NetworkPolicyEgressRule] = []
            if cidrs:
                egress_rules.append(
                    k8s_client.V1NetworkPolicyEgressRule(
                        to=[
                            k8s_client.V1NetworkPolicyPeer(
                                ip_block=k8s_client.V1IPBlock(cidr=cidr)
                            )
                            for cidr in cidrs
                        ]
                    )
                )

            if allow_metadata_server:
                egress_rules.append(
                    k8s_client.V1NetworkPolicyEgressRule(
                        to=[
                            k8s_client.V1NetworkPolicyPeer(
                                ip_block=k8s_client.V1IPBlock(cidr=METADATA_SERVER_CIDR)
                            )
                        ],
                        ports=[
                            k8s_client.V1NetworkPolicyPort(protocol="TCP", port=80),
                            k8s_client.V1NetworkPolicyPort(protocol="TCP", port=8080),
                        ],
                    )
                )
                egress_rules.append(
                    k8s_client.V1NetworkPolicyEgressRule(
                        to=[
                            k8s_client.V1NetworkPolicyPeer(
                                ip_block=k8s_client.V1IPBlock(
                                    cidr=GKE_METADATA_PROXY_CIDR
                                )
                            )
                        ],
                        ports=[
                            k8s_client.V1NetworkPolicyPort(protocol="TCP", port=988),
                            k8s_client.V1NetworkPolicyPort(protocol="TCP", port=987),
                        ],
                    )
                )

            if egress_rules or hostnames:
                egress_rules.append(_dns_egress_rule())

            # Unconditionally create or replace V1NetworkPolicy so an empty allowlist fails closed (egress=[]).
            await _create_or_replace_egress_policy(egress_rules)

        case NetworkMode.PUBLIC:
            if not allow_metadata_server:
                await _create_or_replace_egress_policy(
                    [
                        k8s_client.V1NetworkPolicyEgressRule(
                            to=[
                                k8s_client.V1NetworkPolicyPeer(
                                    ip_block=k8s_client.V1IPBlock(
                                        cidr="0.0.0.0/0",
                                        _except=list(METADATA_IPV4_CIDRS),
                                    )
                                ),
                            ]
                        ),
                        _dns_egress_rule(),
                    ]
                )
                await _delete_fqdn_policy()
            else:
                await delete_network_policies(
                    networking_api=networking_api,
                    custom_api=custom_api,
                    namespace=namespace,
                    pod_name=naming_key,
                )


async def delete_network_policies(
    networking_api: k8s_client.NetworkingV1Api | None,
    custom_api: k8s_client.CustomObjectsApi | None,
    namespace: str,
    pod_name: str,
) -> None:
    """Delete network policies associated with the given pod session."""
    policy_name = _sanitize_kubernetes_resource_name(f"harbor-netpol-{pod_name}")
    fqdn_policy_name = _sanitize_kubernetes_resource_name(f"harbor-fqdn-{pod_name}")

    if networking_api is not None:
        try:
            await asyncio.to_thread(
                networking_api.delete_namespaced_network_policy,
                name=policy_name,
                namespace=namespace,
            )
        except ApiException as e:
            if e.status != 404:
                logger.debug(f"Failed to delete NetworkPolicy {policy_name}: {e}")
        except Exception as e:
            logger.debug(f"Error deleting NetworkPolicy {policy_name}: {e}")

    await _delete_fqdn_network_policy(custom_api, namespace, fqdn_policy_name)
