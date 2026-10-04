from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from harbor.models.task.config import NetworkMode, NetworkPolicy
from harbor_gke_ext.network_policy import (
    apply_network_policy,
    delete_network_policies,
)


@pytest.mark.unit
@pytest.mark.asyncio
class TestApplyNetworkPolicyNoNetwork:
    @pytest.mark.parametrize(
        "mode,allowed_hosts",
        [
            (NetworkMode.NO_NETWORK, []),
            (NetworkMode.ALLOWLIST, ["10.0.0.1"]),
            (NetworkMode.PUBLIC, []),
        ],
    )
    async def test_egress_policy_conflict_replaces_existing_and_error_handling(
        self, mode, allowed_hosts
    ):
        networking_api = MagicMock()
        existing_netpol = MagicMock()
        existing_netpol.metadata.resource_version = "rv-123"
        networking_api.read_namespaced_network_policy.return_value = existing_netpol

        policy = NetworkPolicy(network_mode=mode, allowed_hosts=allowed_hosts)

        networking_api.create_namespaced_network_policy.side_effect = ApiException(
            status=409, reason="Conflict"
        )
        await apply_network_policy(
            networking_api=networking_api,
            custom_api=None,
            namespace="default",
            pod_name="test-pod",
            session_id="session-1",
            network_policy=policy,
        )

        networking_api.read_namespaced_network_policy.assert_called_once_with(
            name="harbor-netpol-test-pod",
            namespace="default",
        )
        networking_api.replace_namespaced_network_policy.assert_called_once()
        replaced_body = (
            networking_api.replace_namespaced_network_policy.call_args.kwargs["body"]
        )
        assert replaced_body.metadata.resource_version == "rv-123"

        networking_api.create_namespaced_network_policy.side_effect = ApiException(
            status=500, reason="Internal Server Error"
        )
        with pytest.raises(ApiException) as exc_info:
            await apply_network_policy(
                networking_api=networking_api,
                custom_api=None,
                namespace="default",
                pod_name="test-pod",
                session_id="session-1",
                network_policy=policy,
            )
        assert exc_info.value.status == 500


@pytest.mark.unit
@pytest.mark.asyncio
class TestApplyNetworkPolicyAllowlist:
    async def test_hostname_without_fqdn_supported_raises_runtime_error(self):
        networking_api = MagicMock()
        policy = NetworkPolicy(
            network_mode=NetworkMode.ALLOWLIST,
            allowed_hosts=["api.github.com"],
        )

        with pytest.raises(RuntimeError, match="does not support FQDNNetworkPolicy"):
            await apply_network_policy(
                networking_api=networking_api,
                custom_api=None,
                namespace="default",
                pod_name="test-pod",
                session_id="session-1",
                network_policy=policy,
                fqdn_supported=False,
            )

    async def test_fqdn_handles_conflict_by_replacing_and_other_error_raises(self):
        networking_api = MagicMock()
        custom_api = MagicMock()
        custom_api.create_namespaced_custom_object.side_effect = ApiException(
            status=409
        )
        custom_api.get_namespaced_custom_object.return_value = {
            "metadata": {"resourceVersion": "fqdn-rv-456"}
        }
        policy = NetworkPolicy(
            network_mode=NetworkMode.ALLOWLIST,
            allowed_hosts=["api.github.com"],
        )

        # 409 should read resourceVersion and replace
        await apply_network_policy(
            networking_api=networking_api,
            custom_api=custom_api,
            namespace="default",
            pod_name="test-pod",
            session_id="session-1",
            network_policy=policy,
            fqdn_supported=True,
        )
        custom_api.get_namespaced_custom_object.assert_called_once_with(
            group="networking.gke.io",
            version="v1alpha1",
            namespace="default",
            plural="fqdnnetworkpolicies",
            name="harbor-fqdn-test-pod",
        )
        custom_api.replace_namespaced_custom_object.assert_called_once()
        replaced_fqdn = custom_api.replace_namespaced_custom_object.call_args.kwargs[
            "body"
        ]
        assert replaced_fqdn["metadata"]["resourceVersion"] == "fqdn-rv-456"

        # 500 should raise
        custom_api.create_namespaced_custom_object.side_effect = ApiException(
            status=500
        )
        with pytest.raises(ApiException):
            await apply_network_policy(
                networking_api=networking_api,
                custom_api=custom_api,
                namespace="default",
                pod_name="test-pod",
                session_id="session-1",
                network_policy=policy,
                fqdn_supported=True,
            )

    async def test_cidrs_and_single_ips_and_metadata_server(self):
        networking_api = MagicMock()
        policy = NetworkPolicy(
            network_mode=NetworkMode.ALLOWLIST,
            allowed_hosts=[
                "192.168.1.0/24",
                "10.0.0.1",
                "2001:db8::1",
                "storage.googleapis.com",
            ],
        )

        await apply_network_policy(
            networking_api=networking_api,
            custom_api=MagicMock(),
            namespace="default",
            pod_name="test-pod",
            session_id="session-1",
            network_policy=policy,
            fqdn_supported=True,
            allow_metadata_server=True,
        )

        netpol_body = networking_api.create_namespaced_network_policy.call_args.kwargs[
            "body"
        ]
        # Should have 4 egress rules:
        # 1. CIDRs ("192.168.1.0/24", "10.0.0.1/32", and "2001:db8::1/128")
        # 2. GCE Metadata Server ("169.254.169.254/32" ports 80, 8080)
        # 3. GKE Metadata Proxy ("169.254.169.252/32" ports 988, 987)
        # 4. DNS (port 53)
        assert len(netpol_body.spec.egress) == 4

        cidr_rule = netpol_body.spec.egress[0]
        cidrs = [peer.ip_block.cidr for peer in cidr_rule.to]
        assert "192.168.1.0/24" in cidrs
        assert "10.0.0.1/32" in cidrs
        assert "2001:db8::1/128" in cidrs

        metadata_rule = netpol_body.spec.egress[1]
        assert metadata_rule.to[0].ip_block.cidr == "169.254.169.254/32"
        assert [p.port for p in metadata_rule.ports] == [80, 8080]
        assert all(p.protocol == "TCP" for p in metadata_rule.ports)

        proxy_rule = netpol_body.spec.egress[2]
        assert proxy_rule.to[0].ip_block.cidr == "169.254.169.252/32"
        assert [p.port for p in proxy_rule.ports] == [988, 987]
        assert all(p.protocol == "TCP" for p in proxy_rule.ports)

        dns_rule = netpol_body.spec.egress[3]
        assert all(p.port == 53 for p in dns_rule.ports)


@pytest.mark.unit
@pytest.mark.asyncio
class TestDynamicNetworkPolicyTransitions:
    async def test_public_to_allowlist_replaces_netpol_and_creates_fqdn(self):
        """Regression test for e-ve-sa-sv-diff: transitioning from PUBLIC (ve) to
        ALLOWLIST (sv) must replace the 0.0.0.0/0 V1NetworkPolicy with DNS-only egress
        instead of swallowing HTTP 409 Conflict."""
        networking_api = MagicMock()
        custom_api = MagicMock()

        # Step 1: Start with PUBLIC (allow_metadata_server=False creates 0.0.0.0/0 netpol)
        await apply_network_policy(
            networking_api=networking_api,
            custom_api=custom_api,
            namespace="default",
            pod_name="verifier-pod",
            session_id="session-1",
            network_policy=NetworkPolicy(network_mode=NetworkMode.PUBLIC),
            fqdn_supported=True,
            allow_metadata_server=False,
        )
        initial_body = networking_api.create_namespaced_network_policy.call_args.kwargs[
            "body"
        ]
        assert len(initial_body.spec.egress) == 2
        assert initial_body.spec.egress[0].to[0].ip_block.cidr == "0.0.0.0/0"

        # Step 2: Transition to ALLOWLIST(["www.iana.org"]); create raises 409 Conflict
        networking_api.create_namespaced_network_policy.side_effect = ApiException(
            status=409, reason="Conflict"
        )
        existing_obj = MagicMock()
        existing_obj.metadata.resource_version = "rv-public-1"
        networking_api.read_namespaced_network_policy.return_value = existing_obj

        await apply_network_policy(
            networking_api=networking_api,
            custom_api=custom_api,
            namespace="default",
            pod_name="verifier-pod",
            session_id="session-1",
            network_policy=NetworkPolicy(
                network_mode=NetworkMode.ALLOWLIST,
                allowed_hosts=["www.iana.org"],
            ),
            fqdn_supported=True,
            allow_metadata_server=False,
        )

        # Verify FQDNNetworkPolicy was created for www.iana.org
        custom_api.create_namespaced_custom_object.assert_called_once()
        fqdn_body = custom_api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert fqdn_body["spec"]["egress"] == [{"matches": [{"name": "www.iana.org"}]}]

        # Verify V1NetworkPolicy was replaced with DNS-only egress (no 0.0.0.0/0!)
        networking_api.replace_namespaced_network_policy.assert_called_once()
        replaced_body = (
            networking_api.replace_namespaced_network_policy.call_args.kwargs["body"]
        )
        assert replaced_body.metadata.resource_version == "rv-public-1"
        assert len(replaced_body.spec.egress) == 1
        assert all(p.port == 53 for p in replaced_body.spec.egress[0].ports)
        assert all(
            peer.ip_block.cidr != "0.0.0.0/0"
            for peer in replaced_body.spec.egress[0].to
            if peer.ip_block
        )

    async def test_allowlist_to_no_network_replaces_netpol_and_deletes_fqdn(self):
        networking_api = MagicMock()
        custom_api = MagicMock()
        networking_api.create_namespaced_network_policy.side_effect = ApiException(
            status=409, reason="Conflict"
        )

        await apply_network_policy(
            networking_api=networking_api,
            custom_api=custom_api,
            namespace="default",
            pod_name="test-pod",
            session_id="session-1",
            network_policy=NetworkPolicy(network_mode=NetworkMode.NO_NETWORK),
            fqdn_supported=True,
        )

        networking_api.replace_namespaced_network_policy.assert_called_once()
        replaced_body = (
            networking_api.replace_namespaced_network_policy.call_args.kwargs["body"]
        )
        assert replaced_body.spec.egress == []
        custom_api.delete_namespaced_custom_object.assert_called_once_with(
            group="networking.gke.io",
            version="v1alpha1",
            namespace="default",
            plural="fqdnnetworkpolicies",
            name="harbor-fqdn-test-pod",
        )

    async def test_allowlist_without_hostnames_deletes_stale_fqdn_policy(self):
        networking_api = MagicMock()
        custom_api = MagicMock()

        await apply_network_policy(
            networking_api=networking_api,
            custom_api=custom_api,
            namespace="default",
            pod_name="test-pod",
            session_id="session-1",
            network_policy=NetworkPolicy(
                network_mode=NetworkMode.ALLOWLIST,
                allowed_hosts=["10.0.0.1/32"],
            ),
            fqdn_supported=True,
        )

        custom_api.delete_namespaced_custom_object.assert_called_once_with(
            group="networking.gke.io",
            version="v1alpha1",
            namespace="default",
            plural="fqdnnetworkpolicies",
            name="harbor-fqdn-test-pod",
        )

    async def test_public_with_allow_metadata_server_deletes_all_policies(self):
        networking_api = MagicMock()
        custom_api = MagicMock()

        await apply_network_policy(
            networking_api=networking_api,
            custom_api=custom_api,
            namespace="default",
            pod_name="test-pod",
            session_id="session-1",
            network_policy=NetworkPolicy(network_mode=NetworkMode.PUBLIC),
            fqdn_supported=True,
            allow_metadata_server=True,
        )

        networking_api.delete_namespaced_network_policy.assert_called_once()
        custom_api.delete_namespaced_custom_object.assert_called_once()


@pytest.mark.unit
@pytest.mark.asyncio
class TestDeleteNetworkPolicies:
    @pytest.mark.parametrize(
        "exc",
        [
            ApiException(status=404),
            ApiException(status=500),
            RuntimeError("network error"),
        ],
    )
    async def test_delete_handles_exceptions_gracefully(self, exc):
        networking_api = MagicMock()
        custom_api = MagicMock()
        networking_api.delete_namespaced_network_policy.side_effect = exc
        custom_api.delete_namespaced_custom_object.side_effect = exc

        await delete_network_policies(
            networking_api=networking_api,
            custom_api=custom_api,
            namespace="default",
            pod_name="test-pod",
        )


@pytest.mark.unit
@pytest.mark.asyncio
class TestGKEEnvironmentApplyNetworkPolicy:
    async def test_apply_network_policy_settlement_when_pod_ready(self, tmp_path):
        from harbor.models.task.config import EnvironmentConfig
        from harbor.models.trial.paths import TrialPaths
        from harbor_gke_ext.environment import GKEEnvironment

        env = GKEEnvironment(
            environment_dir=tmp_path,
            environment_name="test-env",
            session_id="test-session",
            trial_paths=TrialPaths(trial_dir=tmp_path),
            task_env_config=EnvironmentConfig(docker_image="python:3.12"),
            cluster_name="test-cluster",
            project_id="test-project",
            region="us-central1",
            network_policy_settlement_sec=0.25,
        )
        env._ensure_client = AsyncMock()
        env._networking_api = MagicMock()
        env._custom_api = MagicMock()
        env.pod = MagicMock()
        env.pod.metadata.uid = "uid-1"
        env.pod.metadata.labels = {"session": "test-pod"}

        with (
            patch(
                "harbor_gke_ext.environment.apply_network_policy",
                new_callable=AsyncMock,
            ) as mock_apply,
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            env._pod_ready = False
            await env._apply_network_policy(
                NetworkPolicy(network_mode=NetworkMode.NO_NETWORK)
            )
            mock_apply.assert_awaited_once()
            mock_sleep.assert_not_awaited()

            mock_apply.reset_mock()
            env._pod_ready = True
            await env._apply_network_policy(
                NetworkPolicy(network_mode=NetworkMode.PUBLIC)
            )
            mock_apply.assert_awaited_once()
            mock_sleep.assert_awaited_once_with(0.25)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_apply_network_policy_uses_policy_key_for_names_and_pod_name_for_owner_ref():
    from harbor_gke_ext.network_policy import apply_network_policy

    networking_api = MagicMock()
    custom_api = MagicMock()
    created_policies = []
    networking_api.create_namespaced_network_policy.side_effect = (
        lambda namespace, body: created_policies.append(body)
    )

    await apply_network_policy(
        networking_api=networking_api,
        custom_api=custom_api,
        namespace="default",
        session_id="sess-1",
        pod_name="job-abc-pod-9f2x1",
        pod_uid="uid-123",
        pod_labels={"session": "job-abc"},
        network_policy=NetworkPolicy(network_mode=NetworkMode.NO_NETWORK),
        policy_key="job-abc",
    )

    assert len(created_policies) == 1
    pol = created_policies[0]
    # Object name uses stable policy_key
    assert pol.metadata.name == "harbor-netpol-job-abc"
    # Owner reference binds GC to the actual Pod
    assert pol.metadata.owner_references[0].name == "job-abc-pod-9f2x1"
    assert pol.metadata.owner_references[0].uid == "uid-123"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dns_egress_rule_default_and_extra_cidrs():
    from harbor_gke_ext.network_policy import apply_network_policy

    async def _run_policy(extra_cidrs=None, kube_dns_cluster_ip=None):
        networking_api = MagicMock()
        custom_api = MagicMock()
        created = []
        networking_api.create_namespaced_network_policy.side_effect = (
            lambda namespace, body: created.append(body)
        )
        await apply_network_policy(
            networking_api=networking_api,
            custom_api=custom_api,
            namespace="default",
            session_id="sess-1",
            pod_name="job-abc-pod",
            pod_uid="uid-1",
            pod_labels={"session": "job-abc"},
            network_policy=NetworkPolicy(
                network_mode=NetworkMode.ALLOWLIST,
                allowed_hosts=["1.1.1.1/32"],
            ),
            policy_key="job-abc",
            dns_egress_extra_cidrs=extra_cidrs,
            kube_dns_cluster_ip=kube_dns_cluster_ip,
        )
        return created[0].spec.egress[-1]

    # WP-5 (F-09): Default opens only kube-system DNS pods + link-local DNS + discovered kube-dns ClusterIP,
    # NEVER broad RFC1918/CGNAT ranges (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 169.254.0.0/16, 100.64.0.0/10, 34.118.224.0/20).
    rule = await _run_policy(kube_dns_cluster_ip="10.96.0.10")
    assert rule.to[0].namespace_selector.match_labels == {
        "kubernetes.io/metadata.name": "kube-system"
    }
    assert rule.to[0].pod_selector is not None
    expr = rule.to[0].pod_selector.match_expressions[0]
    assert expr.key == "k8s-app"
    assert expr.operator == "In"
    assert set(expr.values) == {"kube-dns", "node-local-dns"}

    cidrs = {peer.ip_block.cidr for peer in rule.to if peer.ip_block}
    assert cidrs == {
        "169.254.20.10/32",
        "169.254.169.254/32",
        "10.96.0.10/32",
    }
    for forbidden in (
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "169.254.0.0/16",
        "100.64.0.0/10",
        "34.118.224.0/20",
        "8.8.8.8/32",
        "8.8.4.4/32",
    ):
        assert forbidden not in cidrs
    assert all(p.port == 53 for p in rule.ports)

    r_extra = await _run_policy(
        extra_cidrs=["198.18.0.10/32"],
        kube_dns_cluster_ip="34.118.224.10",
    )
    extra_cidrs = {peer.ip_block.cidr for peer in r_extra.to if peer.ip_block}
    assert extra_cidrs == {
        "169.254.20.10/32",
        "169.254.169.254/32",
        "34.118.224.10/32",
        "198.18.0.10/32",
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_public_metadata_block_and_pod_ingress_isolation():
    networking_api = MagicMock()
    created = []
    networking_api.create_namespaced_network_policy.side_effect = (
        lambda namespace, body: created.append(body)
    )

    # 1. Default (allow_metadata_server=False, allow_pod_ingress=False)
    await apply_network_policy(
        networking_api=networking_api,
        custom_api=None,
        namespace="default",
        session_id="sess-1",
        pod_name="job-abc-pod",
        network_policy=NetworkPolicy(network_mode=NetworkMode.PUBLIC),
        allow_metadata_server=False,
        allow_pod_ingress=False,
    )
    pol = created[-1]
    assert pol.spec.policy_types == ["Ingress", "Egress"]
    assert pol.spec.ingress == []
    assert len(pol.spec.egress[0].to) == 1
    assert pol.spec.egress[0].to[0].ip_block.cidr == "0.0.0.0/0"
    assert pol.spec.egress[0].to[0].ip_block._except == [
        "169.254.169.254/32",
        "169.254.169.252/32",
    ]

    # 2. With allow_pod_ingress=True: Egress-only policy
    await apply_network_policy(
        networking_api=networking_api,
        custom_api=None,
        namespace="default",
        session_id="sess-1",
        pod_name="job-abc-pod",
        network_policy=NetworkPolicy(network_mode=NetworkMode.PUBLIC),
        allow_metadata_server=False,
        allow_pod_ingress=True,
    )
    pol_ingress = created[-1]
    assert pol_ingress.spec.policy_types == ["Egress"]
    assert pol_ingress.spec.ingress is None

    # 3. NO_NETWORK with allow_pod_ingress=False: blocks both Ingress and Egress
    await apply_network_policy(
        networking_api=networking_api,
        custom_api=None,
        namespace="default",
        session_id="sess-1",
        pod_name="job-abc-pod",
        network_policy=NetworkPolicy(network_mode=NetworkMode.NO_NETWORK),
        allow_metadata_server=False,
        allow_pod_ingress=False,
    )
    pol_no_net = created[-1]
    assert pol_no_net.spec.policy_types == ["Ingress", "Egress"]
    assert pol_no_net.spec.ingress == []
    assert pol_no_net.spec.egress == []
