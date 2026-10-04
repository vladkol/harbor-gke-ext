"""Unit tests for harbor_gke_ext.image_ref (Commit S0)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from harbor_gke_ext.image_ref import (
    DOCKER_HUB_REGISTRY,
    ImageOrigin,
    ImageRef,
    ImageResolver,
    UnresolvedPodImageError,
    parse_image_ref,
)


@pytest.mark.unit
def test_parse_image_ref_docker_hub_short_names() -> None:
    ref = parse_image_ref("redis")
    assert ref == ImageRef(
        registry=DOCKER_HUB_REGISTRY,
        repository="library/redis",
        tag="latest",
        digest=None,
    )
    assert ref.canonical == "docker.io/library/redis:latest"
    assert ref.streaming_eligible is True
    assert ref.is_digest_pinned is False

    ref_tag = parse_image_ref("postgres:15-alpine")
    assert ref_tag.registry == DOCKER_HUB_REGISTRY
    assert ref_tag.repository == "library/postgres"
    assert ref_tag.tag == "15-alpine"
    assert ref_tag.streaming_eligible is True

    ref_org = parse_image_ref("myorg/myrepo:v1.2")
    assert ref_org.registry == DOCKER_HUB_REGISTRY
    assert ref_org.repository == "myorg/myrepo"
    assert ref_org.tag == "v1.2"
    assert ref_org.streaming_eligible is True


@pytest.mark.unit
def test_parse_image_ref_artifact_registry_and_ecr() -> None:
    ar = parse_image_ref("us-central1-docker.pkg.dev/my-proj/harbor-tasks/main:abc123")
    assert ar.registry == "us-central1-docker.pkg.dev"
    assert ar.repository == "my-proj/harbor-tasks/main"
    assert ar.tag == "abc123"
    assert ar.streaming_eligible is True

    digest = "sha256:" + "a" * 64
    ecr = parse_image_ref(f"public.ecr.aws/k4t1e3r5/harbor-hub@{digest}")
    assert ecr.registry == "public.ecr.aws"
    assert ecr.repository == "k4t1e3r5/harbor-hub"
    assert ecr.tag is None
    assert ecr.digest == digest
    assert ecr.is_digest_pinned is True
    assert ecr.streaming_eligible is False


@pytest.mark.unit
def test_parse_image_ref_localhost_with_port() -> None:
    local = parse_image_ref("localhost:5000/test/img:dev")
    assert local.registry == "localhost:5000"
    assert local.repository == "test/img"
    assert local.tag == "dev"
    assert local.streaming_eligible is False


@pytest.mark.unit
def test_image_resolver_assert_pod_images_resolved() -> None:
    resolver = ImageResolver(project_id="test-proj")
    main_img = resolver.resolve(
        "us-central1-docker.pkg.dev/test-proj/harbor-tasks/main:1",
        origin=ImageOrigin.MAIN_BUILT,
    )
    sidecar_img = resolver.resolve(
        "redis:7-alpine",
        origin=ImageOrigin.SIDECAR_EXTERNAL,
    )

    pod = SimpleNamespace(
        spec=SimpleNamespace(
            init_containers=[SimpleNamespace(name="redis", image=sidecar_img)],
            containers=[SimpleNamespace(name="main", image=main_img)],
            ephemeral_containers=None,
        )
    )

    # All images registered -> passes
    resolver.assert_pod_images_resolved(pod)

    # Inject an un-resolved image -> must raise UnresolvedPodImageError
    bad_pod = SimpleNamespace(
        spec=SimpleNamespace(
            init_containers=[
                SimpleNamespace(name="redis", image=sidecar_img),
                SimpleNamespace(name="rogue", image="docker:28.3.3-dind"),
            ],
            containers=[SimpleNamespace(name="main", image=main_img)],
        )
    )
    with pytest.raises(UnresolvedPodImageError, match="rogue"):
        resolver.assert_pod_images_resolved(bad_pod)


@pytest.mark.unit
def test_image_resolver_streaming_report() -> None:
    resolver = ImageResolver(project_id="test-proj")
    digest = "sha256:" + "b" * 64
    resolver.resolve(
        "us-central1-docker.pkg.dev/test-proj/harbor-tasks/main:1",
        origin=ImageOrigin.MAIN_BUILT,
    )
    resolver.resolve(
        f"public.ecr.aws/k4t1e3r5/harbor-hub@{digest}",
        origin=ImageOrigin.SIDECAR_EXTERNAL,
    )

    report = resolver.streaming_report()
    assert report["total"] == 2
    assert report["streaming_eligible"] == 1
    assert report["streaming_ineligible"] == 1
    assert report["digest_pinned"] == 1
    assert report["ineligible_images"][0]["registry"] == "public.ecr.aws"


@pytest.mark.unit
@pytest.mark.parametrize(
    "malicious_ref",
    [
        "attacker.example:8443#.gcr.io/proj/img:latest",
        "attacker.example?.gcr.io/proj/img:latest",
        "gcr.io@attacker.example/proj/img:latest",
        "us-docker.pkg.dev/proj/img:latest\nAuthorization: foo",
        "us-docker.pkg.dev/proj/img:latest\r\nX-Injected: 1",
        "docker.io/library/../../etc/passwd:latest",
        "docker.io/library/../ubuntu:latest",
        "docker.io/library/ubuntu:latest#fragment",
        "docker.io/library/ubuntu:latest?query=1",
        "docker.io/library/ubuntu@sha256:not-a-valid-hex-digest",
        "localhost:99999/test/img:dev",
        "localhost:0/test/img:dev",
        "-invalid-host.io/repo/img:latest",
    ],
)
def test_parse_image_ref_rejects_malformed_and_url_injection_refs(
    malicious_ref: str,
) -> None:
    with pytest.raises(ValueError):
        parse_image_ref(malicious_ref)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("gcr.io", True),
        ("us.gcr.io", True),
        ("eu.gcr.io", True),
        ("asia.gcr.io", True),
        ("us-central1-docker.pkg.dev", True),
        ("us-docker.pkg.dev", True),
        ("europe-west4-docker.pkg.dev", True),
        ("attacker.example#.gcr.io", False),
        ("attacker.example:8443#.gcr.io", False),
        ("evil.gcr.io", False),
        ("sub.us.gcr.io", False),
        ("foo.bar-docker.pkg.dev", False),
        ("evil.pkg.dev", False),
        ("gcr.io.attacker.example", False),
        ("docker.io", False),
        ("ghcr.io", False),
        ("", False),
    ],
)
def test_is_google_registry_host(host: str, expected: bool) -> None:
    from harbor_gke_ext.image_ref import is_google_registry_host

    assert is_google_registry_host(host) is expected

