"""Centralized container image reference parser, streaming eligibility classifier,
and Pod image resolution chokepoint for the GKE environment backend.

Why this module exists
----------------------
Historically, container image strings entered GKE Pod specifications through six
independent code paths (prebuilt main images, Cloud Build main images, built
compose sidecars, external compose sidecars, infrastructure init containers, and
DinD engine images). Only prebuilt main images consulted Artifact Registry
cache resolution; external sidecars and infrastructure images bypassed it, and
unrecognized compose service definitions silently degraded to ``<service>:latest``.

This module enforces Invariant 6: every container image string placed on any
Kubernetes Pod container (``initContainers``, ``containers``, or
``ephemeralContainers``) must be issued by an :class:`ImageResolver` instance
and validated by :meth:`ImageResolver.assert_pod_images_resolved` before Pod
creation.

Note on scope (Decision D7)
---------------------------
Active tag-to-digest resolution and Artifact Registry remote-repository URL
rewriting are deliberately deferred. The resolver records origin metadata,
classifies GKE Image Streaming eligibility, and provides a single pluggable
rewrite hook so that future Artifact Registry pull-through caching requires
changing one function rather than auditing Pod construction sites.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from enum import StrEnum
import re
from typing import Any

DOCKER_HUB_REGISTRY = "docker.io"
_GAR_HOST_RE = re.compile(r"^[a-z0-9-]+-docker\.pkg\.dev$")
_GCR_HOSTS = frozenset({"gcr.io", "us.gcr.io", "eu.gcr.io", "asia.gcr.io"})
_DOCKER_HUB_HOSTS = frozenset(
    {
        "docker.io",
        "index.docker.io",
        "registry-1.docker.io",
    }
)
_REGISTRY_HOST_RE = re.compile(
    r"^(?:localhost|[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+)(?::[0-9]{1,5})?$"
)
_REPOSITORY_RE = re.compile(
    r"^[a-zA-Z0-9]+(?:(?:\.|_+|[-]+)[a-zA-Z0-9]+)*(?:/[a-zA-Z0-9]+(?:(?:\.|_+|[-]+)[a-zA-Z0-9]+)*)*$"
)
_TAG_RE = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
_FORBIDDEN_CHARS_RE = re.compile(r"[\s?#\\]")


def _validate_registry_host(registry: str) -> None:
    candidate = registry.strip().lower()
    if not candidate or not _REGISTRY_HOST_RE.fullmatch(candidate):
        raise ValueError(f"Invalid registry host: {registry!r}")
    _, sep, port_str = candidate.partition(":")
    if sep:
        port = int(port_str)
        if not (1 <= port <= 65535):
            raise ValueError(f"Invalid registry port in {registry!r}: {port}")


def is_google_registry_host(host: str) -> bool:
    """Return True iff ``host`` is an official Google Container/Artifact Registry domain."""
    candidate = host.strip().lower()
    try:
        _validate_registry_host(candidate)
    except ValueError:
        return False
    hostname, _, _ = candidate.partition(":")
    return hostname in _GCR_HOSTS or bool(_GAR_HOST_RE.fullmatch(hostname))


class ImageOrigin(StrEnum):
    """Provenance category of a container image reference in a GKE trial."""

    MAIN_BUILT = "main_built"
    SIDECAR_BUILT = "sidecar_built"
    SIDECAR_EXTERNAL = "sidecar_external"
    INFRA = "infra"


class UnresolvedPodImageError(RuntimeError):
    """Raised when a Pod spec contains an image string not issued by ImageResolver."""


@dataclass(frozen=True)
class ImageRef:
    """Normalized OCI container image reference."""

    registry: str
    repository: str
    tag: str | None = None
    digest: str | None = None

    def __post_init__(self) -> None:
        if not self.registry or not self.repository:
            raise ValueError(
                f"ImageRef requires non-empty registry and repository, got {self!r}"
            )
        if not self.tag and not self.digest:
            raise ValueError(
                f"ImageRef requires at least one of tag or digest, got {self!r}"
            )
        _validate_registry_host(self.registry)
        if not _REPOSITORY_RE.fullmatch(self.repository):
            raise ValueError(
                f"Invalid OCI repository path {self.repository!r} in {self!r}"
            )
        if self.tag is not None and not _TAG_RE.fullmatch(self.tag):
            raise ValueError(f"Invalid OCI image tag {self.tag!r} in {self!r}")
        if self.digest and not _DIGEST_RE.match(self.digest):
            raise ValueError(
                f"Invalid image digest {self.digest!r}; expected 'sha256:<64 hex chars>'"
            )

    @property
    def canonical(self) -> str:
        """Return canonical fully-qualified reference string."""
        base = f"{self.registry}/{self.repository}"
        if self.tag and self.digest:
            return f"{base}:{self.tag}@{self.digest}"
        if self.digest:
            return f"{base}@{self.digest}"
        return f"{base}:{self.tag}"

    @property
    def is_digest_pinned(self) -> bool:
        """True if the reference pins an immutable sha256 manifest digest."""
        return self.digest is not None

    @property
    def is_google_hosted(self) -> bool:
        """True if served by Artifact Registry (``*-docker.pkg.dev``) or GCR.

        Harbor mirrors *into* Artifact Registry, so an image that is already
        Google-hosted has nothing to gain from being mirrored.
        """
        return is_google_registry_host(self.registry)

    @property
    def streaming_eligible(self) -> bool:
        """True if eligible for GKE Image Streaming on containerd nodes.

        GKE Image Streaming supports container images hosted in:
        1. Google Artifact Registry (``*-docker.pkg.dev``) or legacy GCR (``*gcr.io``)
        2. Public Docker Hub (``docker.io``)

        Images hosted on other registries (e.g. ``public.ecr.aws``, ``ghcr.io``,
        ``quay.io``) are NOT eligible unless mirrored through an Artifact Registry
        remote repository.
        """
        return self.registry.lower() in _DOCKER_HUB_HOSTS or self.is_google_hosted


def parse_image_ref(reference: str) -> ImageRef:
    """Parse an arbitrary Docker/OCI image string into a normalized :class:`ImageRef`."""
    raw = reference.strip()
    if not raw:
        raise ValueError("Image reference cannot be empty")
    if _FORBIDDEN_CHARS_RE.search(raw):
        raise ValueError(
            f"Invalid forbidden characters in image reference {reference!r}"
        )

    digest: str | None = None
    if "@" in raw:
        raw, digest_part = raw.rsplit("@", 1)
        if "@" in raw:
            raise ValueError(f"Invalid '@' in image reference {reference!r}")
        digest = digest_part.strip()
        if not _DIGEST_RE.match(digest):
            raise ValueError(
                f"Invalid digest in image reference {reference!r}: {digest!r}"
            )

    # Determine registry vs repository split.
    # Docker convention: the first slash-delimited component is a registry host
    # if and only if it contains a '.' or ':' or is literally 'localhost'.
    parts = raw.split("/")
    first = parts[0]
    if len(parts) == 1:
        registry = DOCKER_HUB_REGISTRY
        repo_and_tag = f"library/{first}"
    elif "." in first or ":" in first or first == "localhost":
        registry = first
        repo_and_tag = "/".join(parts[1:])
    else:
        registry = DOCKER_HUB_REGISTRY
        repo_and_tag = raw

    if registry in _DOCKER_HUB_HOSTS:
        registry = DOCKER_HUB_REGISTRY
        if "/" not in repo_and_tag:
            repo_and_tag = f"library/{repo_and_tag}"

    # Extract tag from the final path segment.
    last_slash = repo_and_tag.rfind("/")
    last_segment = repo_and_tag[last_slash + 1 :] if last_slash >= 0 else repo_and_tag
    tag: str | None = None
    if ":" in last_segment:
        prefix = repo_and_tag[: last_slash + 1] if last_slash >= 0 else ""
        repo_leaf, tag = last_segment.rsplit(":", 1)
        repository = f"{prefix}{repo_leaf}"
    else:
        repository = repo_and_tag
        if not digest:
            tag = "latest"

    if not repository:
        raise ValueError(f"Missing repository name in image reference {reference!r}")

    return ImageRef(
        registry=registry,
        repository=repository,
        tag=tag,
        digest=digest,
    )


@dataclass(frozen=True)
class ResolvedImage:
    """Record of an image reference processed by :class:`ImageResolver`."""

    original: str
    ref: ImageRef
    origin: ImageOrigin
    rewritten: bool = False

    @property
    def streaming_eligible(self) -> bool:
        return self.ref.streaming_eligible


@dataclass
class ImageResolver:
    """Per-environment container image resolver and Pod image auditor.

    Each :class:`GKEEnvironment` instance owns its own :class:`ImageResolver` so
    that concurrent trials running in the same Python process remain completely
    isolated.
    """

    project_id: str | None = None
    registry_name: str = "harbor-tasks"
    registry_location: str = "us-central1"
    _issued_by_string: dict[str, ResolvedImage] = field(default_factory=dict)

    def resolve(self, reference: str, *, origin: ImageOrigin) -> str:
        """Parse, record, and return an image reference string."""
        ref = parse_image_ref(reference)
        resolved = ResolvedImage(
            original=reference,
            ref=ref,
            origin=origin,
            rewritten=False,
        )
        emitted = reference

        self._issued_by_string[emitted] = resolved
        self._issued_by_string[reference] = resolved
        self._issued_by_string[resolved.ref.canonical] = resolved
        return emitted

    def resolved_images(self) -> list[ResolvedImage]:
        """Return deduplicated list of all images resolved by this instance."""
        seen: dict[tuple[str, ImageOrigin], ResolvedImage] = {}
        for item in self._issued_by_string.values():
            key = (item.ref.canonical, item.origin)
            if key not in seen:
                seen[key] = item
        return list(seen.values())

    def streaming_report(self) -> dict[str, Any]:
        """Return structured summary of Image Streaming eligibility for trial logs."""
        items = self.resolved_images()
        eligible = [i for i in items if i.streaming_eligible]
        ineligible = [i for i in items if not i.streaming_eligible]
        digest_pinned = [i for i in items if i.ref.is_digest_pinned]
        return {
            "total": len(items),
            "streaming_eligible": len(eligible),
            "streaming_ineligible": len(ineligible),
            "digest_pinned": len(digest_pinned),
            "ineligible_images": [
                {
                    "image": i.original,
                    "registry": i.ref.registry,
                    "origin": str(i.origin),
                }
                for i in ineligible
            ],
        }

    def assert_pod_images_resolved(self, pod: Any) -> None:
        """Verify that every container image in ``pod`` was issued by this resolver.

        Raises :class:`UnresolvedPodImageError` if any container (in
        ``init_containers``, ``containers``, or ``ephemeral_containers``) has
        an image string that was not registered via :meth:`resolve`.
        """
        unregistered: list[tuple[str, str]] = []
        for container_name, image_str in _iter_pod_images(pod):
            if not image_str or image_str not in self._issued_by_string:
                unregistered.append((container_name, image_str or "<empty>"))

        if unregistered:
            details = ", ".join(
                f"container={cname!r} image={img!r}" for cname, img in unregistered
            )
            raise UnresolvedPodImageError(
                f"Pod specification contains {len(unregistered)} container image(s) "
                f"that bypassed ImageResolver.resolve(): {details}"
            )


def _iter_pod_images(pod: Any) -> Iterator[tuple[str, str]]:
    """Yield ``(container_name, image)`` pairs from a V1Pod, V1Job, or dict spec."""
    spec = getattr(pod, "spec", None)
    if spec is None and isinstance(pod, dict):
        spec = pod.get("spec")
    if spec is None:
        return

    # Support V1Job / V1Deployment template wrapper if passed
    template = getattr(spec, "template", None)
    if template is None and isinstance(spec, dict):
        template = spec.get("template")
    if template is not None:
        inner_spec = getattr(template, "spec", None)
        if inner_spec is None and isinstance(template, dict):
            inner_spec = template.get("spec")
        if inner_spec is not None:
            spec = inner_spec

    for field_name in (
        "init_containers",
        "initContainers",
        "containers",
        "ephemeral_containers",
        "ephemeralContainers",
    ):
        containers = (
            spec.get(field_name)
            if isinstance(spec, dict)
            else getattr(spec, field_name, None)
        )
        if not containers:
            continue
        for c in containers:
            if isinstance(c, dict):
                name = str(c.get("name") or "<unnamed>")
                image = str(c.get("image") or "")
            else:
                name = str(getattr(c, "name", None) or "<unnamed>")
                image = str(getattr(c, "image", None) or "")
            yield name, image
