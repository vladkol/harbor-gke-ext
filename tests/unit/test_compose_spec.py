"""Unit tests for harbor_gke_ext.compose_spec (Commit S1)."""

from __future__ import annotations

from pathlib import Path

import pytest

from harbor_gke_ext.compose_spec import (
    discover_compose_build_services,
    ensure_compose_binary,
    normalize_compose_project,
    parse_duration_seconds,
)


@pytest.mark.unit
def test_parse_duration_seconds() -> None:
    assert parse_duration_seconds(None) is None
    assert parse_duration_seconds("") is None
    assert parse_duration_seconds("0s") == 0
    assert parse_duration_seconds(0) == 0
    assert parse_duration_seconds("500ms") == 1
    assert parse_duration_seconds("1m0s") == 60
    assert parse_duration_seconds("2m30s") == 150
    assert parse_duration_seconds("1h15m") == 4500
    assert parse_duration_seconds(30_000_000_000) == 30
    assert parse_duration_seconds("45") == 45


@pytest.mark.unit
def test_ensure_compose_binary() -> None:
    cmd = ensure_compose_binary()
    assert isinstance(cmd, list)
    assert len(cmd) >= 1


@pytest.mark.unit
def test_normalize_compose_project_fragment_and_interpolation(tmp_path: Path) -> None:
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    command: ["sh", "-c", "echo $$MY_VAR && sleep infinity"]
    environment:
      - CUSTOM_KEY=${CUSTOM_VAL:-default_val}
    volumes:
      - ./rel_dir:/workspace:ro
      - /workspace/solution
    deploy:
      resources:
        limits:
          cpus: ${CPUS}
          memory: ${MEMORY}
  redis:
    image: redis:7-alpine
    depends_on:
      main:
        condition: service_started
""",
        encoding="utf-8",
    )

    project = normalize_compose_project(
        [compose_file],
        env_vars={"CUSTOM_VAL": "overridden", "MAIN_IMAGE_NAME": "my-main:v1"},
        context_dir=tmp_path,
    )

    services = project["services"]
    assert "main" in services
    assert "redis" in services
    assert services["main"]["image"] == "my-main:v1"
    assert services["main"]["environment"]["CUSTOM_KEY"] == "overridden"

    volumes = services["main"]["volumes"]
    bind_vols = [v for v in volumes if v.get("type") == "bind"]
    anon_vols = [
        v for v in volumes if v.get("type") == "volume" and not v.get("source")
    ]
    assert len(bind_vols) == 1
    assert bind_vols[0]["target"] == "/workspace"
    assert bind_vols[0]["read_only"] is True
    assert len(anon_vols) == 1
    assert anon_vols[0]["target"] == "/workspace/solution"


@pytest.mark.unit
def test_discover_compose_build_services(tmp_path: Path) -> None:
    (tmp_path / "sidecars" / "a").mkdir(parents=True)
    (tmp_path / "sidecars" / "b").mkdir(parents=True)
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    build: .
  sidecar_a:
    build: ./sidecars/a
  sidecar_b:
    build:
      context: ./sidecars/b
      dockerfile: Dockerfile.custom
  external_svc:
    image: postgres:15
""",
        encoding="utf-8",
    )

    discovered = discover_compose_build_services([compose_file])
    assert set(discovered.keys()) == {"sidecar_a", "sidecar_b"}
    assert discovered["sidecar_a"] == ((tmp_path / "sidecars" / "a").resolve(), None)
    assert discovered["sidecar_b"] == (
        (tmp_path / "sidecars" / "b").resolve(),
        "Dockerfile.custom",
    )


@pytest.mark.unit
def test_discover_compose_build_services_when_main_needs_overlay(
    tmp_path: Path,
) -> None:
    """When main omits image/build, prepending temp_base_file must not shift
    the Compose project directory away from tmp_path."""
    (tmp_path / "customer").mkdir(parents=True)
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    command: ["sleep", "infinity"]
  customer:
    build: ./customer
  api:
    build:
      context: .
      dockerfile: Dockerfile.api
""",
        encoding="utf-8",
    )

    discovered = discover_compose_build_services([compose_file])
    assert set(discovered.keys()) == {"customer", "api"}
    assert discovered["customer"] == ((tmp_path / "customer").resolve(), None)
    assert discovered["api"] == (tmp_path.resolve(), "Dockerfile.api")


# --- Base `main` overlay contract -------------------------------------------
#
# Harbor's Docker path always prepends a base overlay declaring
# `command: ["sh", "-c", "sleep infinity"]`. Without it `main` runs the image's
# default CMD, which for most task images is `python3`; with no TTY that exits 0
# immediately and the trial dies on the next exec. These tests pin the parity.


@pytest.mark.unit
def test_base_overlay_supplies_command_when_main_is_a_fragment(
    tmp_path: Path,
) -> None:
    """U1.1: main with neither image nor build gets both image and command."""
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    environment:
      - FOO=bar
""",
        encoding="utf-8",
    )

    project = normalize_compose_project(
        [compose_file],
        env_vars={"MAIN_IMAGE_NAME": "my-main:v1"},
        context_dir=tmp_path,
    )

    main = project["services"]["main"]
    assert main["image"] == "my-main:v1"
    assert main["command"] == ["sh", "-c", "sleep infinity"]


@pytest.mark.unit
def test_base_overlay_supplies_command_without_image_for_build_only_main(
    tmp_path: Path,
) -> None:
    """U1.2: main declaring build: gets the command but keeps resolving its own image.

    This is the branch no live trial had ever exercised: `_needs_base_main_overlay`
    returns False, so before the fix the overlay was skipped entirely and `main`
    was left with no command at all.
    """
    (tmp_path / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    build:
      context: .
      dockerfile: Dockerfile
""",
        encoding="utf-8",
    )

    project = normalize_compose_project(
        [compose_file],
        env_vars={"MAIN_IMAGE_NAME": "should-not-be-used:v1"},
        context_dir=tmp_path,
    )

    main = project["services"]["main"]
    assert main["command"] == ["sh", "-c", "sleep infinity"]
    assert "build" in main
    assert main.get("image") != "should-not-be-used:v1"


@pytest.mark.unit
def test_task_declared_command_overrides_base_overlay(tmp_path: Path) -> None:
    """U1.3: the overlay is first on the -f list, so the task always wins."""
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    command: ["python3", "/app/server.py"]
""",
        encoding="utf-8",
    )

    project = normalize_compose_project(
        [compose_file],
        env_vars={"MAIN_IMAGE_NAME": "my-main:v1"},
        context_dir=tmp_path,
    )

    assert project["services"]["main"]["command"] == ["python3", "/app/server.py"]


@pytest.mark.unit
def test_task_declared_entrypoint_and_command_both_survive(tmp_path: Path) -> None:
    """U1.4: an explicit entrypoint is not displaced by the overlay's command."""
    compose_file = tmp_path / "docker-compose.yaml"
    compose_file.write_text(
        """
services:
  main:
    entrypoint: ["/entrypoint.sh"]
    command: ["--serve"]
""",
        encoding="utf-8",
    )

    project = normalize_compose_project(
        [compose_file],
        env_vars={"MAIN_IMAGE_NAME": "my-main:v1"},
        context_dir=tmp_path,
    )

    main = project["services"]["main"]
    assert main["entrypoint"] == ["/entrypoint.sh"]
    assert main["command"] == ["--serve"]
