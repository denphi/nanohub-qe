from __future__ import annotations

from pathlib import Path

import nanohubqe.pseudo as pseudo_mod
from nanohubqe import (
    ensure_pseudopotentials,
    ensure_workflow_pseudopotentials,
    silicon_bands_dos_reference_workflow,
    workflow_pseudopotential_requirements,
)


def test_workflow_pseudopotential_requirements_deduplicate() -> None:
    workflow = silicon_bands_dos_reference_workflow(
        pseudo_file="Si.UPF",
        pseudo_dir="./pseudo",
        include_plotband=False,
    )

    requirements = workflow_pseudopotential_requirements(workflow)

    assert len(requirements) == 1
    assert requirements[0].pseudo_dir == "./pseudo"
    assert requirements[0].pseudo_file == "Si.UPF"


def test_ensure_pseudopotentials_copies_from_local_search_dir(tmp_path: Path) -> None:
    search_dir = tmp_path / "source"
    search_dir.mkdir()
    (search_dir / "Si.UPF").write_text("local-si-pseudo", encoding="utf-8")

    statuses = ensure_pseudopotentials(
        ["Si.UPF"],
        pseudo_dir="./pseudo",
        workdir=tmp_path,
        local_search_dirs=[search_dir],
        source_urls=[],
    )

    target = tmp_path / "pseudo" / "Si.UPF"
    assert target.exists()
    assert target.read_text(encoding="utf-8") == "local-si-pseudo"
    assert statuses[0].action == "copied"
    assert statuses[0].target_path == target


def test_ensure_pseudopotentials_downloads_when_missing(
    tmp_path: Path, monkeypatch
) -> None:
    class DummyResponse:
        def __init__(self, payload: bytes) -> None:
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return self.payload

    def fake_urlopen(url: str, timeout: float = 0.0):
        assert url.endswith("/Si.UPF")
        assert timeout == 20.0
        return DummyResponse(b"downloaded-si-pseudo")

    monkeypatch.setattr(pseudo_mod, "urlopen", fake_urlopen)

    statuses = ensure_pseudopotentials(
        ["Si.UPF"],
        pseudo_dir="./pseudo",
        workdir=tmp_path,
        local_search_dirs=[],
        source_urls=["https://example.invalid/upf_files"],
    )

    target = tmp_path / "pseudo" / "Si.UPF"
    assert target.exists()
    assert target.read_bytes() == b"downloaded-si-pseudo"
    assert statuses[0].action == "downloaded"
    assert statuses[0].source == "https://example.invalid/upf_files/Si.UPF"


def test_ensure_workflow_pseudopotentials_uses_workdir(tmp_path: Path) -> None:
    workflow = silicon_bands_dos_reference_workflow(
        pseudo_file="Si.UPF",
        pseudo_dir="./pseudo",
        include_plotband=False,
    )
    search_dir = tmp_path / "library"
    search_dir.mkdir()
    (search_dir / "Si.UPF").write_text("si-upf-data", encoding="utf-8")

    statuses = ensure_workflow_pseudopotentials(
        workflow,
        workdir=tmp_path,
        local_search_dirs=[search_dir],
        source_urls=[],
    )

    target = tmp_path / "pseudo" / "Si.UPF"
    assert target.exists()
    assert target.read_text(encoding="utf-8") == "si-upf-data"
    assert any(item.target_path == target for item in statuses)
