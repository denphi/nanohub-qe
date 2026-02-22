"""Pseudopotential discovery and download helpers."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence
from urllib.parse import quote
from urllib.request import urlopen

from .deck import PWInputDeck
from .workflow import QEStep, QEWorkflow

DEFAULT_PSEUDO_SOURCE_URLS = (
    "https://pseudopotentials.quantum-espresso.org/upf_files",
)


def _default_local_search_dirs() -> list[Path]:
    dirs: list[Path] = []

    for env_name in ("ESPRESSO_PSEUDO", "QE_PSEUDO", "PSEUDO_DIR"):
        value = os.environ.get(env_name)
        if value:
            dirs.append(Path(value))

    dirs.extend(
        [
            Path("/apps/dftqe/r113/data/atoms_lda"),
            Path("/apps/dftqe/current/data/atoms_lda"),
            Path("/apps/share64/espresso/pseudo"),
            Path("/usr/share/espresso/pseudo"),
        ]
    )

    unique: list[Path] = []
    seen: set[str] = set()
    for directory in dirs:
        key = str(directory)
        if key in seen:
            continue
        seen.add(key)
        unique.append(directory)
    return unique


def _resolve_workdir_path(workdir: str | Path | None) -> Path:
    if workdir is None:
        return Path.cwd()
    return Path(workdir)


def _resolve_pseudo_dir(pseudo_dir: str, workdir: str | Path | None = None) -> Path:
    directory = Path(pseudo_dir)
    if directory.is_absolute():
        return directory
    return _resolve_workdir_path(workdir) / directory


def _iter_workflow_decks(workflow: QEWorkflow) -> list[PWInputDeck]:
    decks: list[PWInputDeck] = []
    for step in workflow.steps.values():
        if isinstance(step, PWInputDeck):
            decks.append(step)
            continue
        if isinstance(step, QEStep) and step.deck is not None:
            decks.append(step.deck)
    return decks


@dataclass(frozen=True)
class PseudoRequirement:
    """A pseudopotential required by a workflow."""

    pseudo_dir: str
    pseudo_file: str


@dataclass(frozen=True)
class PseudoStatus:
    """Result of ensuring a pseudopotential file."""

    pseudo_file: str
    target_path: Path
    action: str
    source: str | None = None


def workflow_pseudopotential_requirements(workflow: QEWorkflow) -> list[PseudoRequirement]:
    """Collect unique pseudopotential requirements from workflow decks."""

    seen: set[tuple[str, str]] = set()
    requirements: list[PseudoRequirement] = []

    for deck in _iter_workflow_decks(workflow):
        pseudo_dir = str(deck.control.get("pseudo_dir", "./pseudo"))
        for species in deck.atomic_species:
            key = (pseudo_dir, species.pseudo_file)
            if key in seen:
                continue
            seen.add(key)
            requirements.append(PseudoRequirement(pseudo_dir=pseudo_dir, pseudo_file=species.pseudo_file))

    return requirements


def ensure_pseudopotentials(
    pseudo_files: Sequence[str],
    *,
    pseudo_dir: str = "./pseudo",
    workdir: str | Path | None = None,
    source_urls: Sequence[str] | None = None,
    local_search_dirs: Sequence[str | Path] | None = None,
    timeout: float = 20.0,
    overwrite: bool = False,
) -> list[PseudoStatus]:
    """Ensure pseudopotential files exist in *pseudo_dir*.

    The function first checks if files already exist, then tries copying from
    local search directories, and finally attempts HTTP downloads.
    """

    target_dir = _resolve_pseudo_dir(pseudo_dir, workdir=workdir)
    target_dir.mkdir(parents=True, exist_ok=True)

    urls = list(source_urls or DEFAULT_PSEUDO_SOURCE_URLS)
    search_dirs = [Path(value) for value in (local_search_dirs or _default_local_search_dirs())]

    statuses: list[PseudoStatus] = []
    for raw_name in pseudo_files:
        pseudo_name = Path(raw_name).name
        target_path = target_dir / pseudo_name

        if target_path.exists() and not overwrite:
            statuses.append(
                PseudoStatus(
                    pseudo_file=pseudo_name,
                    target_path=target_path,
                    action="exists",
                )
            )
            continue

        copied = False
        for search_dir in search_dirs:
            candidate = search_dir / pseudo_name
            if not candidate.is_file():
                continue
            if candidate.resolve() != target_path.resolve():
                shutil.copy2(candidate, target_path)
            copied = True
            statuses.append(
                PseudoStatus(
                    pseudo_file=pseudo_name,
                    target_path=target_path,
                    action="copied",
                    source=str(candidate),
                )
            )
            break

        if copied:
            continue

        downloaded = False
        attempted_urls: list[str] = []
        for base_url in urls:
            url = f"{base_url.rstrip('/')}/{quote(pseudo_name)}"
            attempted_urls.append(url)
            try:
                with urlopen(url, timeout=timeout) as response:  # nosec B310
                    data = response.read()
                if not data:
                    continue
                target_path.write_bytes(data)
                downloaded = True
                statuses.append(
                    PseudoStatus(
                        pseudo_file=pseudo_name,
                        target_path=target_path,
                        action="downloaded",
                        source=url,
                    )
                )
                break
            except Exception:
                continue

        if not downloaded:
            search_text = ", ".join(str(path) for path in search_dirs)
            url_text = ", ".join(attempted_urls)
            raise FileNotFoundError(
                f"Unable to provision pseudopotential '{pseudo_name}'. "
                f"Searched local directories [{search_text}] and URLs [{url_text}]."
            )

    return statuses


def ensure_workflow_pseudopotentials(
    workflow: QEWorkflow,
    *,
    workdir: str | Path | None = None,
    source_urls: Sequence[str] | None = None,
    local_search_dirs: Sequence[str | Path] | None = None,
    timeout: float = 20.0,
    overwrite: bool = False,
) -> list[PseudoStatus]:
    """Ensure all pseudopotentials required by *workflow* are available."""

    statuses: list[PseudoStatus] = []
    for requirement in workflow_pseudopotential_requirements(workflow):
        statuses.extend(
            ensure_pseudopotentials(
                [requirement.pseudo_file],
                pseudo_dir=requirement.pseudo_dir,
                workdir=workdir,
                source_urls=source_urls,
                local_search_dirs=local_search_dirs,
                timeout=timeout,
                overwrite=overwrite,
            )
        )
    return statuses
