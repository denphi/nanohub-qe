"""Workflow helpers for multi-step Quantum ESPRESSO simulations."""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Sequence

from .deck import PWInputDeck

InputMode = Literal["flag", "stdin", "none"]

if TYPE_CHECKING:
    from .pseudo import PseudoStatus
    from .runner import ExecutionResult, QERunner, SubmitConfig


@dataclass
class QEStep:
    """Single executable step in a Quantum ESPRESSO workflow."""

    executable: str = "pw.x"
    deck: PWInputDeck | None = None
    input_text: str | None = None
    args: list[str] = field(default_factory=list)
    input_mode: InputMode = "flag"
    input_flag: str = "-in"
    input_filename: str | None = None
    output_filename: str | None = None
    submit_input_files: list[str] = field(default_factory=list)
    allow_missing_submit_input_files: bool = False
    expected_output_files: list[str] = field(default_factory=list)
    expected_output_globs: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    notes: str | None = None

    def with_filenames(
        self,
        *,
        step_name: str,
        input_suffix: str = ".in",
        output_suffix: str = ".out",
    ) -> QEStep:
        """Return a copy with default input/output filenames resolved."""

        input_name = self.input_filename or f"{step_name}{input_suffix}"
        output_name = self.output_filename or f"{step_name}{output_suffix}"
        return replace(self, input_filename=input_name, output_filename=output_name)

    def render_input(self) -> str | None:
        """Render input text from deck/text payload."""

        if self.deck is not None:
            return self.deck.to_string()
        if self.input_text is None:
            return None
        text = self.input_text.strip()
        if not text:
            return None
        return text + "\n"

    def validate(self) -> None:
        """Validate that the step configuration is coherent."""

        if self.deck is not None and self.input_text is not None:
            raise ValueError("QEStep cannot set both deck and input_text")
        if self.input_mode not in {"flag", "stdin", "none"}:
            raise ValueError("QEStep input_mode must be one of: flag, stdin, none")

        needs_input = self.input_mode in {"flag", "stdin"}
        if needs_input and self.render_input() is None:
            raise ValueError(
                "QEStep requires deck or input_text when input_mode is 'flag' or 'stdin'"
            )
        if self.input_mode == "flag" and not self.input_flag:
            raise ValueError("QEStep input_flag cannot be empty when input_mode is 'flag'")


@dataclass
class QEWorkflow:
    """A named sequence of executable Quantum ESPRESSO steps."""

    name: str
    steps: dict[str, QEStep | PWInputDeck]
    order: list[str]
    notes: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    _last_results: dict[str, ExecutionResult] = field(default_factory=dict, init=False, repr=False)
    _last_workdir: Path | None = field(default=None, init=False, repr=False)

    @staticmethod
    def _print_pseudo_statuses(
        statuses: Sequence[PseudoStatus],
        *,
        verbose: bool | None,
    ) -> None:
        if not verbose:
            return
        for item in statuses:
            if item.source:
                print(
                    f"[nanohubqe] pseudo: {item.pseudo_file} {item.action} -> "
                    f"{item.target_path} (source={item.source})",
                    flush=True,
                )
            else:
                print(
                    f"[nanohubqe] pseudo: {item.pseudo_file} {item.action} -> "
                    f"{item.target_path}",
                    flush=True,
                )

    def _normalize_step(
        self,
        step_name: str,
        step: QEStep | PWInputDeck,
        *,
        input_suffix: str = ".in",
        output_suffix: str = ".out",
    ) -> QEStep:
        if isinstance(step, PWInputDeck):
            normalized = QEStep(deck=step, executable="pw.x", input_mode="flag")
        elif isinstance(step, QEStep):
            normalized = step
        else:
            raise TypeError(f"Unsupported workflow step type for '{step_name}': {type(step)!r}")

        resolved = normalized.with_filenames(
            step_name=step_name,
            input_suffix=input_suffix,
            output_suffix=output_suffix,
        )
        resolved.validate()
        return resolved

    def iter_steps(
        self,
        *,
        input_suffix: str = ".in",
        output_suffix: str = ".out",
    ) -> list[tuple[str, QEStep]]:
        """Return ordered `(step_name, step)` pairs."""

        missing = [step for step in self.order if step not in self.steps]
        if missing:
            missing_text = ", ".join(missing)
            raise KeyError(f"Workflow references undefined steps: {missing_text}")

        return [
            (
                step_name,
                self._normalize_step(
                    step_name,
                    self.steps[step_name],
                    input_suffix=input_suffix,
                    output_suffix=output_suffix,
                ),
            )
            for step_name in self.order
        ]

    def write(self, directory: str | Path, suffix: str = ".in") -> dict[str, Path]:
        """Write each step input file to *directory* and return paths."""

        target_dir = Path(directory)
        target_dir.mkdir(parents=True, exist_ok=True)

        outputs: dict[str, Path] = {}
        for step_name, step in self.iter_steps(input_suffix=suffix):
            input_text = step.render_input()
            if input_text is None:
                continue
            assert step.input_filename is not None
            path = target_dir / step.input_filename
            path.write_text(input_text, encoding="utf-8")
            outputs[step_name] = path
        return outputs

    @property
    def last_workdir(self) -> Path | None:
        """Last workflow work directory used by :meth:`run`."""

        return self._last_workdir

    @property
    def results(self) -> dict[str, ExecutionResult]:
        """Results from the last :meth:`run` call."""

        return dict(self._last_results)

    def run(
        self,
        *,
        workdir: str | Path,
        runner: QERunner | None = None,
        backend: str | None = None,
        submit_config: SubmitConfig | None = None,
        timeout: float | None = None,
        dry_run: bool = False,
        verbose: bool | None = None,
        input_suffix: str = ".in",
        output_suffix: str = ".out",
        output_record_filename: str | None = "workflow_outputs.json",
        auto_prepare_pseudopotentials: bool = True,
        pseudo_source_urls: Sequence[str] | None = None,
        pseudo_local_search_dirs: Sequence[str | Path] | None = None,
        pseudo_timeout: float = 20.0,
        pseudo_overwrite: bool = False,
    ) -> QEWorkflow:
        """Run this workflow and cache results for convenience helpers."""

        from .runner import QERunner

        if auto_prepare_pseudopotentials and not dry_run:
            statuses = self.prepare_pseudopotentials(
                workdir=workdir,
                source_urls=pseudo_source_urls,
                local_search_dirs=pseudo_local_search_dirs,
                timeout=pseudo_timeout,
                overwrite=pseudo_overwrite,
            )
            self._print_pseudo_statuses(statuses, verbose=verbose)

        active_runner = runner or QERunner()
        results = active_runner.run_workflow(
            self,
            workdir=workdir,
            backend=backend,
            submit_config=submit_config,
            timeout=timeout,
            dry_run=dry_run,
            verbose=verbose,
            input_suffix=input_suffix,
            output_suffix=output_suffix,
            output_record_filename=output_record_filename,
        )
        self._last_results = results
        self._last_workdir = Path(workdir)
        return self

    def run_submit(
        self,
        *,
        workdir: str | Path,
        runner: QERunner | None = None,
        submit_config: SubmitConfig | None = None,
        timeout: float | None = None,
        dry_run: bool = False,
        verbose: bool | None = None,
        wait: bool = True,
        sync_outputs: bool = True,
        poll_interval: float = 20.0,
        wait_timeout: float | None = None,
        assign_step_run_names: bool = True,
        input_suffix: str = ".in",
        output_suffix: str = ".out",
        output_record_filename: str | None = "workflow_outputs.json",
        auto_prepare_pseudopotentials: bool = True,
        pseudo_source_urls: Sequence[str] | None = None,
        pseudo_local_search_dirs: Sequence[str | Path] | None = None,
        pseudo_timeout: float = 20.0,
        pseudo_overwrite: bool = False,
    ) -> QEWorkflow:
        """Submit this workflow and optionally wait/sync outputs for plotting."""

        from .runner import QERunner

        if auto_prepare_pseudopotentials and not dry_run:
            statuses = self.prepare_pseudopotentials(
                workdir=workdir,
                source_urls=pseudo_source_urls,
                local_search_dirs=pseudo_local_search_dirs,
                timeout=pseudo_timeout,
                overwrite=pseudo_overwrite,
            )
            self._print_pseudo_statuses(statuses, verbose=verbose)

        active_runner = runner or QERunner(default_backend="submit")
        results = active_runner.run_workflow_submit(
            self,
            workdir=workdir,
            submit_config=submit_config,
            timeout=timeout,
            dry_run=dry_run,
            verbose=verbose,
            wait=wait,
            sync_outputs=sync_outputs,
            poll_interval=poll_interval,
            wait_timeout=wait_timeout,
            assign_step_run_names=assign_step_run_names,
            input_suffix=input_suffix,
            output_suffix=output_suffix,
            output_record_filename=output_record_filename,
        )
        self._last_results = results
        self._last_workdir = Path(workdir)
        return self

    def prepare_pseudopotentials(
        self,
        *,
        workdir: str | Path | None = None,
        source_urls: Sequence[str] | None = None,
        local_search_dirs: Sequence[str | Path] | None = None,
        timeout: float = 20.0,
        overwrite: bool = False,
    ) -> list[PseudoStatus]:
        """Ensure required pseudopotential files are available for this workflow."""

        from .pseudo import ensure_workflow_pseudopotentials

        return ensure_workflow_pseudopotentials(
            self,
            workdir=workdir,
            source_urls=source_urls,
            local_search_dirs=local_search_dirs,
            timeout=timeout,
            overwrite=overwrite,
        )

    def step_result(self, step_name: str) -> ExecutionResult:
        """Get a step result from the most recent :meth:`run`."""

        if not self._last_results:
            raise RuntimeError("Workflow has not been run yet. Call sim.run(...) first.")
        if step_name not in self._last_results:
            known = ", ".join(self._last_results) or "(none)"
            raise KeyError(f"Step '{step_name}' is unavailable. Executed steps: {known}")
        return self._last_results[step_name]

    def _resolve_step_output(self, step_name: str) -> Path:
        result = self.step_result(step_name)
        if result.output_file.exists():
            return result.output_file
        raise FileNotFoundError(
            f"Output file for step '{step_name}' was not found: {result.output_file}"
        )

    def _resolve_generated_from_step(
        self,
        step_name: str,
        *,
        label: str,
        matcher,
    ) -> Path:
        if not self._last_results:
            raise RuntimeError("Workflow has not been run yet. Call sim.run(...) first.")

        if step_name not in self._last_results:
            executed = ", ".join(self._last_results) or "(none)"
            raise FileNotFoundError(
                f"{label} is unavailable because step '{step_name}' did not run. "
                f"Executed steps: {executed}"
            )

        result = self._last_results[step_name]
        if not result.ok:
            raise FileNotFoundError(
                f"{label} is unavailable because step '{step_name}' failed "
                f"(returncode={result.returncode}). Check: {result.output_file}"
            )

        for candidate in result.discovered_outputs:
            if candidate.is_file() and matcher(candidate):
                return candidate

        discovered = ", ".join(path.name for path in result.discovered_outputs) or "(none)"
        raise FileNotFoundError(
            f"{label} was not found in outputs of step '{step_name}'. "
            f"Discovered outputs: {discovered}. Check: {result.output_file}"
        )

    def _resolve_dos_path(self, path: str | Path | None = None) -> Path:
        if path is not None:
            resolved = Path(path)
            if resolved.exists():
                return resolved
            raise FileNotFoundError(f"DOS file does not exist: {resolved}")

        return self._resolve_generated_from_step(
            "dos",
            label="DOS file",
            matcher=lambda path: path.name.endswith(".dos"),
        )

    def _resolve_bands_path(self, path: str | Path | None = None) -> Path:
        if path is not None:
            resolved = Path(path)
            if resolved.exists():
                return resolved
            raise FileNotFoundError(f"Bands file does not exist: {resolved}")

        if "bands_pp" in self._last_results:
            return self._resolve_generated_from_step(
                "bands_pp",
                label="Bands file",
                matcher=lambda path: path.name.endswith(".bands.dat.gnu")
                or path.name.endswith(".bands.dat")
                or path.name.endswith(".gnu"),
            )
        return self._resolve_generated_from_step(
            "bands",
            label="Bands file",
            matcher=lambda path: path.name.endswith(".bands.dat.gnu")
            or path.name.endswith(".bands.dat")
            or path.name.endswith(".gnu"),
        )

    def _bands_labels_from_metadata(self) -> list[str] | None:
        raw = self.metadata.get("bands_k_labels")
        if not raw:
            return None
        labels = [item.strip() for item in raw.split(",") if item.strip()]
        return labels or None

    def _bands_step_deck(self) -> PWInputDeck | None:
        for step_name in ("bands_pw", "bands"):
            step = self.steps.get(step_name)
            if isinstance(step, PWInputDeck):
                return step
            if isinstance(step, QEStep) and step.deck is not None:
                return step.deck
        return None

    def _bands_ticks(
        self,
        labels: Sequence[str] | None = None,
    ) -> list[tuple[float, str]] | None:
        deck = self._bands_step_deck()
        if deck is None or deck.k_points is None:
            return None

        if not isinstance(deck.k_points, Sequence):
            return None

        points: list[tuple[float, float, float]] = []
        for row in deck.k_points:
            if not isinstance(row, Sequence):
                continue
            if len(row) < 3:
                continue
            points.append((float(row[0]), float(row[1]), float(row[2])))

        if len(points) < 2:
            return None

        resolved_labels = list(labels) if labels is not None else self._bands_labels_from_metadata()
        if not resolved_labels:
            return None

        x_positions = [0.0]
        for prev, curr in zip(points, points[1:]):
            x_positions.append(x_positions[-1] + math.dist(prev, curr))

        count = min(len(x_positions), len(resolved_labels))
        if count == 0:
            return None

        return [(x_positions[index], resolved_labels[index]) for index in range(count)]

    def _resolve_pdos_path(self, path: str | Path | None = None) -> Path:
        if path is not None:
            resolved = Path(path)
            if resolved.exists():
                return resolved
            raise FileNotFoundError(f"PDOS file does not exist: {resolved}")

        return self._resolve_generated_from_step(
            "projwfc",
            label="PDOS file",
            matcher=lambda path: "pdos" in path.name and not path.name.endswith(".out"),
        )

    def _resolve_phonon_path(self, path: str | Path | None = None) -> Path:
        if path is not None:
            resolved = Path(path)
            if resolved.exists():
                return resolved
            raise FileNotFoundError(f"Phonon dispersion file does not exist: {resolved}")

        return self._resolve_generated_from_step(
            "matdyn",
            label="Phonon dispersion file",
            matcher=lambda path: path.name.endswith(".freq"),
        )

    def plot_total_energy(
        self,
        path: str | Path | None = None,
        *,
        step_name: str = "scf",
        unit: str = "Ry",
        backend: str = "matplotlib",
        ax=None,
    ):
        """Plot total energy from a step output file."""

        from .visualize import plot_total_energy

        output_path = Path(path) if path is not None else self._resolve_step_output(step_name)
        return plot_total_energy(output_path, unit=unit, backend=backend, ax=ax)

    def plot_dos(
        self,
        path: str | Path | None = None,
        *,
        fermi_energy_ev: float | None = None,
        backend: str = "matplotlib",
        ax=None,
    ):
        """Plot DOS from the latest workflow outputs."""

        from .visualize import plot_dos

        dos_path = self._resolve_dos_path(path)
        return plot_dos(dos_path, fermi_energy_ev=fermi_energy_ev, backend=backend, ax=ax)

    def plot_bands(
        self,
        path: str | Path | None = None,
        *,
        fermi_energy_ev: float | None = None,
        k_labels: Sequence[str] | None = None,
        backend: str = "matplotlib",
        ax=None,
    ):
        """Plot bands from the latest workflow outputs."""

        from .visualize import plot_bands

        bands_path = self._resolve_bands_path(path)
        kpoint_ticks = self._bands_ticks(labels=k_labels)
        return plot_bands(
            bands_path,
            fermi_energy_ev=fermi_energy_ev,
            kpoint_ticks=kpoint_ticks,
            backend=backend,
            ax=ax,
        )

    def plot_pdos(
        self,
        path: str | Path | None = None,
        *,
        channels: list[str] | None = None,
        fermi_energy_ev: float | None = None,
        backend: str = "matplotlib",
        ax=None,
    ):
        """Plot PDOS from the latest workflow outputs."""

        from .visualize import plot_pdos

        pdos_path = self._resolve_pdos_path(path)
        return plot_pdos(
            pdos_path,
            channels=channels,
            fermi_energy_ev=fermi_energy_ev,
            backend=backend,
            ax=ax,
        )

    def plot_phonon_dispersion(
        self,
        path: str | Path | None = None,
        *,
        backend: str = "matplotlib",
        ax=None,
    ):
        """Plot phonon dispersion from the latest workflow outputs."""

        from .visualize import plot_phonon_dispersion

        phonon_path = self._resolve_phonon_path(path)
        return plot_phonon_dispersion(phonon_path, backend=backend, ax=ax)
