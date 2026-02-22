"""Workflow helpers for multi-step Quantum ESPRESSO simulations."""

from __future__ import annotations

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
        input_suffix: str = ".in",
        output_suffix: str = ".out",
        output_record_filename: str | None = "workflow_outputs.json",
    ) -> QEWorkflow:
        """Run this workflow and cache results for convenience helpers."""

        from .runner import QERunner

        active_runner = runner or QERunner()
        results = active_runner.run_workflow(
            self,
            workdir=workdir,
            backend=backend,
            submit_config=submit_config,
            timeout=timeout,
            dry_run=dry_run,
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

    def _require_workdir(self) -> Path:
        if self._last_workdir is None:
            raise RuntimeError("Workflow has not been run yet. Call sim.run(...) first.")
        return self._last_workdir

    def _find_by_patterns(self, patterns: Sequence[str]) -> Path | None:
        workdir = self._require_workdir()
        for pattern in patterns:
            candidates = sorted(workdir.glob(pattern))
            for candidate in candidates:
                if candidate.is_file():
                    return candidate
        return None

    def _resolve_step_output(self, step_name: str) -> Path:
        result = self.step_result(step_name)
        if result.output_file.exists():
            return result.output_file
        raise FileNotFoundError(
            f"Output file for step '{step_name}' was not found: {result.output_file}"
        )

    def _resolve_dos_path(self, path: str | Path | None = None) -> Path:
        if path is not None:
            resolved = Path(path)
            if resolved.exists():
                return resolved
            raise FileNotFoundError(f"DOS file does not exist: {resolved}")

        dos_result = self._last_results.get("dos")
        if dos_result is not None:
            for candidate in dos_result.discovered_outputs:
                if candidate.name.endswith(".dos") and candidate.is_file():
                    return candidate
        discovered = self._find_by_patterns(["*.dos"])
        if discovered is not None:
            return discovered
        raise FileNotFoundError(
            "No DOS file found. Run a workflow with a DOS step or pass `path=` explicitly."
        )

    def _resolve_bands_path(self, path: str | Path | None = None) -> Path:
        if path is not None:
            resolved = Path(path)
            if resolved.exists():
                return resolved
            raise FileNotFoundError(f"Bands file does not exist: {resolved}")

        bands_pp = self._last_results.get("bands_pp")
        if bands_pp is not None:
            for candidate in bands_pp.discovered_outputs:
                if (
                    candidate.name.endswith(".bands.dat.gnu")
                    or candidate.name.endswith(".bands.dat")
                    or candidate.name.endswith(".gnu")
                ) and candidate.is_file():
                    return candidate

        discovered = self._find_by_patterns(["*.bands.dat.gnu", "*.bands.dat", "*.gnu"])
        if discovered is not None:
            return discovered
        raise FileNotFoundError(
            "No bands file found. Run a workflow with a bands post-processing step or pass `path=` explicitly."
        )

    def _resolve_pdos_path(self, path: str | Path | None = None) -> Path:
        if path is not None:
            resolved = Path(path)
            if resolved.exists():
                return resolved
            raise FileNotFoundError(f"PDOS file does not exist: {resolved}")

        projwfc = self._last_results.get("projwfc")
        if projwfc is not None:
            for candidate in projwfc.discovered_outputs:
                name = candidate.name
                if "pdos" in name and not name.endswith(".out") and candidate.is_file():
                    return candidate

        discovered = self._find_by_patterns(["*.pdos*", "*pdos*"])
        if discovered is not None and not discovered.name.endswith(".out"):
            return discovered
        raise FileNotFoundError(
            "No PDOS file found. Run a workflow with a projwfc step or pass `path=` explicitly."
        )

    def _resolve_phonon_path(self, path: str | Path | None = None) -> Path:
        if path is not None:
            resolved = Path(path)
            if resolved.exists():
                return resolved
            raise FileNotFoundError(f"Phonon dispersion file does not exist: {resolved}")

        matdyn = self._last_results.get("matdyn")
        if matdyn is not None:
            for candidate in matdyn.discovered_outputs:
                if candidate.name.endswith(".freq") and candidate.is_file():
                    return candidate

        discovered = self._find_by_patterns(["*.freq"])
        if discovered is not None:
            return discovered
        raise FileNotFoundError(
            "No phonon frequency file found. Run a workflow with a matdyn step or pass `path=` explicitly."
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
        backend: str = "matplotlib",
        ax=None,
    ):
        """Plot bands from the latest workflow outputs."""

        from .visualize import plot_bands

        bands_path = self._resolve_bands_path(path)
        return plot_bands(bands_path, fermi_energy_ev=fermi_energy_ev, backend=backend, ax=ax)

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
