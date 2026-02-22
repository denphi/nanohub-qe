"""Workflow helpers for multi-step Quantum ESPRESSO simulations."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Literal

from .deck import PWInputDeck

InputMode = Literal["flag", "stdin", "none"]


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
