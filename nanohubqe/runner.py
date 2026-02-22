"""Execution helpers for local and HUBzero submit-based Quantum ESPRESSO runs."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

from .deck import PWInputDeck
from .workflow import QEStep, QEWorkflow


@dataclass
class SubmitConfig:
    """Configuration for remote execution via the HUBzero `submit` command."""

    venue: str | None = None
    run_name: str | None = None
    manager: str | None = None
    nodes: int | None = None
    walltime: str | None = None
    # Backward-compatible aliases for older notebooks/configs.
    n_cpus: int | None = None
    wall_time: str | None = None
    input_files: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    parameters: str | None = None
    extra_args: list[str] = field(default_factory=list)
    # Maps submit program names, e.g. "pw.x" -> "espresso-7.1_pw".
    executable_prefix: str | None = None
    executable_map: dict[str, str] = field(default_factory=dict)
    # Override input flag used by remote executable command.
    # If None and executable_prefix/executable_map is used, defaults to "-i".
    program_input_flag: str | None = None
    # If True, append the generated step input file to submit "-i" inputs.
    stage_input_file: bool = False


@dataclass
class ExecutionResult:
    """Result returned by `QERunner.run` and `QERunner.run_workflow`."""

    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    workdir: Path
    input_file: Path | None
    output_file: Path
    expected_outputs: list[str] = field(default_factory=list)
    discovered_outputs: list[Path] = field(default_factory=list)
    submitted: bool = False

    @property
    def ok(self) -> bool:
        return self.returncode == 0


@dataclass
class QERunner:
    """Run pw.x calculations locally or through the HUBzero `submit` wrapper."""

    pw_executable: str = "pw.x"
    submit_executable: str = "submit"
    mpi_prefix: list[str] = field(default_factory=list)
    default_backend: str = "local"

    def build_pw_command(self, input_filename: str) -> list[str]:
        return [*self.mpi_prefix, self.pw_executable, "-in", input_filename]

    def build_step_command(self, step: QEStep) -> list[str]:
        """Build the command line for an executable workflow step."""

        executable = self.pw_executable if step.executable == "pw.x" else step.executable
        command = [*self.mpi_prefix, executable, *step.args]
        if step.input_mode == "flag":
            if not step.input_filename:
                raise ValueError("Step input filename is required for input_mode='flag'")
            command.extend([step.input_flag, step.input_filename])
        return command

    def build_submit_command(
        self,
        qe_command: Sequence[str],
        submit_config: SubmitConfig,
    ) -> list[str]:
        """Build a HUBzero `submit` command around a QE command."""

        remote_command = list(qe_command)
        if remote_command:
            remote_command[0] = self._submit_executable_name(
                remote_command[0],
                submit_config,
            )

        program_input_flag = submit_config.program_input_flag
        if program_input_flag is None and (
            submit_config.executable_prefix or submit_config.executable_map
        ):
            program_input_flag = "-i"
        if program_input_flag:
            remote_command = [
                program_input_flag if token == "-in" else token
                for token in remote_command
            ]

        command = [self.submit_executable]

        nodes = submit_config.nodes
        if nodes is None:
            nodes = submit_config.n_cpus
        walltime = submit_config.walltime or submit_config.wall_time

        if nodes is not None:
            command.extend(["-n", str(nodes)])
        if walltime:
            command.extend(["-w", walltime])
        if submit_config.manager:
            command.extend(["--manager", submit_config.manager])
        if submit_config.run_name:
            command.extend(["--runName", submit_config.run_name])
        if submit_config.venue:
            command.extend(["--venue", submit_config.venue])

        for item in submit_config.input_files:
            command.extend(["-i", item])

        for key, value in submit_config.env.items():
            command.extend(["--env", f"{key}={value}"])

        if submit_config.parameters:
            command.extend(["--parameters", submit_config.parameters])

        command.extend(submit_config.extra_args)
        command.extend(remote_command)
        return command

    @staticmethod
    def _submit_executable_name(executable: str, submit_config: SubmitConfig) -> str:
        mapped = submit_config.executable_map.get(executable)
        if mapped:
            return mapped
        if submit_config.executable_prefix:
            base = executable[:-2] if executable.endswith(".x") else executable
            return f"{submit_config.executable_prefix}_{base}"
        return executable

    @staticmethod
    def _clone_submit_config(config: SubmitConfig | None) -> SubmitConfig:
        if config is None:
            return SubmitConfig()
        return SubmitConfig(
            venue=config.venue,
            run_name=config.run_name,
            manager=config.manager,
            nodes=config.nodes,
            walltime=config.walltime,
            n_cpus=config.n_cpus,
            wall_time=config.wall_time,
            input_files=list(config.input_files),
            env=dict(config.env),
            parameters=config.parameters,
            extra_args=list(config.extra_args),
            executable_prefix=config.executable_prefix,
            executable_map=dict(config.executable_map),
            program_input_flag=config.program_input_flag,
            stage_input_file=config.stage_input_file,
        )

    @staticmethod
    def _submit_pseudo_inputs(step: QEStep) -> list[str]:
        if step.deck is None:
            return []

        pseudo_dir_raw = str(step.deck.control.get("pseudo_dir", "./pseudo"))
        pseudo_dir = Path(pseudo_dir_raw)
        entries: list[str] = []
        for species in step.deck.atomic_species:
            pseudo_file = species.pseudo_file
            if str(pseudo_dir) in {"", "."}:
                entries.append(pseudo_file)
            else:
                entries.append(str((pseudo_dir / pseudo_file).as_posix()))
        return entries

    def _effective_submit_config(
        self,
        step: QEStep,
        submit_config: SubmitConfig | None,
        input_filename: str | None,
    ) -> SubmitConfig:
        config = self._clone_submit_config(submit_config)
        for entry in step.submit_input_files:
            if entry not in config.input_files:
                config.input_files.append(entry)
        for entry in self._submit_pseudo_inputs(step):
            if entry not in config.input_files:
                config.input_files.append(entry)
        if config.stage_input_file and input_filename and input_filename not in config.input_files:
            config.input_files.append(input_filename)
        config.env.update(step.env)
        return config

    def run(
        self,
        deck: PWInputDeck,
        *,
        workdir: str | Path,
        input_filename: str = "qe.in",
        output_filename: str = "qe.out",
        backend: str | None = None,
        submit_config: SubmitConfig | None = None,
        timeout: float | None = None,
        dry_run: bool = False,
    ) -> ExecutionResult:
        """Write a pw.x input deck and execute it."""

        step = QEStep(
            executable="pw.x",
            deck=deck,
            input_mode="flag",
            input_filename=input_filename,
            output_filename=output_filename,
        )
        return self.run_step(
            step,
            step_name="qe",
            workdir=workdir,
            backend=backend,
            submit_config=submit_config,
            timeout=timeout,
            dry_run=dry_run,
        )

    def run_step(
        self,
        step: QEStep,
        *,
        step_name: str,
        workdir: str | Path,
        backend: str | None = None,
        submit_config: SubmitConfig | None = None,
        timeout: float | None = None,
        dry_run: bool = False,
    ) -> ExecutionResult:
        """Execute a single workflow step."""

        backend_name = (backend or self.default_backend).lower()
        if backend_name not in {"local", "submit"}:
            raise ValueError("backend must be either 'local' or 'submit'")

        step.validate()
        resolved_step = step.with_filenames(step_name=step_name)
        resolved_step.validate()

        workdir_path = Path(workdir)
        workdir_path.mkdir(parents=True, exist_ok=True)

        input_text = resolved_step.render_input()
        input_path: Path | None = None
        if input_text is not None:
            assert resolved_step.input_filename is not None
            input_path = workdir_path / resolved_step.input_filename
            input_path.write_text(input_text, encoding="utf-8")

        assert resolved_step.output_filename is not None
        output_path = workdir_path / resolved_step.output_filename

        expected_outputs = [resolved_step.output_filename]
        expected_outputs.extend(resolved_step.expected_output_files)
        expected_outputs.extend(f"glob:{pattern}" for pattern in resolved_step.expected_output_globs)

        command = self.build_step_command(resolved_step)
        submitted = False

        if backend_name == "submit":
            if resolved_step.input_mode == "stdin":
                raise ValueError(
                    "submit backend does not support input_mode='stdin'; use input_mode='flag'"
                )
            cfg = self._effective_submit_config(
                resolved_step,
                submit_config,
                resolved_step.input_filename if input_text is not None else None,
            )
            command = self.build_submit_command(command, cfg)
            submitted = True

        if dry_run:
            stdout = shlex.join(command)
            output_path.write_text(stdout + "\n", encoding="utf-8")
            discovered_outputs = self._discover_outputs(workdir_path, resolved_step, output_path)
            return ExecutionResult(
                command=command,
                returncode=0,
                stdout=stdout,
                stderr="",
                workdir=workdir_path,
                input_file=input_path,
                output_file=output_path,
                expected_outputs=expected_outputs,
                discovered_outputs=discovered_outputs,
                submitted=submitted,
            )

        run_env = None
        if resolved_step.env and backend_name == "local":
            run_env = dict(os.environ)
            run_env.update(resolved_step.env)

        stdin_text = input_text if resolved_step.input_mode == "stdin" else None
        process = subprocess.run(
            command,
            cwd=workdir_path,
            text=True,
            input=stdin_text,
            capture_output=True,
            timeout=timeout,
            check=False,
            env=run_env,
        )

        combined_output = process.stdout
        if process.stderr:
            combined_output = process.stdout + "\n" + process.stderr

        output_path.write_text(combined_output, encoding="utf-8")

        discovered_outputs = self._discover_outputs(workdir_path, resolved_step, output_path)

        return ExecutionResult(
            command=command,
            returncode=process.returncode,
            stdout=process.stdout,
            stderr=process.stderr,
            workdir=workdir_path,
            input_file=input_path,
            output_file=output_path,
            expected_outputs=expected_outputs,
            discovered_outputs=discovered_outputs,
            submitted=submitted,
        )

    def run_workflow(
        self,
        workflow: QEWorkflow,
        *,
        workdir: str | Path,
        backend: str | None = None,
        submit_config: SubmitConfig | None = None,
        timeout: float | None = None,
        dry_run: bool = False,
        input_suffix: str = ".in",
        output_suffix: str = ".out",
        output_record_filename: str | None = "workflow_outputs.json",
    ) -> dict[str, ExecutionResult]:
        """Run each step in a `QEWorkflow` sequentially."""

        base_dir = Path(workdir)
        base_dir.mkdir(parents=True, exist_ok=True)

        results: dict[str, ExecutionResult] = {}
        for step_name, step in workflow.iter_steps(
            input_suffix=input_suffix,
            output_suffix=output_suffix,
        ):
            result = self.run_step(
                step,
                step_name=step_name,
                workdir=base_dir,
                backend=backend,
                submit_config=submit_config,
                timeout=timeout,
                dry_run=dry_run,
            )
            results[step_name] = result
            if result.returncode != 0:
                break

        if output_record_filename:
            self.write_workflow_output_record(
                results,
                base_dir / output_record_filename,
                workflow_name=workflow.name,
            )
        return results

    @staticmethod
    def _discover_outputs(workdir: Path, step: QEStep, output_path: Path) -> list[Path]:
        discovered: dict[str, Path] = {}
        if output_path.exists():
            discovered[str(output_path.resolve())] = output_path.resolve()

        for relative_file in step.expected_output_files:
            candidate = (workdir / relative_file).resolve()
            if candidate.exists():
                discovered[str(candidate)] = candidate

        for pattern in step.expected_output_globs:
            for candidate in sorted(workdir.glob(pattern)):
                if candidate.is_file():
                    resolved = candidate.resolve()
                    discovered[str(resolved)] = resolved

        return list(discovered.values())

    @staticmethod
    def write_workflow_output_record(
        results: Mapping[str, ExecutionResult],
        path: str | Path,
        *,
        workflow_name: str | None = None,
    ) -> Path:
        """Write a JSON record of expected and discovered outputs for each step."""

        record = {
            "workflow": workflow_name,
            "steps": {
                step: {
                    "returncode": result.returncode,
                    "ok": result.ok,
                    "command": result.command,
                    "input_file": str(result.input_file) if result.input_file else None,
                    "stdout_file": str(result.output_file),
                    "expected_outputs": result.expected_outputs,
                    "discovered_outputs": [str(output) for output in result.discovered_outputs],
                }
                for step, result in results.items()
            },
        }
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
        return output_path

def submit_env(**variables: str) -> Mapping[str, str]:
    """Small helper to build submit environment mappings."""

    return dict(variables)
