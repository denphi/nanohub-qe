"""Execution helpers for local and HUBzero submit-based Quantum ESPRESSO runs."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
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
    # Optional templates for status/download commands used by run_submit workflows.
    # Available placeholders: {run_name}, {step_name}, {workdir}
    status_command_template: str | None = None
    download_command_template: str | None = None
    # Optional extra roots used to locate downloaded results.
    results_search_dirs: list[str] = field(default_factory=list)
    # If True, fail a submit workflow step when expected outputs are unavailable.
    require_expected_outputs: bool = True


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
    remote_run_name: str | None = None
    remote_job_id: str | None = None
    remote_status: str | None = None
    outputs_synced: bool = False

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

    def _prepare_remote_command(
        self,
        qe_command: Sequence[str],
        submit_config: SubmitConfig,
    ) -> list[str]:
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
        return remote_command

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

        remote_command = self._prepare_remote_command(qe_command, submit_config)

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

    def build_submit_command_legacy(
        self,
        qe_command: Sequence[str],
        submit_config: SubmitConfig,
    ) -> list[str]:
        """Build a conservative legacy submit command for compatibility fallback."""

        remote_command = self._prepare_remote_command(qe_command, submit_config)
        command = [self.submit_executable]

        nodes = submit_config.nodes
        if nodes is None:
            nodes = submit_config.n_cpus
        walltime = submit_config.walltime or submit_config.wall_time

        if submit_config.run_name:
            command.extend(["--runName", submit_config.run_name])
        if submit_config.venue:
            command.extend(["--venue", submit_config.venue])
        if nodes is not None:
            command.extend(["--nCpus", str(nodes)])
        if walltime:
            command.extend(["--wallTime", walltime])
        if submit_config.manager:
            command.extend(["--manager", submit_config.manager])

        for item in submit_config.input_files:
            command.extend(["--inputfile", item])

        for key, value in submit_config.env.items():
            command.extend(["--env", f"{key}={value}"])

        if submit_config.parameters:
            command.extend(["--parameters", submit_config.parameters])

        command.extend(submit_config.extra_args)
        command.append(shlex.join(remote_command))
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
            status_command_template=config.status_command_template,
            download_command_template=config.download_command_template,
            results_search_dirs=list(config.results_search_dirs),
            require_expected_outputs=config.require_expected_outputs,
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

    @staticmethod
    def _parse_submit_job_id(text: str) -> str | None:
        patterns = [
            r"\bjob(?:\s+id)?\s*[:=#]?\s*([A-Za-z0-9._:-]+)",
            r"\brun(?:\s+id)?\s*[:=#]?\s*([A-Za-z0-9._:-]+)",
            r"\bsubmitted(?:\s+as)?\s+([A-Za-z0-9._:-]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _render_submit_template(
        template: str,
        *,
        run_name: str,
        step_name: str,
        workdir: Path,
    ) -> list[str]:
        rendered = template.format(
            run_name=run_name,
            step_name=step_name,
            workdir=str(workdir),
        )
        return shlex.split(rendered)

    def _status_command_candidates(
        self,
        *,
        run_name: str,
        step_name: str,
        workdir: Path,
        submit_config: SubmitConfig,
    ) -> list[list[str]]:
        if submit_config.status_command_template:
            return [
                self._render_submit_template(
                    submit_config.status_command_template,
                    run_name=run_name,
                    step_name=step_name,
                    workdir=workdir,
                )
            ]

        return [
            [self.submit_executable, "--status", "--runName", run_name],
            [self.submit_executable, "--status", run_name],
            [self.submit_executable, "status", run_name],
        ]

    def _download_command_candidates(
        self,
        *,
        run_name: str,
        step_name: str,
        workdir: Path,
        submit_config: SubmitConfig,
    ) -> list[list[str]]:
        if submit_config.download_command_template:
            return [
                self._render_submit_template(
                    submit_config.download_command_template,
                    run_name=run_name,
                    step_name=step_name,
                    workdir=workdir,
                )
            ]

        return [
            [self.submit_executable, "--download", "--runName", run_name],
            [self.submit_executable, "--download", run_name],
            [self.submit_executable, "download", run_name],
            [self.submit_executable, "--results", "--runName", run_name],
            [self.submit_executable, "--results", run_name],
            [self.submit_executable, "results", run_name],
            [self.submit_executable, "--fetch", "--runName", run_name],
            [self.submit_executable, "--get", "--runName", run_name],
        ]

    @staticmethod
    def _classify_submit_status(output_text: str) -> str:
        text = output_text.lower()
        failed_markers = (
            "failed",
            "error",
            "cancel",
            "aborted",
            "killed",
            "timed out",
        )
        success_markers = (
            "completed",
            "complete",
            "finished",
            "success",
            "done",
        )
        running_markers = (
            "running",
            "queued",
            "pending",
            "submitted",
            "starting",
            "in progress",
        )

        if any(marker in text for marker in failed_markers):
            return "failed"
        if any(marker in text for marker in success_markers):
            return "completed"
        if any(marker in text for marker in running_markers):
            return "running"
        return "unknown"

    @staticmethod
    def _combine_process_output(process: subprocess.CompletedProcess[str]) -> str:
        if process.stderr:
            return (process.stdout or "") + "\n" + process.stderr
        return process.stdout or ""

    def _query_submit_status(
        self,
        *,
        run_name: str,
        step_name: str,
        workdir: Path,
        submit_config: SubmitConfig,
        timeout: float | None = None,
    ) -> tuple[str, str]:
        candidates = self._status_command_candidates(
            run_name=run_name,
            step_name=step_name,
            workdir=workdir,
            submit_config=submit_config,
        )
        errors: list[str] = []
        for command in candidates:
            try:
                process = subprocess.run(
                    command,
                    cwd=workdir,
                    text=True,
                    capture_output=True,
                    timeout=timeout,
                    check=False,
                )
            except FileNotFoundError:
                errors.append(f"{shlex.join(command)} -> command not found")
                continue

            combined = self._combine_process_output(process)
            if process.returncode != 0:
                errors.append(
                    f"{shlex.join(command)} -> rc={process.returncode}: {combined.strip()}"
                )
                continue
            return self._classify_submit_status(combined), combined

        joined = "; ".join(errors) if errors else "no candidates"
        raise RuntimeError(
            f"Unable to query submit status for run '{run_name}'. Attempts: {joined}"
        )

    def wait_for_submit_run(
        self,
        *,
        run_name: str,
        step_name: str,
        workdir: str | Path,
        submit_config: SubmitConfig,
        poll_interval: float = 20.0,
        wait_timeout: float | None = None,
    ) -> tuple[str, str]:
        """Wait for a submitted run to complete using submit status queries."""

        base_dir = Path(workdir)
        start = time.monotonic()
        last_output = ""

        while True:
            status, output_text = self._query_submit_status(
                run_name=run_name,
                step_name=step_name,
                workdir=base_dir,
                submit_config=submit_config,
                timeout=wait_timeout,
            )
            last_output = output_text
            if status == "completed":
                return status, output_text
            if status == "failed":
                raise RuntimeError(
                    f"Remote submit run '{run_name}' failed for step '{step_name}':\n"
                    f"{output_text.strip()}"
                )
            if status == "unknown":
                return status, output_text

            if wait_timeout is not None and (time.monotonic() - start) > wait_timeout:
                raise TimeoutError(
                    f"Timed out while waiting for submit run '{run_name}' "
                    f"(step '{step_name}') after {wait_timeout} seconds.\n"
                    f"Last status output:\n{last_output.strip()}"
                )
            time.sleep(max(poll_interval, 0.1))

    def sync_submit_run_outputs(
        self,
        *,
        run_name: str,
        step_name: str,
        workdir: str | Path,
        submit_config: SubmitConfig,
        timeout: float | None = None,
    ) -> bool:
        """Try to sync/download outputs for a completed submit run."""

        base_dir = Path(workdir)
        candidates = self._download_command_candidates(
            run_name=run_name,
            step_name=step_name,
            workdir=base_dir,
            submit_config=submit_config,
        )
        for command in candidates:
            try:
                process = subprocess.run(
                    command,
                    cwd=base_dir,
                    text=True,
                    capture_output=True,
                    timeout=timeout,
                    check=False,
                )
            except FileNotFoundError:
                continue

            if process.returncode == 0:
                return True
        return False

    @staticmethod
    def _results_roots(submit_config: SubmitConfig) -> list[Path]:
        roots: list[Path] = [Path(value) for value in submit_config.results_search_dirs]
        home = Path.home()
        roots.extend(
            [
                home / "data" / "results",
                home / "data" / "results" / ".submit_cache",
            ]
        )

        unique: list[Path] = []
        seen: set[str] = set()
        for root in roots:
            key = str(root.expanduser())
            if key in seen:
                continue
            seen.add(key)
            unique.append(root.expanduser())
        return unique

    def _result_locations(
        self,
        *,
        run_name: str,
        submit_config: SubmitConfig,
    ) -> list[Path]:
        locations: list[Path] = []
        for root in self._results_roots(submit_config):
            if not root.exists():
                continue

            direct = root / run_name
            if direct.exists():
                locations.append(direct)

            for candidate in root.glob(f"{run_name}*"):
                if candidate.exists():
                    locations.append(candidate)

            submit_cache = root / ".submit_cache"
            if submit_cache.exists():
                for candidate in submit_cache.glob(f"**/{run_name}*"):
                    if candidate.exists():
                        locations.append(candidate)

        unique: list[Path] = []
        seen: set[str] = set()
        for location in locations:
            resolved = location.resolve()
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            unique.append(resolved)
        return unique

    @staticmethod
    def _copy_file(source: Path, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.resolve() == target.resolve():
            return
        shutil.copy2(source, target)

    def _sync_expected_outputs_from_results_store(
        self,
        *,
        step: QEStep,
        run_name: str,
        workdir: Path,
        submit_config: SubmitConfig,
    ) -> bool:
        locations = self._result_locations(run_name=run_name, submit_config=submit_config)
        if not locations:
            return False

        copied_any = False

        if step.output_filename:
            output_target = workdir / step.output_filename
            output_name = Path(step.output_filename).name
            for location in locations:
                candidates: list[Path] = []
                if location.is_file():
                    candidates = [location] if location.name == output_name else []
                else:
                    direct = location / step.output_filename
                    by_name = location / output_name
                    candidates = [candidate for candidate in (direct, by_name) if candidate.exists()]
                    if not candidates:
                        matches = list(location.glob(f"**/{output_name}"))
                        candidates = [match for match in matches if match.is_file()]
                if candidates:
                    self._copy_file(candidates[0], output_target)
                    copied_any = True
                    break

        for expected in step.expected_output_files:
            target = workdir / expected
            if target.exists():
                continue
            expected_name = Path(expected).name
            for location in locations:
                candidates: list[Path] = []
                if location.is_file():
                    candidates = [location] if location.name == expected_name else []
                else:
                    direct = location / expected
                    by_name = location / expected_name
                    candidates = [candidate for candidate in (direct, by_name) if candidate.exists()]
                    if not candidates:
                        matches = list(location.glob(f"**/{expected_name}"))
                        candidates = [match for match in matches if match.is_file()]
                if candidates:
                    self._copy_file(candidates[0], target)
                    copied_any = True
                    break

        for pattern in step.expected_output_globs:
            for location in locations:
                if not location.is_dir():
                    continue
                local_matches = list(location.glob(pattern))
                deep_matches = list(location.glob(f"**/{pattern}"))
                for candidate in [*local_matches, *deep_matches]:
                    if not candidate.is_file():
                        continue
                    target = workdir / candidate.name
                    self._copy_file(candidate, target)
                    copied_any = True

        return copied_any

    @staticmethod
    def _missing_expected_outputs(workdir: Path, step: QEStep) -> list[str]:
        missing: list[str] = []
        for expected in step.expected_output_files:
            if not (workdir / expected).exists():
                missing.append(expected)
        for pattern in step.expected_output_globs:
            if not list(workdir.glob(pattern)):
                missing.append(f"glob:{pattern}")
        return missing

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

        step_command = self.build_step_command(resolved_step)
        command = list(step_command)
        submitted = False
        effective_submit_config: SubmitConfig | None = None

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
            effective_submit_config = cfg
            command = self.build_submit_command(step_command, cfg)
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
                remote_run_name=(
                    effective_submit_config.run_name if effective_submit_config is not None else None
                ),
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

        retry_notes: list[str] = []
        if submitted and process.returncode != 0 and effective_submit_config is not None:
            def summarize_attempt(cmd: list[str], proc: subprocess.CompletedProcess[str]) -> str:
                combined = self._combine_process_output(proc).strip()
                return f"[{proc.returncode}] {shlex.join(cmd)} :: {combined}"

            retry_notes.append(summarize_attempt(command, process))

            retry_candidates: list[tuple[list[str], SubmitConfig]] = []

            if effective_submit_config.venue:
                venue_free = self._clone_submit_config(effective_submit_config)
                venue_free.venue = None
                retry_candidates.append(
                    (self.build_submit_command(step_command, venue_free), venue_free)
                )

            retry_candidates.append(
                (
                    self.build_submit_command_legacy(step_command, effective_submit_config),
                    effective_submit_config,
                )
            )
            if effective_submit_config.venue:
                venue_free_legacy = self._clone_submit_config(effective_submit_config)
                venue_free_legacy.venue = None
                retry_candidates.append(
                    (
                        self.build_submit_command_legacy(step_command, venue_free_legacy),
                        venue_free_legacy,
                    )
                )

            attempted: set[str] = {shlex.join(command)}
            for retry_command, retry_cfg in retry_candidates:
                key = shlex.join(retry_command)
                if key in attempted:
                    continue
                attempted.add(key)

                retry_process = subprocess.run(
                    retry_command,
                    cwd=workdir_path,
                    text=True,
                    input=stdin_text,
                    capture_output=True,
                    timeout=timeout,
                    check=False,
                    env=run_env,
                )
                retry_notes.append(summarize_attempt(retry_command, retry_process))

                if retry_process.returncode == 0:
                    process = retry_process
                    command = retry_command
                    effective_submit_config = retry_cfg
                    break

        combined_output = process.stdout
        if process.stderr:
            combined_output = process.stdout + "\n" + process.stderr
        if retry_notes and process.returncode != 0:
            combined_output = (combined_output + "\n\nRetry attempts:\n" + "\n".join(retry_notes)).strip()
        elif retry_notes and process.returncode == 0:
            combined_output = (combined_output + "\n\nRetry attempts:\n" + "\n".join(retry_notes)).strip()

        output_path.write_text(combined_output, encoding="utf-8")

        discovered_outputs = self._discover_outputs(workdir_path, resolved_step, output_path)
        job_id = self._parse_submit_job_id(process.stdout + "\n" + process.stderr)
        error_text = process.stderr if process.stderr else (process.stdout if process.returncode != 0 else "")
        if retry_notes and process.returncode != 0:
            note_text = "\n".join(retry_notes)
            if error_text:
                error_text = error_text + "\n\nRetry attempts:\n" + note_text
            else:
                error_text = "Retry attempts:\n" + note_text

        return ExecutionResult(
            command=command,
            returncode=process.returncode,
            stdout=process.stdout,
            stderr=error_text,
            workdir=workdir_path,
            input_file=input_path,
            output_file=output_path,
            expected_outputs=expected_outputs,
            discovered_outputs=discovered_outputs,
            submitted=submitted,
            remote_run_name=(
                effective_submit_config.run_name if effective_submit_config is not None else None
            ),
            remote_job_id=job_id if submitted else None,
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

    def _submit_config_for_step(
        self,
        *,
        submit_config: SubmitConfig | None,
        workflow_name: str,
        step_name: str,
        assign_step_run_names: bool,
    ) -> SubmitConfig:
        cfg = self._clone_submit_config(submit_config)

        if assign_step_run_names:
            base_name = cfg.run_name or workflow_name
            cfg.run_name = f"{base_name}-{step_name}"
        elif cfg.run_name is None:
            cfg.run_name = f"{workflow_name}-{step_name}"
        return cfg

    def run_workflow_submit(
        self,
        workflow: QEWorkflow,
        *,
        workdir: str | Path,
        submit_config: SubmitConfig | None = None,
        timeout: float | None = None,
        dry_run: bool = False,
        wait: bool = True,
        sync_outputs: bool = True,
        poll_interval: float = 20.0,
        wait_timeout: float | None = None,
        assign_step_run_names: bool = True,
        input_suffix: str = ".in",
        output_suffix: str = ".out",
        output_record_filename: str | None = "workflow_outputs.json",
    ) -> dict[str, ExecutionResult]:
        """Submit each workflow step and optionally wait/sync outputs."""

        base_dir = Path(workdir)
        base_dir.mkdir(parents=True, exist_ok=True)

        results: dict[str, ExecutionResult] = {}
        for step_name, step in workflow.iter_steps(
            input_suffix=input_suffix,
            output_suffix=output_suffix,
        ):
            step_submit_config = self._submit_config_for_step(
                submit_config=submit_config,
                workflow_name=workflow.name,
                step_name=step_name,
                assign_step_run_names=assign_step_run_names,
            )

            result = self.run_step(
                step,
                step_name=step_name,
                workdir=base_dir,
                backend="submit",
                submit_config=step_submit_config,
                timeout=timeout,
                dry_run=dry_run,
            )
            if result.remote_run_name is None:
                result.remote_run_name = step_submit_config.run_name
            results[step_name] = result
            if result.returncode != 0:
                result.remote_status = "submit_failed"
                break

            if dry_run:
                continue

            if wait and result.remote_run_name:
                try:
                    remote_status, status_text = self.wait_for_submit_run(
                        run_name=result.remote_run_name,
                        step_name=step_name,
                        workdir=base_dir,
                        submit_config=step_submit_config,
                        poll_interval=poll_interval,
                        wait_timeout=wait_timeout,
                    )
                    result.remote_status = remote_status
                    if status_text.strip():
                        result.stdout = (result.stdout + "\n" + status_text).strip()
                except RuntimeError as exc:
                    if "Unable to query submit status" in str(exc):
                        result.remote_status = "unknown"
                    else:
                        result.returncode = 1
                        error_text = str(exc)
                        if result.stderr:
                            result.stderr = result.stderr + "\n" + error_text
                        else:
                            result.stderr = error_text
                        break
                except Exception as exc:
                    result.returncode = 1
                    error_text = str(exc)
                    if result.stderr:
                        result.stderr = result.stderr + "\n" + error_text
                    else:
                        result.stderr = error_text
                    break

            if sync_outputs and result.remote_run_name:
                result.outputs_synced = self.sync_submit_run_outputs(
                    run_name=result.remote_run_name,
                    step_name=step_name,
                    workdir=base_dir,
                    submit_config=step_submit_config,
                    timeout=timeout,
                )
                copied = self._sync_expected_outputs_from_results_store(
                    step=step,
                    run_name=result.remote_run_name,
                    workdir=base_dir,
                    submit_config=step_submit_config,
                )
                result.outputs_synced = result.outputs_synced or copied

            result.discovered_outputs = self._discover_outputs(base_dir, step, result.output_file)
            if step_submit_config.require_expected_outputs and (wait or sync_outputs):
                missing_outputs = self._missing_expected_outputs(base_dir, step)
                if missing_outputs:
                    result.returncode = 1
                    missing_text = ", ".join(missing_outputs)
                    error_text = (
                        f"Expected output files not available after submit completion/sync "
                        f"for step '{step_name}': {missing_text}"
                    )
                    if result.stderr:
                        result.stderr = result.stderr + "\n" + error_text
                    else:
                        result.stderr = error_text
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
                    "submitted": result.submitted,
                    "remote_run_name": result.remote_run_name,
                    "remote_job_id": result.remote_job_id,
                    "remote_status": result.remote_status,
                    "outputs_synced": result.outputs_synced,
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
