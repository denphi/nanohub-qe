"""Execution helpers for local and HUBzero submit-based Quantum ESPRESSO runs."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Mapping, Sequence

from .deck import PWInputDeck
from .workflow import QEStep, QEWorkflow


@dataclass
class SubmitConfig:
    """Configuration for remote execution via the HUBzero `submit` command."""

    # Optional venue; passed to submit only when explicitly set.
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
    stage_input_file: bool = True
    # Optional templates for status/download commands used by run_submit workflows.
    # Available placeholders: {run_name}, {step_name}, {workdir}
    status_command_template: str | None = None
    download_command_template: str | None = None
    # Optional extra roots used to locate downloaded results.
    results_search_dirs: list[str] = field(default_factory=list)
    # If True, fail a submit workflow step when expected outputs are unavailable.
    require_expected_outputs: bool = True
    # If True, align espresso manager version with executable_prefix (when possible).
    align_manager_with_executable_prefix: bool = True
    # If submit returns non-zero, try status probing and accept if the run is visible.
    accept_nonzero_submit_if_status_visible: bool = True
    # If True, sanitize run names to [A-Za-z0-9] for submit compatibility.
    sanitize_run_name: bool = True
    # Automatically apply manager file-action chaining across workflow steps.
    # None => auto-enable when manager is set.
    apply_manager_file_actions: bool | None = None
    # Environment variable used for manager file-action choreography.
    manager_file_action_env: str = "OPTICDFTFileAction"
    # Locator file passed to FETCH stages when available (manager-specific).
    manager_file_action_locator: str | None = "OPTICDFT.wavefilelocation"
    # Stage pseudopotentials in submit run root (e.g., "Si.UPF" instead of "pseudo/Si.UPF").
    # This matches submit environments that flatten uploaded files into "./".
    flatten_pseudo_inputs: bool = True


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
    verbose: bool = False

    def _is_verbose(self, verbose: bool | None) -> bool:
        if verbose is None:
            return self.verbose
        return verbose

    @staticmethod
    def _verbose_print(enabled: bool, message: str) -> None:
        if enabled:
            print(message, flush=True)

    @staticmethod
    def _debug_snippet(text: str, max_chars: int = 500) -> str:
        cleaned = " ".join(text.split())
        if len(cleaned) <= max_chars:
            return cleaned
        return cleaned[: max_chars - 3] + "..."

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
        *,
        use_double_dash: bool = False,
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
        if use_double_dash:
            command.append("--")
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
            align_manager_with_executable_prefix=config.align_manager_with_executable_prefix,
            accept_nonzero_submit_if_status_visible=config.accept_nonzero_submit_if_status_visible,
            sanitize_run_name=config.sanitize_run_name,
            apply_manager_file_actions=config.apply_manager_file_actions,
            manager_file_action_env=config.manager_file_action_env,
            manager_file_action_locator=config.manager_file_action_locator,
            flatten_pseudo_inputs=config.flatten_pseudo_inputs,
        )

    @staticmethod
    def _series_file_action(step_index: int, total_steps: int) -> str:
        if total_steps <= 1:
            return "CREATESTORE:DESTROY"
        if step_index == 0:
            return "CREATESTORE:SAVE"
        if step_index == (total_steps - 1):
            return "FETCH:DESTROY"
        return "FETCH:SAVE"

    @staticmethod
    def _espresso_version_token(value: str | None) -> str | None:
        if not value:
            return None
        match = re.match(r"^(espresso-\d+(?:\.\d+)*)(?:_|$)", value)
        if match:
            return match.group(1)
        return None

    def _normalize_submit_manager(self, config: SubmitConfig) -> SubmitConfig:
        if not config.align_manager_with_executable_prefix:
            return config

        executable_token = self._espresso_version_token(config.executable_prefix)
        if executable_token is None:
            return config

        if config.manager is None:
            config.manager = f"{executable_token}_mpi-cleanup_pw"
            return config

        manager_token = self._espresso_version_token(config.manager)
        if manager_token is None or manager_token == executable_token:
            return config

        suffix = config.manager[len(manager_token) :]
        if not suffix:
            suffix = "_mpi-cleanup_pw"
        config.manager = f"{executable_token}{suffix}"
        return config

    @staticmethod
    def _sanitize_submit_run_name(run_name: str) -> str:
        sanitized = re.sub(r"[^A-Za-z0-9]", "", run_name)
        if not sanitized:
            return "run"
        return sanitized

    @staticmethod
    def _submit_pseudo_inputs(step: QEStep, config: SubmitConfig) -> list[str]:
        if step.deck is None:
            return []

        if config.flatten_pseudo_inputs:
            return [Path(species.pseudo_file).name for species in step.deck.atomic_species]

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
        for entry in self._submit_pseudo_inputs(step, config):
            entry_name = Path(entry).name
            basename_exists = any(Path(existing).name == entry_name for existing in config.input_files)
            if entry not in config.input_files and not basename_exists:
                config.input_files.append(entry)
        if config.stage_input_file and input_filename and input_filename not in config.input_files:
            config.input_files.append(input_filename)
        config.env.update(step.env)
        config = self._normalize_submit_manager(config)
        if config.venue and config.venue.strip().lower() == "nanohub":
            config.venue = None
        if config.sanitize_run_name and config.run_name:
            config.run_name = self._sanitize_submit_run_name(config.run_name)
        return config

    @staticmethod
    def _stage_submit_pseudo_inputs(
        step: QEStep,
        *,
        workdir: Path,
        submit_config: SubmitConfig,
        verbose: bool,
    ) -> None:
        if step.deck is None or not submit_config.flatten_pseudo_inputs:
            return

        pseudo_dir_raw = str(step.deck.control.get("pseudo_dir", "./pseudo"))
        pseudo_dir = Path(pseudo_dir_raw)

        for species in step.deck.atomic_species:
            source_rel = Path(species.pseudo_file)
            if str(pseudo_dir) not in {"", "."} and not source_rel.is_absolute():
                source_rel = pseudo_dir / source_rel

            source = source_rel if source_rel.is_absolute() else (workdir / source_rel)
            target = workdir / Path(species.pseudo_file).name

            if not source.exists() and target.exists():
                continue
            if not source.exists():
                continue
            if source.resolve() == target.resolve():
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            if verbose:
                print(
                    "[nanohubqe] staged pseudo for submit root: "
                    f"{source.as_posix()} -> {target.as_posix()}",
                    flush=True,
                )

    @staticmethod
    def _parse_submit_job_id(text: str) -> str | None:
        patterns = [
            r"\bjob\s+id\s*[:=#]?\s*([A-Za-z0-9._:-]+)",
            r"\brun(?:\s+id)?\s*[:=#]?\s*([A-Za-z0-9._:-]+)",
            r"\bsubmitted(?:\s+as)?\s+([A-Za-z0-9._:-]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _is_submit_submission_accepted(process: subprocess.CompletedProcess[str]) -> bool:
        """Detect submit variants that return non-zero even after successful enqueue."""

        text = ((process.stdout or "") + "\n" + (process.stderr or "")).lower()
        has_registered = "registered" in text and "job instance" in text
        has_release = "released for submission" in text
        return has_registered and has_release

    @staticmethod
    def _is_submit_fatal_error(process: subprocess.CompletedProcess[str]) -> bool:
        """Detect submit failures that should not be treated as accepted."""

        text = ((process.stdout or "") + "\n" + (process.stderr or "")).lower()
        fatal_markers = (
            "all specified venues are out of service",
            "please select another venue or attempt execution at a later time",
            "command line argument parsing failed",
            "runname contains non-alphanumeric characters",
            "invalid manager",
            "unknown manager",
        )
        return any(marker in text for marker in fatal_markers)

    @staticmethod
    def _is_submit_venue_outage_error(process: subprocess.CompletedProcess[str]) -> bool:
        text = ((process.stdout or "") + "\n" + (process.stderr or "")).lower()
        markers = (
            "all specified venues are out of service",
            "please select another venue or attempt execution at a later time",
        )
        return any(marker in text for marker in markers)

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
        # The default nanoHUB submit help does not advertise download/results/fetch
        # commands. Only run an explicit download template when provided.
        return []

    @staticmethod
    def _classify_submit_status(output_text: str) -> str:
        text = output_text.lower()
        state_match = re.search(r"\bstate\s*[:=]\s*([a-z])\b", text)
        if state_match:
            state_code = state_match.group(1)
            if state_code in {"q", "r", "w", "h", "p", "s"}:
                return "running"
            if state_code in {"c", "d"}:
                return "completed"
            if state_code in {"f", "e", "k", "x"}:
                return "failed"

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
        verbose: bool | None = None,
    ) -> tuple[str, str]:
        verbose_enabled = self._is_verbose(verbose)
        candidates = self._status_command_candidates(
            run_name=run_name,
            step_name=step_name,
            workdir=workdir,
            submit_config=submit_config,
        )
        errors: list[str] = []
        for command in candidates:
            self._verbose_print(
                verbose_enabled,
                f"[nanohubqe] status command: {shlex.join(command)} (cwd={workdir})",
            )
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
            self._verbose_print(
                verbose_enabled,
                f"[nanohubqe] status rc={process.returncode}",
            )
            if combined.strip():
                self._verbose_print(
                    verbose_enabled,
                    f"[nanohubqe] status out: {self._debug_snippet(combined)}",
                )
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
        verbose: bool | None = None,
        show_progress: bool = True,
    ) -> tuple[str, str]:
        """Wait for a submitted run to complete using submit status queries."""

        verbose_enabled = self._is_verbose(verbose)
        base_dir = Path(workdir)
        start = time.monotonic()
        last_output = ""
        if show_progress:
            print(
                f"[nanohubqe] waiting for step '{step_name}' "
                f"(run '{run_name}') ...",
                flush=True,
            )

        while True:
            status, output_text = self._query_submit_status(
                run_name=run_name,
                step_name=step_name,
                workdir=base_dir,
                submit_config=submit_config,
                timeout=wait_timeout,
                verbose=verbose_enabled,
            )
            self._verbose_print(
                verbose_enabled,
                f"[nanohubqe] submit status for '{run_name}' ({step_name}): {status}",
            )
            elapsed_s = time.monotonic() - start
            if show_progress:
                print(
                    f"[nanohubqe] step '{step_name}' status={status} "
                    f"elapsed={elapsed_s:.0f}s",
                    flush=True,
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
        verbose: bool | None = None,
    ) -> bool:
        """Try to sync/download outputs for a completed submit run."""

        verbose_enabled = self._is_verbose(verbose)
        base_dir = Path(workdir)
        candidates = self._download_command_candidates(
            run_name=run_name,
            step_name=step_name,
            workdir=base_dir,
            submit_config=submit_config,
        )
        if not candidates:
            self._verbose_print(
                verbose_enabled,
                "[nanohubqe] no download command candidates configured; skipping submit download phase",
            )
            return False
        for command in candidates:
            self._verbose_print(
                verbose_enabled,
                f"[nanohubqe] download command: {shlex.join(command)} (cwd={base_dir})",
            )
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

            self._verbose_print(
                verbose_enabled,
                f"[nanohubqe] download rc={process.returncode}",
            )
            combined = self._combine_process_output(process)
            if combined.strip():
                self._verbose_print(
                    verbose_enabled,
                    f"[nanohubqe] download out: {self._debug_snippet(combined)}",
                )
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

    @staticmethod
    def _copy_tree(source: Path, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.resolve() == target.resolve():
            return
        if target.exists():
            shutil.copytree(source, target, dirs_exist_ok=True)
            return
        shutil.copytree(source, target)

    @staticmethod
    def _step_save_dir(workdir: Path, step: QEStep) -> Path | None:
        if step.deck is None:
            return None
        prefix_raw = step.deck.control.get("prefix")
        if prefix_raw is None:
            return None
        prefix = str(prefix_raw).strip()
        if not prefix:
            return None
        outdir_raw = str(step.deck.control.get("outdir", "./tmp"))
        outdir = Path(outdir_raw)
        if not outdir.is_absolute():
            outdir = workdir / outdir
        return outdir / f"{prefix}.save"

    @staticmethod
    def _submit_input_path(workdir: Path, path: Path) -> str:
        candidate = path
        if path.is_absolute():
            try:
                candidate = path.relative_to(workdir)
            except ValueError:
                return path.as_posix()
        return candidate.as_posix()

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

        if step.deck is not None:
            prefix_raw = step.deck.control.get("prefix")
            if prefix_raw is not None and str(prefix_raw).strip():
                prefix = str(prefix_raw).strip()
                outdir_raw = str(step.deck.control.get("outdir", "./tmp"))
                outdir_rel = Path(outdir_raw)
                if outdir_rel.is_absolute():
                    outdir_rel = Path(outdir_rel.name)
                expected_rel = outdir_rel / f"{prefix}.save"
                target_dir = workdir / expected_rel
                if not target_dir.exists():
                    for location in locations:
                        if not location.is_dir():
                            continue
                        candidates: list[Path] = []
                        direct = location / expected_rel
                        if direct.is_dir():
                            candidates.append(direct)
                        if not candidates:
                            matches = list(location.glob(f"**/{prefix}.save"))
                            candidates = [match for match in matches if match.is_dir()]
                        if candidates:
                            self._copy_tree(candidates[0], target_dir)
                            copied_any = True
                            break

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

    @staticmethod
    def _output_indicates_success(output_file: Path) -> bool:
        if not output_file.exists():
            return False
        try:
            text = output_file.read_text(encoding="utf-8", errors="ignore").lower()
        except OSError:
            return False

        success_markers = (
            "job done",
            "simulation done",
            "convergence has been achieved",
        )
        fatal_markers = (
            "error in routine",
            "all specified venues are out of service",
            "command line argument parsing failed",
        )
        return any(marker in text for marker in success_markers) and not any(
            marker in text for marker in fatal_markers
        )

    @staticmethod
    def _submit_qe_command_variants(step: QEStep, step_command: list[str]) -> list[list[str]]:
        variants: list[list[str]] = [list(step_command)]
        seen: set[str] = {shlex.join(step_command)}

        if step.input_mode == "flag" and step.input_filename:
            prefix = list(step_command)
            if (
                len(step_command) >= 2
                and step_command[-2] == step.input_flag
                and step_command[-1] == step.input_filename
            ):
                prefix = list(step_command[:-2])

            candidate_variants = [
                prefix,
                [*prefix, "-i", step.input_filename],
                [*prefix, "-in", step.input_filename],
                [*prefix, step.input_filename],
            ]
            for candidate in candidate_variants:
                key = shlex.join(candidate)
                if key in seen:
                    continue
                seen.add(key)
                variants.append(candidate)

        return variants

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
        verbose: bool | None = None,
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
            verbose=verbose,
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
        verbose: bool | None = None,
    ) -> ExecutionResult:
        """Execute a single workflow step."""

        verbose_enabled = self._is_verbose(verbose)
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

            if not dry_run:
                self._stage_submit_pseudo_inputs(
                    resolved_step,
                    workdir=workdir_path,
                    submit_config=cfg,
                    verbose=verbose_enabled,
                )
                missing_submit_inputs: list[str] = []
                optional_inputs = set(resolved_step.submit_input_files) if resolved_step.allow_missing_submit_input_files else set()
                for item in cfg.input_files:
                    if item in optional_inputs:
                        continue
                    candidate = Path(item)
                    if not candidate.is_absolute():
                        candidate = workdir_path / candidate
                    if not candidate.exists():
                        missing_submit_inputs.append(item)
                if missing_submit_inputs:
                    missing_text = ", ".join(missing_submit_inputs)
                    error_text = (
                        "Submit input files are missing in workdir "
                        f"'{workdir_path}': {missing_text}"
                    )
                    output_path.write_text(error_text + "\n", encoding="utf-8")
                    return ExecutionResult(
                        command=command,
                        returncode=1,
                        stdout="",
                        stderr=error_text,
                        workdir=workdir_path,
                        input_file=input_path,
                        output_file=output_path,
                        expected_outputs=expected_outputs,
                        discovered_outputs=self._discover_outputs(workdir_path, resolved_step, output_path),
                        submitted=True,
                        remote_run_name=cfg.run_name,
                        remote_status="submit_failed",
                    )

            if input_path is not None and resolved_step.deck is not None:
                # Remote submit generally stages input files by basename in run root,
                # so force pseudo_dir to current directory for submit-generated decks.
                submit_control = dict(resolved_step.deck.control)
                submit_control["pseudo_dir"] = "./"
                submit_deck = replace(resolved_step.deck, control=submit_control)
                input_path.write_text(submit_deck.to_string(), encoding="utf-8")

        if dry_run:
            stdout = shlex.join(command)
            self._verbose_print(
                verbose_enabled,
                f"[nanohubqe] dry-run command: {stdout} (cwd={workdir_path})",
            )
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
        self._verbose_print(
            verbose_enabled,
            f"[nanohubqe] command: {shlex.join(command)} (cwd={workdir_path})",
        )
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
        self._verbose_print(
            verbose_enabled,
            f"[nanohubqe] rc={process.returncode}",
        )

        retry_notes: list[str] = []
        submit_remote_status: str | None = None
        submit_fatal_error = submitted and self._is_submit_fatal_error(process)
        if submit_fatal_error:
            self._verbose_print(
                verbose_enabled,
                "[nanohubqe] fatal submit error detected; skipping submit-acceptance probes",
            )
        submit_accepted = submitted and self._is_submit_submission_accepted(process)
        if submit_accepted:
            submit_remote_status = "submitted"
        if (
            submitted
            and not submit_accepted
            and not submit_fatal_error
            and process.returncode != 0
            and effective_submit_config is not None
            and effective_submit_config.accept_nonzero_submit_if_status_visible
            and effective_submit_config.run_name
        ):
            status_probe_errors: list[str] = []
            status_probe_text = ""
            for _ in range(3):
                try:
                    status, status_output = self._query_submit_status(
                        run_name=effective_submit_config.run_name,
                        step_name=step_name,
                        workdir=workdir_path,
                        submit_config=effective_submit_config,
                        timeout=timeout,
                        verbose=verbose_enabled,
                    )
                    status_probe_text = status_output
                    submit_accepted = True
                    submit_remote_status = status if status != "unknown" else "submitted"
                    self._verbose_print(
                        verbose_enabled,
                        (
                            "[nanohubqe] non-zero submit accepted via status probe: "
                            f"{submit_remote_status}"
                        ),
                    )
                    break
                except Exception as exc:  # pragma: no cover - defensive fallback
                    status_probe_errors.append(str(exc))
                    time.sleep(0.5)

            if submit_accepted and status_probe_text.strip():
                process = subprocess.CompletedProcess(
                    args=process.args,
                    returncode=process.returncode,
                    stdout=(process.stdout + "\n" + status_probe_text).strip(),
                    stderr=process.stderr,
                )
            elif status_probe_errors:
                retry_notes.append("Status probe attempts:\n" + "\n".join(status_probe_errors))

        if (
            submitted
            and process.returncode != 0
            and effective_submit_config is not None
            and not submit_accepted
            and not submit_fatal_error
        ):
            def summarize_attempt(cmd: list[str], proc: subprocess.CompletedProcess[str]) -> str:
                combined = self._combine_process_output(proc).strip()
                return f"[{proc.returncode}] {shlex.join(cmd)} :: {combined}"

            retry_notes.append(summarize_attempt(command, process))

            variant_configs: list[SubmitConfig] = [self._clone_submit_config(effective_submit_config)]

            def add_variant(*, venue: bool | None = None, manager: bool | None = None, input_flag: str | None = None):
                variant = self._clone_submit_config(effective_submit_config)
                if venue is False:
                    variant.venue = None
                if manager is False:
                    variant.manager = None
                if input_flag is not None:
                    variant.program_input_flag = input_flag
                variant_configs.append(variant)

            if effective_submit_config.venue:
                add_variant(venue=False)
            if effective_submit_config.manager:
                add_variant(manager=False)
            if effective_submit_config.venue and effective_submit_config.manager:
                add_variant(venue=False, manager=False)
            if effective_submit_config.program_input_flag != "-in":
                add_variant(input_flag="-in")
                if effective_submit_config.venue:
                    add_variant(venue=False, input_flag="-in")
                if effective_submit_config.manager:
                    add_variant(manager=False, input_flag="-in")
                if effective_submit_config.venue and effective_submit_config.manager:
                    add_variant(venue=False, manager=False, input_flag="-in")

            if effective_submit_config.executable_prefix or effective_submit_config.executable_map:
                add_variant(manager=False, input_flag="-in")

            qe_command_variants = self._submit_qe_command_variants(resolved_step, step_command)
            retry_candidates: list[tuple[list[str], SubmitConfig]] = []
            for variant in variant_configs:
                for qe_variant in qe_command_variants:
                    retry_candidates.append((self.build_submit_command(qe_variant, variant), variant))
                    retry_candidates.append(
                        (
                            self.build_submit_command(
                                qe_variant,
                                variant,
                                use_double_dash=True,
                            ),
                            variant,
                        )
                    )
                    retry_candidates.append(
                        (self.build_submit_command_legacy(qe_variant, variant), variant)
                    )

            attempted: set[str] = {shlex.join(command)}
            for retry_command, retry_cfg in retry_candidates:
                key = shlex.join(retry_command)
                if key in attempted:
                    continue
                attempted.add(key)

                self._verbose_print(
                    verbose_enabled,
                    f"[nanohubqe] retry command: {shlex.join(retry_command)} (cwd={workdir_path})",
                )
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
                self._verbose_print(
                    verbose_enabled,
                    f"[nanohubqe] retry rc={retry_process.returncode}",
                )
                retry_notes.append(summarize_attempt(retry_command, retry_process))

                if self._is_submit_fatal_error(retry_process):
                    process = retry_process
                    command = retry_command
                    submit_fatal_error = True
                    self._verbose_print(
                        verbose_enabled,
                        "[nanohubqe] fatal submit error detected during retry; aborting retries",
                    )
                    break

                retry_accepted = self._is_submit_submission_accepted(retry_process)
                if retry_process.returncode == 0 or retry_accepted:
                    process = retry_process
                    command = retry_command
                    effective_submit_config = retry_cfg
                    submit_accepted = retry_accepted
                    if retry_accepted:
                        submit_remote_status = "submitted"
                    break

        if submitted and not submit_accepted:
            submit_accepted = self._is_submit_submission_accepted(process)
            if submit_accepted:
                submit_remote_status = "submitted"

        if (
            submitted
            and not submit_accepted
            and not submit_fatal_error
            and effective_submit_config is not None
            and effective_submit_config.accept_nonzero_submit_if_status_visible
            and effective_submit_config.run_name
        ):
            status_probe_errors: list[str] = []
            status_probe_text = ""
            for _ in range(3):
                try:
                    status, status_output = self._query_submit_status(
                        run_name=effective_submit_config.run_name,
                        step_name=step_name,
                        workdir=workdir_path,
                        submit_config=effective_submit_config,
                        timeout=timeout,
                        verbose=verbose_enabled,
                    )
                    status_probe_text = status_output
                    # If status command returns successfully, the run exists remotely.
                    submit_accepted = True
                    submit_remote_status = status if status != "unknown" else "submitted"
                    break
                except Exception as exc:  # pragma: no cover - defensive fallback
                    status_probe_errors.append(str(exc))
                    time.sleep(0.5)

            if submit_accepted and status_probe_text.strip():
                process = subprocess.CompletedProcess(
                    args=process.args,
                    returncode=process.returncode,
                    stdout=(process.stdout + "\n" + status_probe_text).strip(),
                    stderr=process.stderr,
                )
            elif status_probe_errors:
                retry_notes.append("Status probe attempts:\n" + "\n".join(status_probe_errors))

        effective_returncode = 0 if (submitted and submit_accepted) else process.returncode

        combined_output = process.stdout
        if process.stderr:
            combined_output = process.stdout + "\n" + process.stderr
        if retry_notes and effective_returncode != 0:
            combined_output = (combined_output + "\n\nRetry attempts:\n" + "\n".join(retry_notes)).strip()
        elif retry_notes and effective_returncode == 0:
            combined_output = (combined_output + "\n\nRetry attempts:\n" + "\n".join(retry_notes)).strip()

        output_path.write_text(combined_output, encoding="utf-8")

        discovered_outputs = self._discover_outputs(workdir_path, resolved_step, output_path)
        job_id = self._parse_submit_job_id(process.stdout + "\n" + process.stderr)
        error_text = process.stderr if process.stderr else (process.stdout if effective_returncode != 0 else "")
        if retry_notes and effective_returncode != 0:
            note_text = "\n".join(retry_notes)
            if error_text:
                error_text = error_text + "\n\nRetry attempts:\n" + note_text
            else:
                error_text = "Retry attempts:\n" + note_text

        return ExecutionResult(
            command=command,
            returncode=effective_returncode,
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
            remote_status=(submit_remote_status if submitted and effective_returncode == 0 else None),
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
        verbose: bool | None = None,
        input_suffix: str = ".in",
        output_suffix: str = ".out",
        output_record_filename: str | None = "workflow_outputs.json",
        auto_prepare_pseudopotentials: bool = True,
        pseudo_source_urls: Sequence[str] | None = None,
        pseudo_local_search_dirs: Sequence[str | Path] | None = None,
        pseudo_timeout: float = 20.0,
        pseudo_overwrite: bool = False,
    ) -> dict[str, ExecutionResult]:
        """Run each step in a `QEWorkflow` sequentially."""

        base_dir = Path(workdir)
        base_dir.mkdir(parents=True, exist_ok=True)

        if auto_prepare_pseudopotentials and not dry_run:
            from .pseudo import ensure_workflow_pseudopotentials

            ensure_workflow_pseudopotentials(
                workflow,
                workdir=base_dir,
                source_urls=pseudo_source_urls,
                local_search_dirs=pseudo_local_search_dirs,
                timeout=pseudo_timeout,
                overwrite=pseudo_overwrite,
            )

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
                verbose=verbose,
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
            cfg.run_name = f"{base_name}_{step_name}"
        elif cfg.run_name is None:
            cfg.run_name = f"{workflow_name}_{step_name}"
        return cfg

    def run_workflow_submit(
        self,
        workflow: QEWorkflow,
        *,
        workdir: str | Path,
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
        show_wait_feedback: bool = True,
    ) -> dict[str, ExecutionResult]:
        """Submit each workflow step and optionally wait/sync outputs."""

        verbose_enabled = self._is_verbose(verbose)
        base_dir = Path(workdir)
        base_dir.mkdir(parents=True, exist_ok=True)

        if auto_prepare_pseudopotentials and not dry_run:
            from .pseudo import ensure_workflow_pseudopotentials

            ensure_workflow_pseudopotentials(
                workflow,
                workdir=base_dir,
                source_urls=pseudo_source_urls,
                local_search_dirs=pseudo_local_search_dirs,
                timeout=pseudo_timeout,
                overwrite=pseudo_overwrite,
            )

        if submit_config is None:
            apply_manager_file_actions = False
            manager_file_action_env = "OPTICDFTFileAction"
            manager_file_action_locator = "OPTICDFT.wavefilelocation"
        else:
            if submit_config.apply_manager_file_actions is None:
                apply_manager_file_actions = bool(submit_config.manager)
            else:
                apply_manager_file_actions = bool(submit_config.apply_manager_file_actions)
            manager_file_action_env = submit_config.manager_file_action_env
            manager_file_action_locator = submit_config.manager_file_action_locator

        total_steps = len(workflow.order)
        shared_submit_inputs: list[str] = []
        results: dict[str, ExecutionResult] = {}
        for step_index, (step_name, step) in enumerate(
            workflow.iter_steps(
            input_suffix=input_suffix,
            output_suffix=output_suffix,
            )
        ):
            self._verbose_print(
                verbose_enabled,
                f"[nanohubqe] workflow step: {step_name}",
            )
            step_submit_config = self._submit_config_for_step(
                submit_config=submit_config,
                workflow_name=workflow.name,
                step_name=step_name,
                assign_step_run_names=assign_step_run_names,
            )
            for item in shared_submit_inputs:
                if item not in step_submit_config.input_files:
                    step_submit_config.input_files.append(item)

            if (
                apply_manager_file_actions
                and manager_file_action_env
                and manager_file_action_env not in step_submit_config.env
                and manager_file_action_env not in step.env
            ):
                file_action = self._series_file_action(
                    step_index,
                    total_steps,
                )
                step_submit_config.env[manager_file_action_env] = file_action

                if file_action.startswith("FETCH") and manager_file_action_locator:
                    locator_path = Path(manager_file_action_locator)
                    if not locator_path.is_absolute():
                        locator_path = base_dir / locator_path
                    if locator_path.exists():
                        locator_item = self._submit_input_path(base_dir, locator_path)
                        if locator_item not in step_submit_config.input_files:
                            step_submit_config.input_files.append(locator_item)
                    else:
                        self._verbose_print(
                            verbose_enabled,
                            (
                                "[nanohubqe] manager FETCH action requested but locator "
                                f"file not found: {manager_file_action_locator}"
                            ),
                        )

            result = self.run_step(
                step,
                step_name=step_name,
                workdir=base_dir,
                backend="submit",
                submit_config=step_submit_config,
                timeout=timeout,
                dry_run=dry_run,
                verbose=verbose_enabled,
            )
            if result.remote_run_name is None:
                result.remote_run_name = step_submit_config.run_name
            results[step_name] = result
            if result.returncode != 0:
                result.remote_status = "submit_failed"
                self._verbose_print(
                    verbose_enabled,
                    f"[nanohubqe] step '{step_name}' failed rc={result.returncode}",
                )
                if result.stderr:
                    self._verbose_print(
                        verbose_enabled,
                        f"[nanohubqe] step '{step_name}' error: {self._debug_snippet(result.stderr)}",
                    )
                break

            step_save_dir = self._step_save_dir(base_dir, step)
            if step_save_dir is not None and step_save_dir.exists():
                save_input = self._submit_input_path(base_dir, step_save_dir)
                if save_input not in shared_submit_inputs:
                    shared_submit_inputs.append(save_input)
                    self._verbose_print(
                        verbose_enabled,
                        f"[nanohubqe] staging shared submit input for next steps: {save_input}",
                    )

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
                        verbose=verbose_enabled,
                        show_progress=show_wait_feedback,
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

                if (
                    wait
                    and step_submit_config.require_expected_outputs
                    and result.remote_status in {None, "unknown"}
                ):
                    if self._output_indicates_success(result.output_file):
                        result.remote_status = "completed"
                        self._verbose_print(
                            verbose_enabled,
                            (
                                f"[nanohubqe] step '{step_name}' inferred completed "
                                "from output log despite unknown submit status"
                            ),
                        )
                    else:
                        result.returncode = 1
                        error_text = (
                            f"Remote status for step '{step_name}' is unknown; cannot safely continue "
                            "workflow dependencies. Configure SubmitConfig.status_command_template "
                            "to a command that reports a parseable state, or set "
                            "SubmitConfig.require_expected_outputs=False to proceed."
                        )
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
                    verbose=verbose_enabled,
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
