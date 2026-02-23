from __future__ import annotations

import json
from pathlib import Path

from nanohubqe import QERunner, QEStep, SubmitConfig, silicon_bands_workflow, silicon_scf


def _touch_workflow_pseudos(workflow, workdir: Path) -> None:
    for _, step in workflow.iter_steps():
        if step.deck is None:
            continue
        pseudo_dir_raw = str(step.deck.control.get("pseudo_dir", "./pseudo"))
        pseudo_dir = Path(pseudo_dir_raw)
        if not pseudo_dir.is_absolute():
            pseudo_dir = workdir / pseudo_dir
        pseudo_dir.mkdir(parents=True, exist_ok=True)
        for species in step.deck.atomic_species:
            pseudo_path = pseudo_dir / species.pseudo_file
            if not pseudo_path.exists():
                pseudo_path.write_text("pseudo\n", encoding="utf-8")


def _write_fake_submit(script_path: Path) -> None:
    script_path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import pathlib\n"
            "import sys\n"
            "\n"
            "args = sys.argv[1:]\n"
            "workdir = pathlib.Path.cwd()\n"
            "\n"
            "def run_name_from_args(items):\n"
            "    if '--runName' in items:\n"
            "        idx = items.index('--runName')\n"
            "        if idx + 1 < len(items):\n"
            "            return items[idx + 1]\n"
            "    return 'unknown'\n"
            "\n"
            "if '--status' in args or (args and args[0] == 'status'):\n"
            "    print('status: completed')\n"
            "    raise SystemExit(0)\n"
            "\n"
            "if '--download' in args or (args and args[0] == 'download'):\n"
            "    run_name = run_name_from_args(args)\n"
            "    normalized = run_name.replace('_', '-')\n"
            "    if normalized.endswith('-scf'):\n"
            "        (workdir / 'scf.out').write_text(\n"
            "            ' total energy = -10.0 Ry\\n total energy = -11.0 Ry\\n JOB DONE.\\n',\n"
            "            encoding='utf-8',\n"
            "        )\n"
            "    if normalized.endswith('-dos'):\n"
            "        (workdir / 'qe.dos').write_text('0.0 1.0 1.0\\n1.0 2.0 3.0\\n', encoding='utf-8')\n"
            "    if normalized.endswith('-bands-pp') or run_name.endswith('bands_pp'):\n"
            "        (workdir / 'qe.bands.dat').write_text(\n"
            "            '&plot nbnd= 2, nks= 3 /\\n'\n"
            "            ' 0.500000 0.500000 0.500000\\n'\n"
            "            ' -3.0 0.0\\n'\n"
            "            ' 0.000000 0.000000 0.000000\\n'\n"
            "            ' -2.0 1.0\\n'\n"
            "            ' 1.000000 0.000000 0.000000\\n'\n"
            "            ' -1.0 2.0\\n',\n"
            "            encoding='utf-8',\n"
            "        )\n"
            "    print('downloaded')\n"
            "    raise SystemExit(0)\n"
            "\n"
            "run_name = run_name_from_args(args)\n"
            "print(f'Submitted job id: {run_name}-job')\n"
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def _write_fake_submit_without_dos(script_path: Path) -> None:
    script_path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import pathlib\n"
            "import sys\n"
            "\n"
            "args = sys.argv[1:]\n"
            "workdir = pathlib.Path.cwd()\n"
            "\n"
            "def run_name_from_args(items):\n"
            "    if '--runName' in items:\n"
            "        idx = items.index('--runName')\n"
            "        if idx + 1 < len(items):\n"
            "            return items[idx + 1]\n"
            "    return 'unknown'\n"
            "\n"
            "if '--status' in args or (args and args[0] == 'status'):\n"
            "    print('status: completed')\n"
            "    raise SystemExit(0)\n"
            "\n"
            "if '--download' in args or (args and args[0] == 'download'):\n"
            "    run_name = run_name_from_args(args)\n"
            "    normalized = run_name.replace('_', '-')\n"
            "    if normalized.endswith('-scf'):\n"
            "        (workdir / 'scf.out').write_text(\n"
            "            ' total energy = -10.0 Ry\\n total energy = -11.0 Ry\\n JOB DONE.\\n',\n"
            "            encoding='utf-8',\n"
            "        )\n"
            "    print('downloaded')\n"
            "    raise SystemExit(0)\n"
            "\n"
            "run_name = run_name_from_args(args)\n"
            "print(f'Submitted job id: {run_name}-job')\n"
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def _write_fake_submit_rc1_but_submitted(script_path: Path) -> None:
    script_path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import pathlib\n"
            "\n"
            "workdir = pathlib.Path.cwd()\n"
            "counter = workdir / 'submit_calls.txt'\n"
            "calls = int(counter.read_text(encoding='utf-8')) if counter.exists() else 0\n"
            "counter.write_text(str(calls + 1), encoding='utf-8')\n"
            "print('Run 10551771 registered 1 job instance. Mon Feb 23 09:36:49 2026')\n"
            "print('Run 10551771 instance 1 released for submission. Mon Feb 23 09:36:54 2026')\n"
            "raise SystemExit(1)\n"
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def _write_fake_submit_rc1_status_visible(script_path: Path) -> None:
    script_path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "\n"
            "args = sys.argv[1:]\n"
            "if '--status' in args or (args and args[0] == 'status'):\n"
            "    print('status: running')\n"
            "    raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def test_build_submit_command_includes_common_flags() -> None:
    runner = QERunner()
    submit_cfg = SubmitConfig(
        venue="nanohub",
        n_cpus=8,
        wall_time="01:30:00",
        manager="espresso-7.1_mpi-cleanup_pw",
        run_name="si_test",
        input_files=["qe.in"],
        env={"ESPRESSO_TMPDIR": "./tmp"},
    )

    command = runner.build_submit_command(["pw.x", "-in", "qe.in"], submit_cfg)

    assert command[0] == "submit"
    assert "-n" in command
    assert "-w" in command
    assert "--manager" in command
    assert "--venue" in command
    assert "-i" in command
    assert "--env" in command
    assert command[-3:] == ["pw.x", "-in", "qe.in"]


def test_runner_dry_run_generates_input_and_command(tmp_path) -> None:
    runner = QERunner(default_backend="local", pw_executable="pw.x")
    deck = silicon_scf()

    result = runner.run(deck, workdir=tmp_path, dry_run=True)

    assert result.returncode == 0
    assert "pw.x -in qe.in" in result.stdout
    assert result.input_file.exists()
    assert result.output_file.exists()


def test_verbose_prints_dry_run_command(tmp_path, capsys) -> None:
    runner = QERunner(default_backend="local", pw_executable="pw.x", verbose=True)

    runner.run(silicon_scf(), workdir=tmp_path, dry_run=True)

    captured = capsys.readouterr()
    assert "[nanohubqe] dry-run command: pw.x -in qe.in" in captured.out


def test_verbose_can_be_disabled_per_call(tmp_path, capsys) -> None:
    runner = QERunner(default_backend="local", pw_executable="pw.x", verbose=True)

    runner.run(silicon_scf(), workdir=tmp_path, dry_run=True, verbose=False)

    captured = capsys.readouterr()
    assert "[nanohubqe]" not in captured.out


def test_run_step_dry_run_for_postprocess_executable(tmp_path) -> None:
    runner = QERunner(default_backend="local")
    step = QEStep(executable="dos.x", input_text="&DOS\n/\n")

    result = runner.run_step(step, step_name="dos", workdir=tmp_path, dry_run=True)

    assert result.returncode == 0
    assert "dos.x -in dos.in" in result.stdout
    assert result.input_file is not None
    assert result.input_file.exists()


def test_run_step_submit_adds_generated_inputfile(tmp_path) -> None:
    runner = QERunner(default_backend="submit")
    step = QEStep(executable="dos.x", input_text="&DOS\n/\n")

    result = runner.run_step(
        step,
        step_name="dos",
        workdir=tmp_path,
        submit_config=SubmitConfig(venue="nanohub"),
        dry_run=True,
    )

    assert "submit --venue nanohub -i dos.in dos.x -in dos.in" == result.stdout


def test_run_submit_matches_nanohub_style_pw_command(tmp_path) -> None:
    runner = QERunner(default_backend="submit")
    deck = silicon_scf(pseudo_file="Si.UPF", pseudo_dir="./pseudo")

    result = runner.run(
        deck,
        workdir=tmp_path,
        submit_config=SubmitConfig(
            nodes=2,
            walltime="01:00:00",
            manager="espresso-6.8_mpi-cleanup_pw",
            run_name="si_job",
            executable_prefix="espresso-7.1",
        ),
        dry_run=True,
    )

    assert (
        "submit -n 2 -w 01:00:00 --manager espresso-7.1_mpi-cleanup_pw "
        "--runName si_job -i pseudo/Si.UPF -i qe.in espresso-7.1_pw -i qe.in"
    ) == result.stdout


def test_submit_manager_defaults_from_executable_prefix(tmp_path) -> None:
    runner = QERunner(default_backend="submit")
    deck = silicon_scf(pseudo_file="Si.UPF", pseudo_dir="./pseudo")

    result = runner.run(
        deck,
        workdir=tmp_path,
        submit_config=SubmitConfig(
            nodes=2,
            walltime="01:00:00",
            run_name="si_job",
            executable_prefix="espresso-7.1",
        ),
        dry_run=True,
    )

    assert "--manager espresso-7.1_mpi-cleanup_pw" in result.stdout


def test_submit_run_name_is_sanitized_for_submit_compatibility(tmp_path) -> None:
    runner = QERunner(default_backend="submit")
    deck = silicon_scf(pseudo_file="Si.UPF", pseudo_dir="./pseudo")

    result = runner.run(
        deck,
        workdir=tmp_path,
        submit_config=SubmitConfig(
            run_name="si-reference-remote",
            executable_prefix="espresso-7.1",
        ),
        dry_run=True,
    )

    assert "--runName si_reference_remote" in result.stdout


def test_submit_rc1_registered_release_is_treated_as_success(tmp_path) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit_rc1_but_submitted(submit_script)
    pseudo_dir = tmp_path / "pseudo"
    pseudo_dir.mkdir(parents=True, exist_ok=True)
    (pseudo_dir / "Si.UPF").write_text("pseudo\n", encoding="utf-8")

    runner = QERunner(default_backend="submit", submit_executable=str(submit_script))
    result = runner.run(
        silicon_scf(pseudo_file="Si.UPF", pseudo_dir="./pseudo"),
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="dbg-submit"),
        dry_run=False,
    )

    assert result.returncode == 0
    assert result.submitted
    assert result.remote_status == "submitted"
    assert result.remote_job_id == "10551771"
    assert (tmp_path / "submit_calls.txt").read_text(encoding="utf-8").strip() == "1"


def test_submit_rc1_with_status_visibility_is_treated_as_success(tmp_path) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit_rc1_status_visible(submit_script)
    pseudo_dir = tmp_path / "pseudo"
    pseudo_dir.mkdir(parents=True, exist_ok=True)
    (pseudo_dir / "Si.UPF").write_text("pseudo\n", encoding="utf-8")

    runner = QERunner(default_backend="submit", submit_executable=str(submit_script))
    result = runner.run(
        silicon_scf(pseudo_file="Si.UPF", pseudo_dir="./pseudo"),
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="dbg-status-visible"),
        dry_run=False,
    )

    assert result.returncode == 0
    assert result.submitted
    assert result.remote_status == "running"


def test_run_step_records_expected_and_discovered_outputs(tmp_path) -> None:
    runner = QERunner(default_backend="local")
    step = QEStep(
        executable="bash",
        args=["-lc", "touch test.dat test_extra.log"],
        input_mode="none",
        expected_output_files=["test.dat"],
        expected_output_globs=["test_*.log"],
    )

    result = runner.run_step(step, step_name="touch", workdir=tmp_path, dry_run=False)

    assert result.returncode == 0
    assert "touch.out" in result.expected_outputs
    assert "test.dat" in result.expected_outputs
    assert "glob:test_*.log" in result.expected_outputs
    discovered_names = {path.name for path in result.discovered_outputs}
    assert "test.dat" in discovered_names
    assert "test_extra.log" in discovered_names


def test_run_workflow_writes_output_record_manifest(tmp_path) -> None:
    workflow = silicon_bands_workflow()
    runner = QERunner(default_backend="local", pw_executable="pw.x")

    runner.run_workflow(workflow, workdir=tmp_path, dry_run=True)

    record_path = tmp_path / "workflow_outputs.json"
    assert record_path.exists()
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["workflow"] == "silicon_bands"
    assert "scf" in record["steps"]
    assert "bands" in record["steps"]


def test_run_workflow_submit_waits_and_syncs_outputs(tmp_path: Path) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit(submit_script)

    workflow = silicon_bands_workflow()
    _touch_workflow_pseudos(workflow, tmp_path)
    runner = QERunner(default_backend="submit", submit_executable=str(submit_script))

    results = runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="si-remote"),
        wait=True,
        sync_outputs=True,
        poll_interval=0.01,
        wait_timeout=5.0,
    )

    assert set(results) == {"scf", "bands"}
    assert results["scf"].submitted
    assert results["scf"].remote_status == "completed"
    assert results["scf"].outputs_synced
    assert results["scf"].remote_run_name == "si_remote_scf"


def test_run_workflow_submit_verbose_logs_commands(tmp_path: Path, capsys) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit(submit_script)

    workflow = silicon_bands_workflow()
    _touch_workflow_pseudos(workflow, tmp_path)
    runner = QERunner(default_backend="submit", submit_executable=str(submit_script), verbose=True)

    runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="si-remote"),
        wait=True,
        sync_outputs=True,
        poll_interval=0.01,
        wait_timeout=5.0,
    )

    captured = capsys.readouterr()
    assert "[nanohubqe] command:" in captured.out
    assert "[nanohubqe] status command:" in captured.out
    assert "[nanohubqe] download command:" in captured.out


def test_run_workflow_submit_fails_when_expected_outputs_are_missing(tmp_path: Path) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit_without_dos(submit_script)

    from nanohubqe import silicon_bands_dos_reference_workflow

    workflow = silicon_bands_dos_reference_workflow(include_plotband=False)
    _touch_workflow_pseudos(workflow, tmp_path)
    runner = QERunner(default_backend="submit", submit_executable=str(submit_script))

    results = runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="si-remote"),
        wait=True,
        sync_outputs=True,
        poll_interval=0.01,
        wait_timeout=5.0,
    )

    assert "dos" in results
    assert not results["dos"].ok
    assert "Expected output files not available" in results["dos"].stderr
