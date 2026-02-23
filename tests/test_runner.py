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
            "    lowered = run_name.lower()\n"
            "    if lowered.endswith('scf'):\n"
            "        (workdir / 'scf.out').write_text(\n"
            "            ' total energy = -10.0 Ry\\n total energy = -11.0 Ry\\n JOB DONE.\\n',\n"
            "            encoding='utf-8',\n"
            "        )\n"
            "    if lowered.endswith('dos'):\n"
            "        (workdir / 'qe.dos').write_text('0.0 1.0 1.0\\n1.0 2.0 3.0\\n', encoding='utf-8')\n"
            "    if lowered.endswith('bandspp'):\n"
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
            "    lowered = run_name.lower()\n"
            "    if lowered.endswith('scf'):\n"
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
            "import pathlib\n"
            "import sys\n"
            "\n"
            "args = sys.argv[1:]\n"
            "workdir = pathlib.Path.cwd()\n"
            "if '--status' in args or (args and args[0] == 'status'):\n"
            "    print('status: running')\n"
            "    raise SystemExit(0)\n"
            "counter = workdir / 'submit_attempts.txt'\n"
            "attempts = int(counter.read_text(encoding='utf-8')) if counter.exists() else 0\n"
            "counter.write_text(str(attempts + 1), encoding='utf-8')\n"
            "raise SystemExit(1)\n"
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def _write_fake_submit_status_unknown(script_path: Path) -> None:
    script_path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import pathlib\n"
            "import sys\n"
            "\n"
            "args = sys.argv[1:]\n"
            "workdir = pathlib.Path.cwd()\n"
            "if '--status' in args or (args and args[0] == 'status'):\n"
            "    print('status: ???')\n"
            "    raise SystemExit(0)\n"
            "counter = workdir / 'submit_attempts_unknown.txt'\n"
            "attempts = int(counter.read_text(encoding='utf-8')) if counter.exists() else 0\n"
            "counter.write_text(str(attempts + 1), encoding='utf-8')\n"
            "raise SystemExit(1)\n"
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def _write_fake_submit_status_unknown_but_done(script_path: Path) -> None:
    script_path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import pathlib\n"
            "import sys\n"
            "\n"
            "args = sys.argv[1:]\n"
            "workdir = pathlib.Path.cwd()\n"
            "if '--status' in args or (args and args[0] == 'status'):\n"
            "    print('status: ???')\n"
            "    raise SystemExit(0)\n"
            "log = workdir / 'submit_unknown_but_done_calls.txt'\n"
            "calls = int(log.read_text(encoding='utf-8')) if log.exists() else 0\n"
            "log.write_text(str(calls + 1), encoding='utf-8')\n"
            "print('Simulation Done at ncn@negishi Mon Feb 23 11:26:58 2026')\n"
            "print('JOB DONE.')\n"
            "raise SystemExit(0)\n"
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def _write_fake_submit_out_of_service(script_path: Path) -> None:
    script_path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "\n"
            "args = sys.argv[1:]\n"
            "if '--status' in args or (args and args[0] == 'status'):\n"
            "    print('status: submitted')\n"
            "    raise SystemExit(0)\n"
            "print('All specified venues are out of service.')\n"
            "print('Please select another venue or attempt execution at a later time.')\n"
            "raise SystemExit(8)\n"
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def _write_fake_submit_out_of_service_when_venue(script_path: Path) -> None:
    script_path.write_text(
        (
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "\n"
            "args = sys.argv[1:]\n"
            "if '--status' in args or (args and args[0] == 'status'):\n"
            "    print('status: completed')\n"
            "    raise SystemExit(0)\n"
            "if '--venue' in args:\n"
            "    print('All specified venues are out of service.')\n"
            "    print('Please select another venue or attempt execution at a later time.')\n"
            "    raise SystemExit(8)\n"
            "print('Submitted job id: no-venue-job')\n"
            "raise SystemExit(0)\n"
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def test_build_submit_command_includes_common_flags() -> None:
    runner = QERunner()
    submit_cfg = SubmitConfig(
        venue="clusterx",
        n_cpus=8,
        wall_time="01:30:00",
        manager="espresso-7.1_mpi-cleanup_pw",
        run_name="sitest",
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
        submit_config=SubmitConfig(venue="clusterx"),
        dry_run=True,
    )

    assert "submit --venue clusterx -i dos.in dos.x -in dos.in" == result.stdout


def test_run_step_submit_omits_default_nanohub_venue(tmp_path) -> None:
    runner = QERunner(default_backend="submit")
    step = QEStep(executable="dos.x", input_text="&DOS\n/\n")

    result = runner.run_step(
        step,
        step_name="dos",
        workdir=tmp_path,
        submit_config=SubmitConfig(venue="nanohub"),
        dry_run=True,
    )

    assert "--venue" not in result.stdout


def test_run_step_submit_optional_inputs_can_be_missing(tmp_path) -> None:
    runner = QERunner(default_backend="submit")
    step = QEStep(
        executable="epsilon.x",
        input_text="&inputpp\n/\n",
        submit_input_files=["OPTICDFT.wavefilelocation"],
        allow_missing_submit_input_files=True,
    )

    result = runner.run_step(
        step,
        step_name="optical",
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="optic"),
        dry_run=True,
    )

    assert result.returncode == 0
    assert "-i OPTICDFT.wavefilelocation" in result.stdout


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
            run_name="sijob",
            executable_prefix="espresso-7.1",
        ),
        dry_run=True,
    )

    assert (
        "submit -n 2 -w 01:00:00 --manager espresso-7.1_mpi-cleanup_pw "
        "--runName sijob -i Si.UPF -i qe.in espresso-7.1_pw -i qe.in"
    ) == result.stdout

    text = result.input_file.read_text(encoding="utf-8")
    assert " pseudo_dir = './'," in text


def test_submit_stages_flattened_pseudo_inputs_in_workdir_root(tmp_path: Path) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit(submit_script)

    pseudo_dir = tmp_path / "pseudo"
    pseudo_dir.mkdir(parents=True, exist_ok=True)
    (pseudo_dir / "Si.UPF").write_text("pseudo\n", encoding="utf-8")

    runner = QERunner(default_backend="submit", submit_executable=str(submit_script))
    result = runner.run(
        silicon_scf(pseudo_file="Si.UPF", pseudo_dir="./pseudo"),
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="sipseudo"),
        dry_run=False,
    )

    assert result.returncode == 0
    assert "Si.UPF" in result.command
    assert (tmp_path / "Si.UPF").exists()


def test_submit_manager_defaults_from_executable_prefix(tmp_path) -> None:
    runner = QERunner(default_backend="submit")
    deck = silicon_scf(pseudo_file="Si.UPF", pseudo_dir="./pseudo")

    result = runner.run(
        deck,
        workdir=tmp_path,
        submit_config=SubmitConfig(
            nodes=2,
            walltime="01:00:00",
            run_name="sijob",
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

    assert "--runName sireferenceremote" in result.stdout


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
    assert (tmp_path / "submit_attempts.txt").read_text(encoding="utf-8").strip() == "1"


def test_submit_status_classifies_state_q_as_running() -> None:
    assert QERunner._classify_submit_status("state=Q") == "running"


def test_submit_out_of_service_is_reported_as_error(tmp_path: Path) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit_out_of_service(submit_script)
    pseudo_dir = tmp_path / "pseudo"
    pseudo_dir.mkdir(parents=True, exist_ok=True)
    (pseudo_dir / "Si.UPF").write_text("pseudo\n", encoding="utf-8")

    runner = QERunner(default_backend="submit", submit_executable=str(submit_script))
    result = runner.run(
        silicon_scf(pseudo_file="Si.UPF", pseudo_dir="./pseudo"),
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="sioos"),
        dry_run=False,
    )

    assert result.returncode == 8
    assert result.remote_status is None
    assert "out of service" in result.stderr.lower()


def test_submit_out_of_service_retries_without_venue(tmp_path: Path) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit_out_of_service_when_venue(submit_script)
    pseudo_dir = tmp_path / "pseudo"
    pseudo_dir.mkdir(parents=True, exist_ok=True)
    (pseudo_dir / "Si.UPF").write_text("pseudo\n", encoding="utf-8")

    runner = QERunner(default_backend="submit", submit_executable=str(submit_script))
    result = runner.run(
        silicon_scf(pseudo_file="Si.UPF", pseudo_dir="./pseudo"),
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="sioosretry", venue="nanohub"),
        dry_run=False,
    )

    assert result.returncode == 0
    assert result.submitted
    assert result.remote_job_id == "no-venue-job"


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
        submit_config=SubmitConfig(
            run_name="si-remote",
            download_command_template=f"{submit_script} --download --runName {{run_name}}",
        ),
        wait=True,
        sync_outputs=True,
        poll_interval=0.01,
        wait_timeout=5.0,
    )

    assert set(results) == {"scf", "bands"}
    assert results["scf"].submitted
    assert results["scf"].remote_status == "completed"
    assert results["scf"].outputs_synced
    assert results["scf"].remote_run_name == "siremotescf"


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
    assert "no download command candidates configured" in captured.out


def test_run_workflow_submit_non_opticdft_manager_skips_opticdft_locator_warning(
    tmp_path: Path, capsys
) -> None:
    workflow = silicon_bands_workflow()
    _touch_workflow_pseudos(workflow, tmp_path)
    runner = QERunner(default_backend="submit", verbose=True)

    runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(
            run_name="si-remote",
            manager="espresso-7.1_mpi-cleanup_pw",
        ),
        dry_run=True,
        wait=False,
        sync_outputs=False,
    )

    captured = capsys.readouterr()
    assert "locator file not found: OPTICDFT.wavefilelocation" not in captured.out


def test_run_workflow_submit_shows_wait_feedback_by_default(tmp_path: Path, capsys) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit(submit_script)

    workflow = silicon_bands_workflow()
    _touch_workflow_pseudos(workflow, tmp_path)
    runner = QERunner(default_backend="submit", submit_executable=str(submit_script))

    runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="si-remote"),
        wait=True,
        sync_outputs=False,
        poll_interval=0.01,
        wait_timeout=5.0,
    )

    captured = capsys.readouterr()
    assert "waiting for step 'scf'" in captured.out
    assert "status=completed" in captured.out


def test_run_workflow_submit_stops_on_unknown_status_when_strict(tmp_path: Path) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit_status_unknown(submit_script)

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
        wait_timeout=1.0,
    )

    assert set(results) == {"scf"}
    assert results["scf"].returncode == 1
    assert "Remote status for step 'scf' is unknown" in results["scf"].stderr
    assert (tmp_path / "submit_attempts_unknown.txt").read_text(encoding="utf-8").strip() == "1"


def test_run_workflow_submit_unknown_status_can_infer_success_from_output(
    tmp_path: Path,
) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit_status_unknown_but_done(submit_script)

    workflow = silicon_bands_workflow()
    _touch_workflow_pseudos(workflow, tmp_path)
    runner = QERunner(default_backend="submit", submit_executable=str(submit_script))

    results = runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="si-remote"),
        wait=True,
        sync_outputs=False,
        poll_interval=0.01,
        wait_timeout=1.0,
    )

    assert set(results) == {"scf", "bands"}
    assert results["scf"].ok
    assert results["bands"].ok
    assert results["scf"].remote_status == "completed"
    assert results["bands"].remote_status == "completed"
    assert (
        tmp_path / "submit_unknown_but_done_calls.txt"
    ).read_text(encoding="utf-8").strip() == "2"


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


def test_run_workflow_submit_stages_existing_save_dir_between_steps(tmp_path: Path) -> None:
    from nanohubqe import silicon_bands_dos_reference_workflow

    workflow = silicon_bands_dos_reference_workflow(include_plotband=False)
    _touch_workflow_pseudos(workflow, tmp_path)

    save_dir = tmp_path / "tmp" / "qe.save"
    save_dir.mkdir(parents=True, exist_ok=True)
    (save_dir / "data-file-schema.xml").write_text("<xml/>", encoding="utf-8")

    runner = QERunner(default_backend="submit")
    results = runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(run_name="si-remote"),
        dry_run=True,
        wait=False,
        sync_outputs=False,
    )

    assert "-i tmp/qe.save" in results["dos"].stdout
    assert "-i tmp/qe.save" in results["bands_pw"].stdout
    assert "-i tmp/qe.save" in results["bands_pp"].stdout


def test_run_workflow_submit_does_not_stage_save_dir_when_manager_is_set(tmp_path: Path) -> None:
    from nanohubqe import silicon_bands_dos_reference_workflow

    workflow = silicon_bands_dos_reference_workflow(include_plotband=False)
    _touch_workflow_pseudos(workflow, tmp_path)

    save_dir = tmp_path / "tmp" / "qe.save"
    save_dir.mkdir(parents=True, exist_ok=True)
    (save_dir / "data-file-schema.xml").write_text("<xml/>", encoding="utf-8")

    runner = QERunner(default_backend="submit")
    results = runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(
            run_name="si-remote",
            manager="espresso-7.1_mpi-cleanup_pw",
        ),
        dry_run=True,
        wait=False,
        sync_outputs=False,
    )

    assert "-i tmp/qe.save" not in results["dos"].stdout
    assert "-i tmp/qe.save" not in results["bands_pw"].stdout
    assert "-i tmp/qe.save" not in results["bands_pp"].stdout


def test_run_workflow_submit_auto_applies_manager_file_actions(tmp_path: Path) -> None:
    workflow = silicon_bands_workflow()
    _touch_workflow_pseudos(workflow, tmp_path)

    runner = QERunner(default_backend="submit")
    results = runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(
            manager="espresso-7.1_mpi",
            run_name="si-remote",
        ),
        dry_run=True,
        wait=False,
        sync_outputs=False,
    )

    assert "--env OPTICDFTFileAction=CREATESTORE:SAVE" in results["scf"].stdout
    assert "--env OPTICDFTFileAction=FETCH:DESTROY" in results["bands"].stdout


def test_run_workflow_submit_keeps_explicit_step_file_action_env(tmp_path: Path) -> None:
    from nanohubqe import gaas_opticdft_epsilon_workflow

    workflow = gaas_opticdft_epsilon_workflow()
    _touch_workflow_pseudos(workflow, tmp_path)

    runner = QERunner(default_backend="submit")
    results = runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(
            manager="opticdft-espresso-7.1_mpi",
            run_name="gaas-optic",
            apply_manager_file_actions=True,
        ),
        dry_run=True,
        wait=False,
        sync_outputs=False,
    )

    assert "--env OPTICDFTFileAction=CREATESTORE:SAVE" in results["scf"].stdout
    assert "--env OPTICDFTFileAction=FETCH:DESTROY" in results["optical"].stdout


def test_run_workflow_submit_fetch_stage_adds_locator_when_present(tmp_path: Path) -> None:
    workflow = silicon_bands_workflow()
    _touch_workflow_pseudos(workflow, tmp_path)
    (tmp_path / "OPTICDFT.wavefilelocation").write_text("loc\n", encoding="utf-8")

    runner = QERunner(default_backend="submit")
    results = runner.run_workflow_submit(
        workflow,
        workdir=tmp_path,
        submit_config=SubmitConfig(
            manager="opticdft-espresso-7.1_mpi",
            run_name="si-remote",
            apply_manager_file_actions=True,
        ),
        dry_run=True,
        wait=False,
        sync_outputs=False,
    )

    assert "--env OPTICDFTFileAction=FETCH:DESTROY" in results["bands"].stdout
    assert "-i OPTICDFT.wavefilelocation" in results["bands"].stdout
