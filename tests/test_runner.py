from __future__ import annotations

import json

from nanohubqe import QERunner, QEStep, SubmitConfig, silicon_bands_workflow, silicon_scf


def test_build_submit_command_includes_common_flags() -> None:
    runner = QERunner()
    submit_cfg = SubmitConfig(
        venue="nanohub",
        n_cpus=8,
        wall_time="01:30:00",
        manager="espresso-6.8_mpi-cleanup_pw",
        run_name="si-test",
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

    assert "submit --venue nanohub dos.x -in dos.in" == result.stdout


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
            run_name="si-job",
            executable_prefix="espresso-7.1",
        ),
        dry_run=True,
    )

    assert (
        "submit -n 2 -w 01:00:00 --manager espresso-6.8_mpi-cleanup_pw "
        "--runName si-job -i pseudo/Si.UPF espresso-7.1_pw -i qe.in"
    ) == result.stdout


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
