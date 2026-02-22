from __future__ import annotations

import json

from nanohubqe import QERunner, QEStep, SubmitConfig, silicon_bands_workflow, silicon_scf


def test_build_submit_command_includes_common_flags() -> None:
    runner = QERunner()
    submit_cfg = SubmitConfig(
        venue="nanohub",
        n_cpus=8,
        wall_time="01:30:00",
        run_name="si-test",
        input_files=["qe.in"],
        env={"ESPRESSO_TMPDIR": "./tmp"},
    )

    command = runner.build_submit_command(["pw.x", "-in", "qe.in"], submit_cfg)

    assert command[0] == "submit"
    assert "--venue" in command
    assert "--nCpus" in command
    assert "--wallTime" in command
    assert "--inputfile" in command
    assert "--env" in command
    assert "pw.x -in qe.in" in command[-1]


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

    assert "submit --venue nanohub --inputfile dos.in 'dos.x -in dos.in'" == result.stdout


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
    assert (tmp_path / "run.xml").exists()
