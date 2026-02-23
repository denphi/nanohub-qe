from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from nanohubqe import QERunner, SubmitConfig, silicon_bands_dos_reference_workflow


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


def test_workflow_run_caches_results_and_returns_self(tmp_path: Path) -> None:
    sim = silicon_bands_dos_reference_workflow(include_plotband=False)

    returned = sim.run(workdir=tmp_path, dry_run=True)

    assert returned is sim
    assert sim.last_workdir == tmp_path
    assert set(sim.results) == {"scf", "dos", "bands_pw", "bands_pp"}
    assert sim.step_result("scf").ok


def test_step_result_raises_before_run() -> None:
    sim = silicon_bands_dos_reference_workflow(include_plotband=False)
    with pytest.raises(RuntimeError):
        sim.step_result("scf")


def test_plot_dos_raises_when_no_dos_file_is_available(tmp_path: Path) -> None:
    sim = silicon_bands_dos_reference_workflow(include_plotband=False).run(
        workdir=tmp_path,
        dry_run=True,
    )

    with pytest.raises(FileNotFoundError):
        sim.plot_dos()


def test_plot_dos_requires_file_in_recorded_outputs(tmp_path: Path) -> None:
    sim = silicon_bands_dos_reference_workflow(include_plotband=False).run(
        workdir=tmp_path,
        dry_run=True,
    )
    (tmp_path / "qe.dos").write_text("0.0 1.0 1.0\n1.0 2.0 3.0\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="not found in outputs"):
        sim.plot_dos()


def test_plot_dos_finds_generated_file_in_workdir(tmp_path: Path) -> None:
    (tmp_path / "qe.dos").write_text("0.0 1.0 1.0\n1.0 2.0 3.0\n", encoding="utf-8")
    sim = silicon_bands_dos_reference_workflow(include_plotband=False).run(
        workdir=tmp_path,
        dry_run=True,
    )

    axis = sim.plot_dos(backend="matplotlib")

    assert hasattr(axis, "plot")


def test_plot_total_energy_uses_scf_output_by_default(tmp_path: Path) -> None:
    sim = silicon_bands_dos_reference_workflow(include_plotband=False).run(
        workdir=tmp_path,
        dry_run=True,
    )

    axis = sim.plot_total_energy(backend="matplotlib")

    assert hasattr(axis, "plot")


def test_plot_bands_uses_template_kpoint_labels(tmp_path: Path) -> None:
    (tmp_path / "qe.bands.dat").write_text(
        (
            "&plot nbnd= 2, nks= 3 /\n"
            " 0.500000 0.500000 0.500000\n"
            " -3.0 0.0\n"
            " 0.000000 0.000000 0.000000\n"
            " -2.0 1.0\n"
            " 1.000000 0.000000 0.000000\n"
            " -1.0 2.0\n"
        ),
        encoding="utf-8",
    )
    sim = silicon_bands_dos_reference_workflow(include_plotband=False).run(
        workdir=tmp_path,
        dry_run=True,
    )

    figure = sim.plot_bands(backend="plotly")

    assert list(figure.layout.xaxis.ticktext) == ["L", "G", "X"]


def test_prepare_pseudopotentials_delegates_to_helper(monkeypatch) -> None:
    sim = silicon_bands_dos_reference_workflow(include_plotband=False)
    captured: dict[str, object] = {}

    def fake_ensure(workflow, **kwargs):
        captured["workflow"] = workflow
        captured["kwargs"] = kwargs
        return [SimpleNamespace(action="exists")]

    monkeypatch.setattr("nanohubqe.pseudo.ensure_workflow_pseudopotentials", fake_ensure)

    status = sim.prepare_pseudopotentials(workdir="runs/sim")

    assert status[0].action == "exists"
    assert captured["workflow"] is sim
    assert captured["kwargs"]["workdir"] == "runs/sim"


def test_run_submit_supports_plotting_after_wait_and_sync(tmp_path: Path) -> None:
    submit_script = tmp_path / "submit"
    _write_fake_submit(submit_script)

    sim = silicon_bands_dos_reference_workflow(include_plotband=False)
    _touch_workflow_pseudos(sim, tmp_path)
    runner = QERunner(default_backend="submit", submit_executable=str(submit_script))

    sim.run_submit(
        workdir=tmp_path,
        runner=runner,
        submit_config=SubmitConfig(run_name="si-remote"),
        wait=True,
        sync_outputs=True,
        poll_interval=0.01,
        wait_timeout=5.0,
    )

    assert set(sim.results) == {"scf", "dos", "bands_pw", "bands_pp"}
    assert sim.step_result("dos").remote_status == "completed"
    assert sim.step_result("dos").outputs_synced

    dos_axis = sim.plot_dos(backend="matplotlib")
    bands_axis = sim.plot_bands(backend="matplotlib")
    energy_axis = sim.plot_total_energy(backend="matplotlib")

    assert hasattr(dos_axis, "plot")
    assert hasattr(bands_axis, "plot")
    assert hasattr(energy_axis, "plot")
