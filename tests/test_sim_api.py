from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from nanohubqe import silicon_bands_dos_reference_workflow


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
