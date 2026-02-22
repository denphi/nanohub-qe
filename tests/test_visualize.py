from __future__ import annotations

from nanohubqe import (
    DOSData,
    NanoHUBRun,
    PDOSData,
    PhononDispersion,
    RunCurve,
    plot_dos,
    plot_pdos,
    plot_phonon_dispersion,
    plot_run_curve,
)


def test_plot_dos_supports_matplotlib_backend() -> None:
    data = DOSData(energies_ev=[-1.0, 0.0, 1.0], density=[0.1, 0.3, 0.2], integrated_density=None)
    axis = plot_dos(data, backend="matplotlib")
    assert hasattr(axis, "plot")


def test_plot_dos_supports_plotly_backend() -> None:
    data = DOSData(energies_ev=[-1.0, 0.0, 1.0], density=[0.1, 0.3, 0.2], integrated_density=None)
    figure = plot_dos(data, backend="plotly")
    assert hasattr(figure, "to_dict")


def test_plot_pdos_supports_plotly_backend() -> None:
    data = PDOSData(
        energies_ev=[-2.0, -1.0, 0.0],
        channels={"s": [0.1, 0.15, 0.2], "p": [0.2, 0.25, 0.3]},
    )
    figure = plot_pdos(data, backend="plotly")
    assert hasattr(figure, "to_dict")


def test_plot_phonon_dispersion_supports_matplotlib_backend() -> None:
    data = PhononDispersion(
        q_path=[0.0, 0.5, 1.0],
        branches_cm1=[[0.0, 2.0, 4.0], [5.0, 6.0, 8.0]],
        q_points=None,
    )
    axis = plot_phonon_dispersion(data, backend="matplotlib")
    assert hasattr(axis, "plot")


def test_plot_run_curve_supports_plotly_backend() -> None:
    run = NanoHUBRun(
        output_curves={
            "E_scf": RunCurve(
                curve_id="E_scf",
                x=[1.0, 2.0, 3.0],
                y=[-15.7, -15.8, -15.9],
                x_label="SCF",
                y_label="Energy",
                y_units="Ry",
            )
        }
    )
    figure = plot_run_curve(run, "E_scf", backend="plotly")
    assert hasattr(figure, "to_dict")
