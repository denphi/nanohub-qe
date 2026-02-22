"""Plotting helpers for parsed Quantum ESPRESSO outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from .parser import (
    DOSData,
    PDOSData,
    PhononDispersion,
    QERunSummary,
    read_bands_gnu,
    read_dos,
    read_matdyn_freq,
    read_pdos,
    read_pw_output,
)


def _import_matplotlib():
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise ImportError(
            "matplotlib is required for visualization helpers. "
            "Install with `pip install matplotlib`."
        ) from exc
    return plt


def _import_plotly():
    try:
        import plotly.graph_objects as go
    except ImportError as exc:
        raise ImportError(
            "plotly is required for plotly backend support. "
            "Install with `pip install plotly`."
        ) from exc
    return go


def _validate_backend(backend: str) -> Literal["matplotlib", "plotly"]:
    name = backend.lower()
    if name not in {"matplotlib", "plotly"}:
        raise ValueError("backend must be either 'matplotlib' or 'plotly'")
    return name  # type: ignore[return-value]


def plot_total_energy(
    summary_or_path: QERunSummary | str | Path,
    *,
    unit: str = "Ry",
    backend: str = "matplotlib",
    ax=None,
):
    """Plot the total energy history from an SCF/relax run."""

    if isinstance(summary_or_path, QERunSummary):
        summary = summary_or_path
    else:
        summary = read_pw_output(summary_or_path)

    energies = summary.total_energies_ry
    if unit.lower() == "ev":
        energies = [energy * 13.605693122994 for energy in energies]
        ylabel = "Total Energy (eV)"
    else:
        ylabel = "Total Energy (Ry)"

    x_values = list(range(1, len(energies) + 1))
    backend_name = _validate_backend(backend)

    if backend_name == "matplotlib":
        plt = _import_matplotlib()
        if ax is None:
            _, ax = plt.subplots(figsize=(7, 4))
        ax.plot(x_values, energies, marker="o", linewidth=1.5)
        ax.set_xlabel("Iteration")
        ax.set_ylabel(ylabel)
        ax.set_title("QE Energy Convergence")
        ax.grid(True, alpha=0.25)
        return ax

    go = _import_plotly()
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(x=x_values, y=energies, mode="lines+markers", name="total energy")
    )
    figure.update_layout(
        title="QE Energy Convergence",
        xaxis_title="Iteration",
        yaxis_title=ylabel,
        template="plotly_white",
    )
    return figure


def plot_run_curve(*args, **kwargs):
    """Compatibility shim for removed XML curve plotting API.

    XML-based run records were removed from nanohubqe. Use QE-native parsers
    and plotting helpers such as `plot_total_energy`, `plot_dos`,
    `plot_pdos`, `plot_bands`, and `plot_phonon_dispersion`.
    """

    raise RuntimeError(
        "plot_run_curve is no longer supported because XML run records were removed. "
        "Use JSON workflow records plus QE-native plotters "
        "(plot_total_energy, plot_dos, plot_pdos, plot_bands, plot_phonon_dispersion)."
    )


def plot_bands(
    bands_file: str | Path,
    *,
    fermi_energy_ev: float | None = None,
    backend: str = "matplotlib",
    ax=None,
):
    """Plot a `bands.x` gnuplot file with optional Fermi-level alignment."""

    segments = read_bands_gnu(bands_file)
    backend_name = _validate_backend(backend)

    if backend_name == "matplotlib":
        plt = _import_matplotlib()
        if ax is None:
            _, ax = plt.subplots(figsize=(7.5, 4.5))

        for x_values, y_values in segments:
            if fermi_energy_ev is not None:
                y_values = [energy - fermi_energy_ev for energy in y_values]
            ax.plot(x_values, y_values, color="tab:blue", linewidth=1.0)

        if fermi_energy_ev is not None:
            ax.axhline(0.0, color="black", linewidth=0.8, linestyle="--")
            ax.set_ylabel("Energy - Ef (eV)")
        else:
            ax.set_ylabel("Energy (eV)")

        ax.set_xlabel("k-path")
        ax.set_title("Band Structure")
        ax.grid(True, alpha=0.2)
        return ax

    go = _import_plotly()
    figure = go.Figure()
    for index, (x_values, y_values) in enumerate(segments):
        if fermi_energy_ev is not None:
            y_values = [energy - fermi_energy_ev for energy in y_values]
        figure.add_trace(
            go.Scatter(
                x=x_values,
                y=y_values,
                mode="lines",
                line={"color": "#1f77b4", "width": 1},
                name=f"band_{index + 1}",
                showlegend=False,
            )
        )
    if fermi_energy_ev is not None:
        figure.add_hline(y=0.0, line_dash="dash", line_color="black")
        ylabel = "Energy - Ef (eV)"
    else:
        ylabel = "Energy (eV)"

    figure.update_layout(
        title="Band Structure",
        xaxis_title="k-path",
        yaxis_title=ylabel,
        template="plotly_white",
    )
    return figure


def plot_dos(
    dos_or_path: DOSData | str | Path,
    *,
    fermi_energy_ev: float | None = None,
    backend: str = "matplotlib",
    ax=None,
):
    """Plot DOS from a file path or `DOSData`."""

    if isinstance(dos_or_path, DOSData):
        dos_data = dos_or_path
    else:
        dos_data = read_dos(dos_or_path)
    energies = list(dos_data.energies_ev)
    density = list(dos_data.density)

    if fermi_energy_ev is not None:
        energies = [energy - fermi_energy_ev for energy in energies]

    backend_name = _validate_backend(backend)

    if backend_name == "matplotlib":
        plt = _import_matplotlib()
        if ax is None:
            _, ax = plt.subplots(figsize=(6.5, 4.0))

        ax.plot(energies, density, color="tab:red", linewidth=1.5)
        ax.fill_between(energies, density, alpha=0.2, color="tab:red")

        if fermi_energy_ev is not None:
            ax.axvline(0.0, color="black", linewidth=0.8, linestyle="--")
            ax.set_xlabel("Energy - Ef (eV)")
        else:
            ax.set_xlabel("Energy (eV)")

        ax.set_ylabel("DOS (states/eV)")
        ax.set_title("Density of States")
        ax.grid(True, alpha=0.25)
        return ax

    go = _import_plotly()
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=energies,
            y=density,
            mode="lines",
            line={"color": "#d62728", "width": 2},
            fill="tozeroy",
            name="DOS",
        )
    )
    if fermi_energy_ev is not None:
        figure.add_vline(x=0.0, line_dash="dash", line_color="black")
        xlabel = "Energy - Ef (eV)"
    else:
        xlabel = "Energy (eV)"

    figure.update_layout(
        title="Density of States",
        xaxis_title=xlabel,
        yaxis_title="DOS (states/eV)",
        template="plotly_white",
    )
    return figure


def plot_pdos(
    pdos_or_path: PDOSData | str | Path,
    *,
    channels: list[str] | None = None,
    fermi_energy_ev: float | None = None,
    backend: str = "matplotlib",
    ax=None,
):
    """Plot projected DOS channels from a file path or `PDOSData`."""

    if isinstance(pdos_or_path, PDOSData):
        pdos_data = pdos_or_path
    else:
        pdos_data = read_pdos(pdos_or_path)

    selected = channels or list(pdos_data.channels)
    missing = [channel for channel in selected if channel not in pdos_data.channels]
    if missing:
        raise ValueError(f"Requested PDOS channels not found: {', '.join(missing)}")

    energies = list(pdos_data.energies_ev)
    if fermi_energy_ev is not None:
        energies = [energy - fermi_energy_ev for energy in energies]

    backend_name = _validate_backend(backend)
    if backend_name == "matplotlib":
        plt = _import_matplotlib()
        if ax is None:
            _, ax = plt.subplots(figsize=(7.0, 4.5))

        for channel in selected:
            ax.plot(energies, pdos_data.channels[channel], linewidth=1.4, label=channel)
        if fermi_energy_ev is not None:
            ax.axvline(0.0, color="black", linewidth=0.8, linestyle="--")
            ax.set_xlabel("Energy - Ef (eV)")
        else:
            ax.set_xlabel("Energy (eV)")
        ax.set_ylabel("PDOS (states/eV)")
        ax.set_title("Projected Density of States")
        ax.grid(True, alpha=0.25)
        if len(selected) > 1:
            ax.legend()
        return ax

    go = _import_plotly()
    figure = go.Figure()
    for channel in selected:
        figure.add_trace(
            go.Scatter(
                x=energies,
                y=pdos_data.channels[channel],
                mode="lines",
                name=channel,
            )
        )
    if fermi_energy_ev is not None:
        figure.add_vline(x=0.0, line_dash="dash", line_color="black")
        xlabel = "Energy - Ef (eV)"
    else:
        xlabel = "Energy (eV)"
    figure.update_layout(
        title="Projected Density of States",
        xaxis_title=xlabel,
        yaxis_title="PDOS (states/eV)",
        template="plotly_white",
    )
    return figure


def plot_phonon_dispersion(
    dispersion_or_path: PhononDispersion | str | Path,
    *,
    backend: str = "matplotlib",
    ax=None,
):
    """Plot phonon branches from `matdyn.x` frequency output."""

    if isinstance(dispersion_or_path, PhononDispersion):
        dispersion = dispersion_or_path
    else:
        dispersion = read_matdyn_freq(dispersion_or_path)

    backend_name = _validate_backend(backend)
    if backend_name == "matplotlib":
        plt = _import_matplotlib()
        if ax is None:
            _, ax = plt.subplots(figsize=(7.5, 4.5))
        for branch in dispersion.branches_cm1:
            ax.plot(dispersion.q_path, branch, color="tab:blue", linewidth=1.0)
        ax.set_xlabel("q-path")
        ax.set_ylabel("Frequency (cm$^{-1}$)")
        ax.set_title("Phonon Dispersion")
        ax.grid(True, alpha=0.2)
        return ax

    go = _import_plotly()
    figure = go.Figure()
    for branch in dispersion.branches_cm1:
        figure.add_trace(
            go.Scatter(
                x=dispersion.q_path,
                y=branch,
                mode="lines",
                line={"color": "#1f77b4", "width": 1},
                showlegend=False,
            )
        )
    figure.update_layout(
        title="Phonon Dispersion",
        xaxis_title="q-path",
        yaxis_title="Frequency (cm^-1)",
        template="plotly_white",
    )
    return figure
