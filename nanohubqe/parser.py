"""Parsers for Quantum ESPRESSO text outputs."""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


_TOTAL_ENERGY_RE = re.compile(
    r"^\s*!?\s*total energy\s+=\s+([-+]?\d+(?:\.\d+)?)\s+Ry",
    re.IGNORECASE | re.MULTILINE,
)
_FERMI_RE = re.compile(r"the Fermi energy is\s+([-+]?\d+(?:\.\d+)?)\s+ev", re.IGNORECASE)
_PRESSURE_RE = re.compile(r"P=\s*([-+]?\d+(?:\.\d+)?)\s*\(kbar\)")
_WARNING_RE = re.compile(r"\bwarning\b", re.IGNORECASE)
_JOB_DONE_RE = re.compile(r"JOB DONE\.", re.IGNORECASE)
_BANDS_FILBAND_HEADER_RE = re.compile(
    r"&plot\s+nbnd\s*=\s*(\d+)\s*,\s*nks\s*=\s*(\d+)\s*/",
    re.IGNORECASE,
)

RY_TO_EV = 13.605693122994


@dataclass
class QERunSummary:
    """High-level values extracted from a pw.x output."""

    total_energies_ry: list[float] = field(default_factory=list)
    fermi_energy_ev: float | None = None
    pressure_kbar: float | None = None
    warnings: list[str] = field(default_factory=list)
    completed: bool = False

    @property
    def final_total_energy_ry(self) -> float | None:
        if not self.total_energies_ry:
            return None
        return self.total_energies_ry[-1]

    @property
    def final_total_energy_ev(self) -> float | None:
        energy_ry = self.final_total_energy_ry
        if energy_ry is None:
            return None
        return energy_ry * RY_TO_EV


@dataclass
class DOSData:
    """Density-of-states data from `dos.x`-style tabular outputs."""

    energies_ev: list[float]
    density: list[float]
    integrated_density: list[float] | None = None


@dataclass
class PDOSData:
    """Projected/partial DOS table with one or more channels."""

    energies_ev: list[float]
    channels: dict[str, list[float]]
    source: str | None = None


@dataclass
class PhononDispersion:
    """Phonon branches sampled along a q-point path."""

    q_path: list[float]
    branches_cm1: list[list[float]]
    q_points: list[tuple[float, float, float]] | None = None


def parse_pw_output(text: str) -> QERunSummary:
    """Parse a pw.x output string into `QERunSummary`."""

    energies = [float(match.group(1)) for match in _TOTAL_ENERGY_RE.finditer(text)]

    fermi_matches = list(_FERMI_RE.finditer(text))
    fermi = float(fermi_matches[-1].group(1)) if fermi_matches else None

    pressure_matches = list(_PRESSURE_RE.finditer(text))
    pressure = float(pressure_matches[-1].group(1)) if pressure_matches else None

    warnings = [line.strip() for line in text.splitlines() if _WARNING_RE.search(line)]
    completed = bool(_JOB_DONE_RE.search(text))

    return QERunSummary(
        total_energies_ry=energies,
        fermi_energy_ev=fermi,
        pressure_kbar=pressure,
        warnings=warnings,
        completed=completed,
    )


def read_pw_output(path: str | Path) -> QERunSummary:
    """Read and parse a pw.x output file from disk."""

    output_text = Path(path).read_text(encoding="utf-8")
    return parse_pw_output(output_text)


def read_columns(path: str | Path, ncols: int = 2) -> list[tuple[float, ...]]:
    """Read whitespace-delimited numeric columns, skipping comments and blanks."""

    rows: list[tuple[float, ...]] = []
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("!"):
            continue
        tokens = line.split()
        if len(tokens) < ncols:
            continue
        row = tuple(float(token) for token in tokens[:ncols])
        rows.append(row)
    return rows


def _numeric_rows_from_text(text: str) -> list[list[float]]:
    rows: list[list[float]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("!"):
            continue
        tokens = line.split()
        values: list[float] = []
        for token in tokens:
            try:
                values.append(float(token))
            except ValueError:
                values = []
                break
        if values:
            rows.append(values)
    return rows


def _rectangular_rows(rows: list[list[float]], min_cols: int = 2) -> list[list[float]]:
    width_counts = Counter(len(row) for row in rows if len(row) >= min_cols)
    if not width_counts:
        raise ValueError(f"No numeric rows with at least {min_cols} columns were found")

    width = max(width_counts.items(), key=lambda item: (item[1], item[0]))[0]
    return [row[:width] for row in rows if len(row) >= width]


def parse_dos_text(
    text: str,
    *,
    energy_column: int = 0,
    dos_column: int = 1,
    integrated_dos_column: int | None = 2,
) -> DOSData:
    """Parse DOS table text into `DOSData`."""

    rows = _rectangular_rows(_numeric_rows_from_text(text), min_cols=2)
    ncols = len(rows[0])

    if not (0 <= energy_column < ncols):
        raise ValueError(f"energy_column={energy_column} out of range for {ncols} columns")
    if not (0 <= dos_column < ncols):
        raise ValueError(f"dos_column={dos_column} out of range for {ncols} columns")
    if energy_column == dos_column:
        raise ValueError("energy_column and dos_column cannot be the same")

    energies = [row[energy_column] for row in rows]
    dos = [row[dos_column] for row in rows]

    integrated = None
    if integrated_dos_column is not None:
        if not (0 <= integrated_dos_column < ncols):
            raise ValueError(
                f"integrated_dos_column={integrated_dos_column} out of range for {ncols} columns"
            )
        integrated = [row[integrated_dos_column] for row in rows]

    return DOSData(energies_ev=energies, density=dos, integrated_density=integrated)


def read_dos(
    path: str | Path,
    *,
    energy_column: int = 0,
    dos_column: int = 1,
    integrated_dos_column: int | None = 2,
) -> DOSData:
    """Read a DOS table file into `DOSData`."""

    text = Path(path).read_text(encoding="utf-8")
    return parse_dos_text(
        text,
        energy_column=energy_column,
        dos_column=dos_column,
        integrated_dos_column=integrated_dos_column,
    )


def _default_pdos_labels(num_channels: int) -> list[str]:
    if num_channels == 1:
        return ["pdos"]
    if num_channels == 2:
        return ["ldos", "pdos"]
    if num_channels == 4:
        return ["ldos_up", "ldos_down", "pdos_up", "pdos_down"]
    return [f"channel_{index + 1}" for index in range(num_channels)]


def parse_pdos_text(
    text: str,
    *,
    energy_column: int = 0,
    channel_labels: list[str] | None = None,
) -> PDOSData:
    """Parse a projected DOS table into `PDOSData`."""

    rows = _rectangular_rows(_numeric_rows_from_text(text), min_cols=2)
    ncols = len(rows[0])
    if not (0 <= energy_column < ncols):
        raise ValueError(f"energy_column={energy_column} out of range for {ncols} columns")

    value_indices = [index for index in range(ncols) if index != energy_column]
    if not value_indices:
        raise ValueError("PDOS table must include at least one channel column")

    labels = channel_labels or _default_pdos_labels(len(value_indices))
    if len(labels) != len(value_indices):
        raise ValueError(
            f"Expected {len(value_indices)} channel labels, got {len(labels)}"
        )

    energies = [row[energy_column] for row in rows]
    channels: dict[str, list[float]] = {}
    for label, column_index in zip(labels, value_indices):
        channels[label] = [row[column_index] for row in rows]

    return PDOSData(energies_ev=energies, channels=channels)


def read_pdos(
    path: str | Path,
    *,
    energy_column: int = 0,
    channel_labels: list[str] | None = None,
) -> PDOSData:
    """Read a projected DOS file into `PDOSData`."""

    source = Path(path)
    data = parse_pdos_text(
        source.read_text(encoding="utf-8"),
        energy_column=energy_column,
        channel_labels=channel_labels,
    )
    data.source = str(source)
    return data


def read_pdos_directory(
    directory: str | Path,
    *,
    pattern: str = "*.pdos*",
    energy_column: int = 0,
) -> dict[str, PDOSData]:
    """Read a set of PDOS files and map them by filename stem."""

    base = Path(directory)
    outputs: dict[str, PDOSData] = {}
    for path in sorted(base.glob(pattern)):
        if path.is_file():
            outputs[path.stem] = read_pdos(path, energy_column=energy_column)
    return outputs


def _looks_like_q_vectors(rows: list[list[float]]) -> bool:
    if len(rows[0]) < 4:
        return False

    q_values = [abs(value) for row in rows for value in row[:3]]
    freq_values = [abs(value) for row in rows for value in row[3:]]
    if not freq_values:
        return False

    return max(q_values) <= 2.0 and max(freq_values) > 5.0


def parse_matdyn_freq_text(
    text: str,
    *,
    q_mode: Literal["auto", "distance", "vector"] = "auto",
) -> PhononDispersion:
    """Parse `matdyn.x` frequency-style output tables."""

    rows = _rectangular_rows(_numeric_rows_from_text(text), min_cols=2)
    ncols = len(rows[0])
    if ncols < 2:
        raise ValueError("Need at least two columns to parse phonon dispersion data")

    if q_mode == "auto":
        vector_mode = _looks_like_q_vectors(rows)
    elif q_mode == "vector":
        vector_mode = True
    elif q_mode == "distance":
        vector_mode = False
    else:
        raise ValueError("q_mode must be one of: auto, distance, vector")

    if vector_mode:
        if ncols < 4:
            raise ValueError("Vector q-mode requires at least 4 columns: qx qy qz freq...")
        q_points = [
            (row[0], row[1], row[2])
            for row in rows
        ]
        q_path = [0.0]
        for prev, curr in zip(q_points, q_points[1:]):
            q_path.append(q_path[-1] + math.dist(prev, curr))
        value_rows = [row[3:] for row in rows]
    else:
        q_points = None
        q_path = [row[0] for row in rows]
        value_rows = [row[1:] for row in rows]

    if len(value_rows[0]) == 0:
        raise ValueError("No phonon branches found in the parsed table")

    branches = [
        [row[index] for row in value_rows]
        for index in range(len(value_rows[0]))
    ]
    return PhononDispersion(q_path=q_path, branches_cm1=branches, q_points=q_points)


def read_matdyn_freq(
    path: str | Path,
    *,
    q_mode: Literal["auto", "distance", "vector"] = "auto",
) -> PhononDispersion:
    """Read and parse a `matdyn.x` frequency table."""

    text = Path(path).read_text(encoding="utf-8")
    return parse_matdyn_freq_text(text, q_mode=q_mode)


def read_bands_gnu(path: str | Path) -> list[tuple[list[float], list[float]]]:
    """Read QE bands output.

    Supports both:
    - gnuplot-style two-column segments separated by blank lines, and
    - QE `filband` format (header `&plot nbnd=..., nks=... /` followed by
      alternating k-point/eigenvalue rows).
    """

    text = Path(path).read_text(encoding="utf-8")

    header_match = _BANDS_FILBAND_HEADER_RE.search(text)
    if header_match is not None:
        nbnd = int(header_match.group(1))
        nks = int(header_match.group(2))
        lines = text[header_match.end() :].splitlines()

        def parse_numeric_tokens(raw: str) -> list[float] | None:
            tokens = raw.split()
            if not tokens:
                return None
            values: list[float] = []
            for token in tokens:
                try:
                    values.append(float(token))
                except ValueError:
                    return None
            return values

        k_points: list[tuple[float, float, float]] = []
        eigs_per_k: list[list[float]] = []

        index = 0
        while index < len(lines) and len(k_points) < nks:
            raw_line = lines[index].strip()
            index += 1
            if not raw_line:
                continue

            k_values = parse_numeric_tokens(raw_line)
            if k_values is None or len(k_values) < 3:
                continue
            k_points.append((k_values[0], k_values[1], k_values[2]))

            eig_values: list[float] = []
            while index < len(lines) and len(eig_values) < nbnd:
                eig_line = lines[index].strip()
                index += 1
                if not eig_line:
                    continue
                values = parse_numeric_tokens(eig_line)
                if values is None:
                    continue
                eig_values.extend(values)

            if len(eig_values) < nbnd:
                break
            eigs_per_k.append(eig_values[:nbnd])

        count = min(len(k_points), len(eigs_per_k))
        if count == 0:
            raise ValueError(
                f"Could not parse bands filband format from: {path}. "
                "Expected alternating k-point/eigenvalue blocks."
            )

        k_points = k_points[:count]
        eigs_per_k = eigs_per_k[:count]

        x_path = [0.0]
        for prev, curr in zip(k_points, k_points[1:]):
            x_path.append(x_path[-1] + math.dist(prev, curr))

        segments: list[tuple[list[float], list[float]]] = []
        for band_index in range(nbnd):
            y_values = [row[band_index] for row in eigs_per_k]
            segments.append((list(x_path), y_values))
        return segments

    segments: list[tuple[list[float], list[float]]] = []
    x_values: list[float] = []
    y_values: list[float] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            if x_values:
                segments.append((x_values, y_values))
                x_values, y_values = [], []
            continue

        tokens = line.split()
        if len(tokens) < 2:
            continue

        try:
            x_value = float(tokens[0])
            y_value = float(tokens[1])
        except ValueError:
            # Some QE outputs (e.g. filband) include non-numeric header lines
            # such as "&plot ... /" before numeric band data.
            continue

        x_values.append(x_value)
        y_values.append(y_value)

    if x_values:
        segments.append((x_values, y_values))

    return segments
