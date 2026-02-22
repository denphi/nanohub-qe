"""Input deck builders for Quantum ESPRESSO pw.x calculations."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence, Tuple, Union

Vector3 = Tuple[float, float, float]
KAutomatic = Tuple[int, int, int, int, int, int]
KList = Sequence[Sequence[Union[float, int]]]


class DeckValidationError(ValueError):
    """Raised when a PWInputDeck contains invalid or inconsistent data."""


def _format_numeric(value: float | int) -> str:
    if isinstance(value, bool):
        raise TypeError("booleans are not valid numeric values")
    if isinstance(value, int):
        return str(value)
    if abs(value) >= 1.0e-4 and abs(value) < 1.0e6:
        return f"{value:.10f}".rstrip("0").rstrip(".")
    return f"{value:.10e}"


def _format_qe_value(value: Any) -> str:
    if isinstance(value, bool):
        return ".true." if value else ".false."
    if isinstance(value, (int, float)):
        return _format_numeric(value)
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("'") and text.endswith("'"):
            return text
        return f"'{text}'"
    raise TypeError(f"Unsupported value for QE namelist: {value!r}")


def _render_namelist(name: str, values: Mapping[str, Any]) -> str:
    lines = [f"&{name.upper()}"]
    for key, value in values.items():
        if value is None:
            continue
        lines.append(f"  {key} = {_format_qe_value(value)},")
    lines.append("/")
    return "\n".join(lines)


@dataclass(frozen=True)
class Species:
    """Atomic species definition in ATOMIC_SPECIES card."""

    symbol: str
    mass_amu: float
    pseudo_file: str


@dataclass(frozen=True)
class Atom:
    """Atom entry in ATOMIC_POSITIONS card."""

    symbol: str
    position: Vector3


@dataclass
class PWInputDeck:
    """Represents a complete pw.x input deck."""

    control: dict[str, Any]
    system: dict[str, Any]
    electrons: dict[str, Any] = field(default_factory=dict)
    ions: dict[str, Any] = field(default_factory=dict)
    cell: dict[str, Any] = field(default_factory=dict)
    atomic_species: list[Species] = field(default_factory=list)
    atomic_positions: list[Atom] = field(default_factory=list)
    atomic_positions_mode: str = "crystal"
    k_points_mode: str = "automatic"
    k_points: KAutomatic | KList | None = (1, 1, 1, 0, 0, 0)
    cell_parameters: list[Vector3] | None = None
    cell_parameters_mode: str = "angstrom"
    extra_cards: list[str] = field(default_factory=list)

    def _system_with_counts(self) -> dict[str, Any]:
        system = dict(self.system)
        nat = len(self.atomic_positions)
        ntyp = len({spec.symbol for spec in self.atomic_species})

        if "nat" in system and int(system["nat"]) != nat:
            raise DeckValidationError(
                f"SYSTEM.nat={system['nat']} but {nat} atomic positions were provided"
            )
        if "ntyp" in system and int(system["ntyp"]) != ntyp:
            raise DeckValidationError(
                f"SYSTEM.ntyp={system['ntyp']} but {ntyp} unique species were provided"
            )

        system.setdefault("nat", nat)
        system.setdefault("ntyp", ntyp)
        return system

    def validate(self) -> None:
        if not self.atomic_species:
            raise DeckValidationError("ATOMIC_SPECIES cannot be empty")
        if not self.atomic_positions:
            raise DeckValidationError("ATOMIC_POSITIONS cannot be empty")

        declared_species = {spec.symbol for spec in self.atomic_species}
        used_species = {atom.symbol for atom in self.atomic_positions}
        missing_species = sorted(used_species - declared_species)
        if missing_species:
            raise DeckValidationError(
                f"Missing species declarations for symbols: {', '.join(missing_species)}"
            )

        ibrav = int(self.system.get("ibrav", 0))
        if ibrav == 0 and not self.cell_parameters:
            raise DeckValidationError("CELL_PARAMETERS are required when SYSTEM.ibrav = 0")

        mode = self.k_points_mode.lower()
        if mode == "automatic":
            if not isinstance(self.k_points, tuple) or len(self.k_points) != 6:
                raise DeckValidationError(
                    "K_POINTS automatic expects a 6-int tuple: (nk1 nk2 nk3 sk1 sk2 sk3)"
                )
        elif mode == "gamma":
            return
        else:
            if self.k_points is None:
                raise DeckValidationError(
                    f"K_POINTS {self.k_points_mode} expects a non-empty list of points"
                )
            if not isinstance(self.k_points, Sequence):
                raise DeckValidationError(
                    f"K_POINTS {self.k_points_mode} must be a sequence of points"
                )
            if len(self.k_points) == 0:
                raise DeckValidationError(
                    f"K_POINTS {self.k_points_mode} cannot be an empty list"
                )

    def _render_atomic_species(self) -> str:
        lines = ["ATOMIC_SPECIES"]
        for species in self.atomic_species:
            lines.append(
                f" {species.symbol} {_format_numeric(species.mass_amu)} {species.pseudo_file}"
            )
        return "\n".join(lines)

    def _render_atomic_positions(self) -> str:
        lines = [f"ATOMIC_POSITIONS {self.atomic_positions_mode}"]
        for atom in self.atomic_positions:
            x, y, z = atom.position
            lines.append(
                f" {atom.symbol} {_format_numeric(x)} {_format_numeric(y)} {_format_numeric(z)}"
            )
        return "\n".join(lines)

    def _render_k_points(self) -> str:
        mode = self.k_points_mode.lower()
        if mode == "automatic":
            assert isinstance(self.k_points, tuple)
            values = " ".join(str(int(v)) for v in self.k_points)
            return f"K_POINTS automatic\n {values}"
        if mode == "gamma":
            return "K_POINTS gamma"

        assert self.k_points is not None
        lines = [f"K_POINTS {self.k_points_mode}", f"{len(self.k_points)}"]
        for row in self.k_points:
            if len(row) != 4:
                raise DeckValidationError(
                    "Explicit k-point rows must have 4 values: kx ky kz weight/segments"
                )
            values = " ".join(_format_numeric(float(value)) for value in row)
            lines.append(f" {values}")
        return "\n".join(lines)

    def _render_cell_parameters(self) -> str:
        if not self.cell_parameters:
            return ""

        lines = [f"CELL_PARAMETERS {self.cell_parameters_mode}"]
        for vector in self.cell_parameters:
            if len(vector) != 3:
                raise DeckValidationError("CELL_PARAMETERS vectors must have exactly 3 values")
            values = " ".join(_format_numeric(float(value)) for value in vector)
            lines.append(f" {values}")
        return "\n".join(lines)

    def to_string(self) -> str:
        """Render the full pw.x input deck text."""

        self.validate()
        system = self._system_with_counts()

        sections = [
            _render_namelist("CONTROL", self.control),
            _render_namelist("SYSTEM", system),
            _render_namelist("ELECTRONS", self.electrons),
        ]

        if self.ions:
            sections.append(_render_namelist("IONS", self.ions))
        if self.cell:
            sections.append(_render_namelist("CELL", self.cell))

        cards = [
            self._render_atomic_species(),
            self._render_atomic_positions(),
            self._render_k_points(),
        ]

        cell_card = self._render_cell_parameters()
        if cell_card:
            cards.append(cell_card)

        for card in self.extra_cards:
            card_text = card.strip()
            if card_text:
                cards.append(card_text)

        return "\n\n".join(sections + cards).strip() + "\n"

    def write(self, path: str | Path) -> Path:
        """Write the rendered input deck to *path* and return the resolved path."""

        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(self.to_string(), encoding="utf-8")
        return output_path
