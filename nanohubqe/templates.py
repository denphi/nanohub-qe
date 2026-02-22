"""Reusable templates for common Quantum ESPRESSO simulations."""

from __future__ import annotations

import math

from .deck import Atom, PWInputDeck, Species
from .workflow import QEStep, QEWorkflow

_ATOMIC_MASSES = {
    "Al": 26.9815385,
    "C": 12.0107,
    "Si": 28.0855,
}

_BOHR_PER_ANGSTROM = 1.8897261254578281
_BULK_STRUCTURE_DEFS = {
    "sc": {"ibrav": 1, "basis": [(0.0, 0.0, 0.0)]},
    "fcc": {"ibrav": 2, "basis": [(0.0, 0.0, 0.0)]},
    "bcc": {"ibrav": 3, "basis": [(0.0, 0.0, 0.0)]},
    "diamond": {"ibrav": 2, "basis": [(0.0, 0.0, 0.0), (0.25, 0.25, 0.25)]},
}


def _silicon_cell(a: float) -> list[tuple[float, float, float]]:
    half = a / 2.0
    return [(0.0, half, half), (half, 0.0, half), (half, half, 0.0)]


def _aluminum_fcc_atoms() -> list[Atom]:
    return [
        Atom("Al", (0.0, 0.0, 0.0)),
        Atom("Al", (0.0, 0.5, 0.5)),
        Atom("Al", (0.5, 0.0, 0.5)),
        Atom("Al", (0.5, 0.5, 0.0)),
    ]


def _bulk_atoms(symbol: str, structure: str) -> tuple[int, list[Atom]]:
    key = structure.lower()
    if key not in _BULK_STRUCTURE_DEFS:
        available = ", ".join(sorted(_BULK_STRUCTURE_DEFS))
        raise ValueError(f"Unsupported structure '{structure}'. Choose one of: {available}")

    definition = _BULK_STRUCTURE_DEFS[key]
    atoms = [Atom(symbol, basis) for basis in definition["basis"]]
    return int(definition["ibrav"]), atoms


def _default_control(
    *,
    calculation: str,
    prefix: str,
    pseudo_dir: str,
    outdir: str,
) -> dict[str, str | bool]:
    return {
        "calculation": calculation,
        "prefix": prefix,
        "pseudo_dir": pseudo_dir,
        "outdir": outdir,
        "tstress": True,
        "tprnfor": True,
    }


def silicon_scf(
    *,
    a: float = 5.43,
    ecutwfc: float = 45.0,
    ecutrho: float | None = None,
    k_points: tuple[int, int, int, int, int, int] = (8, 8, 8, 1, 1, 1),
    pseudo_file: str = "Si.pbe-n-kjpaw_psl.1.0.0.UPF",
    calculation: str = "scf",
    prefix: str = "si",
    pseudo_dir: str = "./pseudo",
    outdir: str = "./tmp",
    conv_thr: float = 1.0e-8,
    mixing_beta: float = 0.3,
) -> PWInputDeck:
    """Silicon diamond primitive cell SCF template."""

    if ecutrho is None:
        ecutrho = 8.0 * ecutwfc

    control = _default_control(
        calculation=calculation,
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )
    system = {
        "ibrav": 0,
        "ecutwfc": ecutwfc,
        "ecutrho": ecutrho,
        "occupations": "fixed",
    }
    electrons = {
        "conv_thr": conv_thr,
        "mixing_beta": mixing_beta,
    }

    cell = _silicon_cell(a)

    return PWInputDeck(
        control=control,
        system=system,
        electrons=electrons,
        atomic_species=[Species("Si", _ATOMIC_MASSES["Si"], pseudo_file)],
        atomic_positions=[Atom("Si", (0.0, 0.0, 0.0)), Atom("Si", (0.25, 0.25, 0.25))],
        atomic_positions_mode="crystal",
        k_points_mode="automatic",
        k_points=k_points,
        cell_parameters=cell,
        cell_parameters_mode="angstrom",
    )


def graphene_relax(
    *,
    a: float = 2.46,
    vacuum: float = 18.0,
    ecutwfc: float = 60.0,
    ecutrho: float | None = None,
    k_points: tuple[int, int, int, int, int, int] = (12, 12, 1, 0, 0, 0),
    pseudo_file: str = "C.pbe-n-kjpaw_psl.1.0.0.UPF",
    prefix: str = "graphene",
    pseudo_dir: str = "./pseudo",
    outdir: str = "./tmp",
    conv_thr: float = 1.0e-9,
) -> PWInputDeck:
    """Graphene 2D cell geometry relaxation template."""

    if ecutrho is None:
        ecutrho = 8.0 * ecutwfc

    control = _default_control(
        calculation="relax",
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )
    system = {
        "ibrav": 0,
        "ecutwfc": ecutwfc,
        "ecutrho": ecutrho,
        "occupations": "smearing",
        "smearing": "mv",
        "degauss": 0.02,
    }
    electrons = {"conv_thr": conv_thr, "mixing_beta": 0.3}
    ions = {"ion_dynamics": "bfgs"}

    cell = [
        (a, 0.0, 0.0),
        (0.5 * a, 0.5 * math.sqrt(3.0) * a, 0.0),
        (0.0, 0.0, vacuum),
    ]

    return PWInputDeck(
        control=control,
        system=system,
        electrons=electrons,
        ions=ions,
        atomic_species=[Species("C", _ATOMIC_MASSES["C"], pseudo_file)],
        atomic_positions=[Atom("C", (0.0, 0.0, 0.5)), Atom("C", (1.0 / 3.0, 2.0 / 3.0, 0.5))],
        atomic_positions_mode="crystal",
        k_points_mode="automatic",
        k_points=k_points,
        cell_parameters=cell,
        cell_parameters_mode="angstrom",
    )


def aluminum_vc_relax(
    *,
    a: float = 4.05,
    ecutwfc: float = 45.0,
    ecutrho: float | None = None,
    k_points: tuple[int, int, int, int, int, int] = (12, 12, 12, 1, 1, 1),
    pseudo_file: str = "Al.pbe-n-kjpaw_psl.1.0.0.UPF",
    prefix: str = "al",
    pseudo_dir: str = "./pseudo",
    outdir: str = "./tmp",
    conv_thr: float = 1.0e-9,
    press_conv_thr: float = 0.5,
) -> PWInputDeck:
    """FCC aluminum variable-cell relaxation template."""

    if ecutrho is None:
        ecutrho = 8.0 * ecutwfc

    control = _default_control(
        calculation="vc-relax",
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )
    system = {
        "ibrav": 0,
        "ecutwfc": ecutwfc,
        "ecutrho": ecutrho,
        "occupations": "smearing",
        "smearing": "mv",
        "degauss": 0.02,
    }
    electrons = {"conv_thr": conv_thr, "mixing_beta": 0.3}
    ions = {"ion_dynamics": "bfgs"}
    cell = {"cell_dynamics": "bfgs", "press_conv_thr": press_conv_thr}

    cell_vectors = [(a, 0.0, 0.0), (0.0, a, 0.0), (0.0, 0.0, a)]
    atoms = _aluminum_fcc_atoms()

    return PWInputDeck(
        control=control,
        system=system,
        electrons=electrons,
        ions=ions,
        cell=cell,
        atomic_species=[Species("Al", _ATOMIC_MASSES["Al"], pseudo_file)],
        atomic_positions=atoms,
        atomic_positions_mode="crystal",
        k_points_mode="automatic",
        k_points=k_points,
        cell_parameters=cell_vectors,
        cell_parameters_mode="angstrom",
    )


def silicon_bands_workflow(
    *,
    a: float = 5.43,
    ecutwfc: float = 45.0,
    ecutrho: float | None = None,
    scf_k_points: tuple[int, int, int, int, int, int] = (8, 8, 8, 1, 1, 1),
    pseudo_file: str = "Si.pbe-n-kjpaw_psl.1.0.0.UPF",
    prefix: str = "si_bands",
    pseudo_dir: str = "./pseudo",
    outdir: str = "./tmp",
) -> QEWorkflow:
    """Two-step silicon band structure workflow: SCF then BANDS."""

    scf_deck = silicon_scf(
        a=a,
        ecutwfc=ecutwfc,
        ecutrho=ecutrho,
        k_points=scf_k_points,
        pseudo_file=pseudo_file,
        calculation="scf",
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )

    if ecutrho is None:
        ecutrho = 8.0 * ecutwfc

    cell = _silicon_cell(a)

    bands_control = _default_control(
        calculation="bands",
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )
    bands_system = {
        "ibrav": 0,
        "ecutwfc": ecutwfc,
        "ecutrho": ecutrho,
        "occupations": "fixed",
        "nbnd": 12,
    }
    bands_k_path = [
        (0.0, 0.0, 0.0, 20),  # Gamma
        (0.5, 0.0, 0.5, 20),  # X
        (0.5, 0.25, 0.75, 20),  # W
        (0.375, 0.375, 0.75, 20),  # K
        (0.0, 0.0, 0.0, 20),  # Gamma
        (0.5, 0.5, 0.5, 20),  # L
        (0.625, 0.25, 0.625, 20),  # U
        (0.5, 0.25, 0.75, 20),  # W
        (0.5, 0.5, 0.5, 20),  # L
        (0.375, 0.375, 0.75, 0),  # K
    ]

    bands_deck = PWInputDeck(
        control=bands_control,
        system=bands_system,
        electrons={"conv_thr": 1.0e-10, "mixing_beta": 0.3},
        atomic_species=[Species("Si", _ATOMIC_MASSES["Si"], pseudo_file)],
        atomic_positions=[Atom("Si", (0.0, 0.0, 0.0)), Atom("Si", (0.25, 0.25, 0.25))],
        atomic_positions_mode="crystal",
        k_points_mode="crystal_b",
        k_points=bands_k_path,
        cell_parameters=cell,
        cell_parameters_mode="angstrom",
    )

    return QEWorkflow(
        name="silicon_bands",
        steps={"scf": scf_deck, "bands": bands_deck},
        order=["scf", "bands"],
        notes="Run bands.x and plotting utilities after pw.x bands step to post-process eigenvalues.",
        metadata={"material": "Si", "workflow_type": "bands"},
    )


def silicon_eos_workflow(
    *,
    a0: float = 5.43,
    scale_factors: tuple[float, ...] = (0.94, 0.96, 0.98, 1.0, 1.02, 1.04, 1.06),
    ecutwfc: float = 45.0,
    ecutrho: float | None = None,
    k_points: tuple[int, int, int, int, int, int] = (8, 8, 8, 1, 1, 1),
    pseudo_file: str = "Si.pbe-n-kjpaw_psl.1.0.0.UPF",
    prefix: str = "si_eos",
    pseudo_dir: str = "./pseudo",
    outdir: str = "./tmp",
) -> QEWorkflow:
    """Silicon equation-of-state workflow using a lattice-constant sweep."""

    if not scale_factors:
        raise ValueError("scale_factors must contain at least one value")

    steps: dict[str, PWInputDeck] = {}
    order: list[str] = []

    for scale in scale_factors:
        lattice = a0 * scale
        tag = f"{scale:.3f}".replace(".", "p")
        step_name = f"scf_a{tag}"
        deck = silicon_scf(
            a=lattice,
            ecutwfc=ecutwfc,
            ecutrho=ecutrho,
            k_points=k_points,
            pseudo_file=pseudo_file,
            calculation="scf",
            prefix=f"{prefix}_{tag}",
            pseudo_dir=pseudo_dir,
            outdir=outdir,
        )
        steps[step_name] = deck
        order.append(step_name)

    return QEWorkflow(
        name="silicon_eos",
        steps=steps,
        order=order,
        notes="Extract final total energies and fit E(V) (e.g., Birch-Murnaghan).",
        metadata={"material": "Si", "workflow_type": "equation_of_state"},
    )


def aluminum_dos_pdos_workflow(
    *,
    a: float = 4.05,
    ecutwfc: float = 45.0,
    ecutrho: float | None = None,
    scf_k_points: tuple[int, int, int, int, int, int] = (12, 12, 12, 1, 1, 1),
    nscf_k_points: tuple[int, int, int, int, int, int] = (24, 24, 24, 1, 1, 1),
    pseudo_file: str = "Al.pbe-n-kjpaw_psl.1.0.0.UPF",
    prefix: str = "al_dos",
    pseudo_dir: str = "./pseudo",
    outdir: str = "./tmp",
    dos_emin: float = -15.0,
    dos_emax: float = 15.0,
    dos_deltae: float = 0.01,
) -> QEWorkflow:
    """Metal DOS/PDOS workflow: SCF -> NSCF -> DOS/PROJWFC post-processing."""

    if ecutrho is None:
        ecutrho = 8.0 * ecutwfc

    cell_vectors = [(a, 0.0, 0.0), (0.0, a, 0.0), (0.0, 0.0, a)]
    atoms = _aluminum_fcc_atoms()

    scf_control = _default_control(
        calculation="scf",
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )
    nscf_control = _default_control(
        calculation="nscf",
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )
    scf_system = {
        "ibrav": 0,
        "ecutwfc": ecutwfc,
        "ecutrho": ecutrho,
        "occupations": "smearing",
        "smearing": "mv",
        "degauss": 0.02,
    }
    nscf_system = {
        "ibrav": 0,
        "ecutwfc": ecutwfc,
        "ecutrho": ecutrho,
        "occupations": "tetrahedra",
        "nbnd": 16,
    }

    scf_deck = PWInputDeck(
        control=scf_control,
        system=scf_system,
        electrons={"conv_thr": 1.0e-10, "mixing_beta": 0.3},
        atomic_species=[Species("Al", _ATOMIC_MASSES["Al"], pseudo_file)],
        atomic_positions=atoms,
        atomic_positions_mode="crystal",
        k_points_mode="automatic",
        k_points=scf_k_points,
        cell_parameters=cell_vectors,
        cell_parameters_mode="angstrom",
    )
    nscf_deck = PWInputDeck(
        control=nscf_control,
        system=nscf_system,
        electrons={"conv_thr": 1.0e-10, "mixing_beta": 0.3},
        atomic_species=[Species("Al", _ATOMIC_MASSES["Al"], pseudo_file)],
        atomic_positions=atoms,
        atomic_positions_mode="crystal",
        k_points_mode="automatic",
        k_points=nscf_k_points,
        cell_parameters=cell_vectors,
        cell_parameters_mode="angstrom",
    )

    dos_input = f"""
&DOS
  prefix = '{prefix}',
  outdir = '{outdir}',
  fildos = '{prefix}.dos',
  Emin = {dos_emin},
  Emax = {dos_emax},
  DeltaE = {dos_deltae},
/
"""
    projwfc_input = f"""
&PROJWFC
  prefix = '{prefix}',
  outdir = '{outdir}',
  filpdos = '{prefix}.pdos',
  DeltaE = {dos_deltae},
/
"""

    return QEWorkflow(
        name="aluminum_dos_pdos",
        steps={
            "scf": scf_deck,
            "nscf": nscf_deck,
            "dos": QEStep(
                executable="dos.x",
                input_text=dos_input,
                expected_output_files=[f"{prefix}.dos"],
            ),
            "projwfc": QEStep(
                executable="projwfc.x",
                input_text=projwfc_input,
                expected_output_globs=[f"{prefix}.pdos*"],
            ),
        },
        order=["scf", "nscf", "dos", "projwfc"],
        notes="Run SCF then dense NSCF before DOS/PDOS post-processing.",
        metadata={"material": "Al", "workflow_type": "dos_pdos"},
    )


def silicon_phonon_dispersion_workflow(
    *,
    a: float = 5.43,
    ecutwfc: float = 45.0,
    ecutrho: float | None = None,
    scf_k_points: tuple[int, int, int, int, int, int] = (8, 8, 8, 1, 1, 1),
    q_grid: tuple[int, int, int] = (4, 4, 4),
    pseudo_file: str = "Si.pbe-n-kjpaw_psl.1.0.0.UPF",
    prefix: str = "si_ph",
    pseudo_dir: str = "./pseudo",
    outdir: str = "./tmp",
) -> QEWorkflow:
    """Silicon phonon workflow: SCF -> PH -> Q2R -> MATDYN."""

    scf_deck = silicon_scf(
        a=a,
        ecutwfc=ecutwfc,
        ecutrho=ecutrho,
        k_points=scf_k_points,
        pseudo_file=pseudo_file,
        calculation="scf",
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )

    nq1, nq2, nq3 = q_grid
    ph_input = f"""
&INPUTPH
  tr2_ph = 1.0e-14,
  prefix = '{prefix}',
  outdir = '{outdir}',
  fildyn = '{prefix}.dyn',
  ldisp = .true.,
  nq1 = {nq1},
  nq2 = {nq2},
  nq3 = {nq3},
/
"""
    q2r_input = f"""
&INPUT
  fildyn = '{prefix}.dyn',
  zasr = 'simple',
  flfrc = '{prefix}.fc',
/
"""
    matdyn_input = f"""
&INPUT
  asr = 'simple',
  flfrc = '{prefix}.fc',
  flfrq = '{prefix}.freq',
  q_in_band_form = .true.,
/
6
0.0000 0.0000 0.0000 20
0.5000 0.0000 0.5000 20
0.5000 0.2500 0.7500 20
0.3750 0.3750 0.7500 20
0.0000 0.0000 0.0000 20
0.5000 0.5000 0.5000 20
"""

    return QEWorkflow(
        name="silicon_phonon_dispersion",
        steps={
            "scf": scf_deck,
            "ph": QEStep(
                executable="ph.x",
                input_text=ph_input,
                expected_output_globs=[f"{prefix}.dyn*"],
            ),
            "q2r": QEStep(
                executable="q2r.x",
                input_text=q2r_input,
                expected_output_files=[f"{prefix}.fc"],
            ),
            "matdyn": QEStep(
                executable="matdyn.x",
                input_text=matdyn_input,
                expected_output_files=[f"{prefix}.freq"],
            ),
        },
        order=["scf", "ph", "q2r", "matdyn"],
        notes="Post-process generated files to inspect phonon frequencies along high-symmetry paths.",
        metadata={"material": "Si", "workflow_type": "phonon_dispersion"},
    )


def silicon_bands_dos_reference_workflow(
    *,
    a: float = 5.43,
    ecutwfc: float = 16.0,
    ecutrho: float = 96.0,
    nbnd: int = 8,
    scf_k_points: tuple[int, int, int, int, int, int] = (8, 8, 8, 0, 0, 0),
    pseudo_file: str = "Si.UPF",
    title: str = "Silicon band structure",
    prefix: str = "qe",
    pseudo_dir: str = "./pseudo",
    outdir: str = "./tmp",
    conv_thr: float = 1.0e-6,
    dos_emin: float = -6.0,
    dos_emax: float = 10.0,
    dos_deltae: float = 0.1,
    bands_k_path: list[tuple[float, float, float, float]] | None = None,
    include_plotband: bool = True,
    plotband_fermi_ev: float = 0.0,
    plotband_weight: float = 1.0,
) -> QEWorkflow:
    """Reference silicon SCF + DOS + bands workflow mirroring the nanoHUB dftqe pattern."""

    celldm1 = a * _BOHR_PER_ANGSTROM
    common_system = {
        "ibrav": 2,
        "celldm(1)": celldm1,
        "ecutwfc": ecutwfc,
        "ecutrho": ecutrho,
        "nbnd": nbnd,
        "occupations": "tetrahedra",
        "degauss": 0.0,
    }
    common_electrons = {"conv_thr": conv_thr}
    common_species = [Species("Si", _ATOMIC_MASSES["Si"], pseudo_file)]
    common_positions = [Atom("Si", (0.0, 0.0, 0.0)), Atom("Si", (0.25, 0.25, 0.25))]

    scf_control = _default_control(
        calculation="scf",
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )
    scf_control["title"] = title
    scf_deck = PWInputDeck(
        control=scf_control,
        system=common_system,
        electrons=common_electrons,
        atomic_species=common_species,
        atomic_positions=common_positions,
        atomic_positions_mode="crystal",
        k_points_mode="automatic",
        k_points=scf_k_points,
    )

    dos_input = f"""
&DOS
  prefix = '{prefix}',
  outdir = '{outdir}',
  fildos = '{prefix}.dos',
  Emax = {dos_emax},
  Emin = {dos_emin},
  DeltaE = {dos_deltae},
/
"""

    if bands_k_path is None:
        bands_k_path = [
            (0.5, 0.5, 0.5, 5.0),  # L
            (0.0, 0.0, 0.0, 5.0),  # Gamma
            (1.0, 0.0, 0.0, 5.0),  # X
        ]

    bands_control = _default_control(
        calculation="bands",
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )
    bands_control["title"] = title
    bands_deck = PWInputDeck(
        control=bands_control,
        system=common_system,
        electrons=common_electrons,
        atomic_species=common_species,
        atomic_positions=common_positions,
        atomic_positions_mode="crystal",
        k_points_mode="tpiba_b",
        k_points=bands_k_path,
    )

    bands_input = f"""
&BANDS
  prefix = '{prefix}',
  outdir = '{outdir}',
  filband = '{prefix}.bands.dat',
/
"""

    steps: dict[str, QEStep | PWInputDeck] = {
        "scf": scf_deck,
        "dos": QEStep(
            executable="dos.x",
            input_text=dos_input,
            expected_output_files=[f"{prefix}.dos"],
        ),
        "bands_pw": bands_deck,
        "bands_pp": QEStep(
            executable="bands.x",
            input_text=bands_input,
            expected_output_files=[f"{prefix}.bands.dat"],
        ),
    }
    order = ["scf", "dos", "bands_pw", "bands_pp"]

    if include_plotband:
        plotband_input = f"""{prefix}.bands.dat
-1000 1000
{prefix}.bands.xmgr
{prefix}.bands.ps
{plotband_fermi_ev}
{plotband_weight}, {plotband_fermi_ev}
"""
        steps["plotband"] = QEStep(
            executable="plotband.x",
            input_text=plotband_input,
            input_mode="stdin",
            expected_output_files=[f"{prefix}.bands.xmgr", f"{prefix}.bands.ps"],
        )
        order.append("plotband")

    return QEWorkflow(
        name="silicon_bands_dos_reference",
        steps=steps,
        order=order,
        notes=(
            "Reference flow matching nanoHUB dftqe silicon case: "
            "SCF -> DOS -> bands (pw.x) -> bands.x -> plotband.x."
        ),
        metadata={"material": "Si", "workflow_type": "bands_dos_reference"},
    )


def bulk_electronic_phonon_workflow(
    *,
    symbol: str = "Si",
    structure: str = "diamond",
    a: float = 5.43,
    mass_amu: float | None = None,
    pseudo_file: str | None = None,
    title: str | None = None,
    prefix: str = "qe",
    pseudo_dir: str = "./pseudo",
    outdir: str = "./tmp",
    ecutwfc: float = 16.0,
    ecutrho: float = 96.0,
    nbnd: int = 8,
    occupations: str = "tetrahedra",
    degauss: float = 0.0,
    conv_thr: float = 1.0e-6,
    scf_k_points: tuple[int, int, int, int, int, int] = (8, 8, 8, 0, 0, 0),
    include_dos: bool = True,
    dos_emin: float = -6.0,
    dos_emax: float = 10.0,
    dos_deltae: float = 0.1,
    include_bands: bool = True,
    bands_k_path: list[tuple[float, float, float, float]] | None = None,
    include_plotband: bool = True,
    plotband_fermi_ev: float = 0.0,
    plotband_weight: float = 1.0,
    include_phonon: bool = False,
    phonon_q_grid: tuple[int, int, int] = (2, 2, 2),
    phonon_q_path: list[tuple[float, float, float]] | None = None,
    phonon_q_num: int = 21,
    phonon_tr2_ph: float = 1.0e-12,
) -> QEWorkflow:
    """UI-like configurable bulk workflow with structure and optional phonons."""

    if mass_amu is None:
        if symbol in _ATOMIC_MASSES:
            mass_amu = _ATOMIC_MASSES[symbol]
        else:
            raise ValueError(
                f"mass_amu is required for symbol '{symbol}'. Add it explicitly."
            )
    if pseudo_file is None:
        pseudo_file = f"{symbol}.UPF"
    if title is None:
        title = f"{symbol} {structure} electronic structure"

    ibrav, atoms = _bulk_atoms(symbol, structure)
    celldm1 = a * _BOHR_PER_ANGSTROM

    common_system = {
        "ibrav": ibrav,
        "celldm(1)": celldm1,
        "ecutwfc": ecutwfc,
        "ecutrho": ecutrho,
        "nbnd": nbnd,
        "occupations": occupations,
        "degauss": degauss,
    }
    common_electrons = {"conv_thr": conv_thr}
    common_species = [Species(symbol, mass_amu, pseudo_file)]

    scf_control = _default_control(
        calculation="scf",
        prefix=prefix,
        pseudo_dir=pseudo_dir,
        outdir=outdir,
    )
    scf_control["title"] = title
    scf_deck = PWInputDeck(
        control=scf_control,
        system=common_system,
        electrons=common_electrons,
        atomic_species=common_species,
        atomic_positions=atoms,
        atomic_positions_mode="crystal",
        k_points_mode="automatic",
        k_points=scf_k_points,
    )

    steps: dict[str, QEStep | PWInputDeck] = {"scf": scf_deck}
    order = ["scf"]

    if include_dos:
        dos_input = f"""
&DOS
  prefix = '{prefix}',
  outdir = '{outdir}',
  fildos = '{prefix}.dos',
  Emax = {dos_emax},
  Emin = {dos_emin},
  DeltaE = {dos_deltae},
/
"""
        steps["dos"] = QEStep(
            executable="dos.x",
            input_text=dos_input,
            expected_output_files=[f"{prefix}.dos"],
        )
        order.append("dos")

    if include_bands:
        if bands_k_path is None:
            bands_k_path = [
                (0.5, 0.5, 0.5, 5.0),
                (0.0, 0.0, 0.0, 5.0),
                (1.0, 0.0, 0.0, 5.0),
            ]

        bands_control = _default_control(
            calculation="bands",
            prefix=prefix,
            pseudo_dir=pseudo_dir,
            outdir=outdir,
        )
        bands_control["title"] = title
        bands_deck = PWInputDeck(
            control=bands_control,
            system=common_system,
            electrons=common_electrons,
            atomic_species=common_species,
            atomic_positions=atoms,
            atomic_positions_mode="crystal",
            k_points_mode="tpiba_b",
            k_points=bands_k_path,
        )
        bands_pp_input = f"""
&BANDS
  prefix = '{prefix}',
  outdir = '{outdir}',
  filband = '{prefix}.bands.dat',
/
"""
        steps["bands_pw"] = bands_deck
        steps["bands_pp"] = QEStep(
            executable="bands.x",
            input_text=bands_pp_input,
            expected_output_files=[f"{prefix}.bands.dat"],
        )
        order.extend(["bands_pw", "bands_pp"])

        if include_plotband:
            plotband_input = f"""{prefix}.bands.dat
-1000 1000
{prefix}.bands.xmgr
{prefix}.bands.ps
{plotband_fermi_ev}
{plotband_weight}, {plotband_fermi_ev}
"""
            steps["plotband"] = QEStep(
                executable="plotband.x",
                input_text=plotband_input,
                input_mode="stdin",
                expected_output_files=[f"{prefix}.bands.xmgr", f"{prefix}.bands.ps"],
            )
            order.append("plotband")

    if include_phonon:
        nq1, nq2, nq3 = phonon_q_grid
        ph_input = f"""
&INPUTPH
  tr2_ph = {phonon_tr2_ph},
  prefix = '{prefix}',
  outdir = '{outdir}',
  fildyn = '{prefix}.dyn',
  ldisp = .true.,
  nq1 = {nq1},
  nq2 = {nq2},
  nq3 = {nq3},
/
"""
        q2r_input = f"""
&INPUT
  fildyn = '{prefix}.dyn',
  zasr = 'simple',
  flfrc = '{prefix}.fc',
/
"""
        if phonon_q_path is None:
            phonon_q_path = [
                (0.0, 0.0, 0.0),
                (0.5, 0.0, 0.0),
                (0.5, 0.5, 0.0),
                (0.5, 0.5, 0.5),
                (0.0, 0.0, 0.0),
            ]
        q_path_lines = "\n".join(
            f"{qx:.4f} {qy:.4f} {qz:.4f} {phonon_q_num}"
            for qx, qy, qz in phonon_q_path
        )
        matdyn_input = f"""
&INPUT
  asr = 'simple',
  flfrc = '{prefix}.fc',
  flfrq = '{prefix}.freq',
  q_in_band_form = .true.,
/
{len(phonon_q_path)}
{q_path_lines}
"""
        steps["ph"] = QEStep(
            executable="ph.x",
            input_text=ph_input,
            expected_output_globs=[f"{prefix}.dyn*"],
        )
        steps["q2r"] = QEStep(
            executable="q2r.x",
            input_text=q2r_input,
            expected_output_files=[f"{prefix}.fc"],
        )
        steps["matdyn"] = QEStep(
            executable="matdyn.x",
            input_text=matdyn_input,
            expected_output_files=[f"{prefix}.freq"],
        )
        order.extend(["ph", "q2r", "matdyn"])

    return QEWorkflow(
        name=f"{symbol.lower()}_{structure.lower()}_configurable",
        steps=steps,
        order=order,
        notes=(
            "Configurable bulk workflow: choose structure, then enable/disable "
            "DOS, band-structure, and phonon branches."
        ),
        metadata={
            "material": symbol,
            "structure": structure.lower(),
            "workflow_type": "configurable_bulk",
        },
    )


def available_templates() -> dict[str, str]:
    """Template names and short descriptions."""

    return {
        "silicon_scf": "Bulk silicon SCF with diamond primitive cell.",
        "graphene_relax": "2D graphene ionic relaxation with vacuum spacing.",
        "aluminum_vc_relax": "FCC aluminum variable-cell relaxation.",
        "silicon_bands_workflow": "Two-step SCF + bands workflow for silicon.",
        "silicon_eos_workflow": "Silicon equation-of-state lattice sweep.",
        "aluminum_dos_pdos_workflow": "Aluminum SCF+NSCF with DOS/PDOS post-processing.",
        "silicon_phonon_dispersion_workflow": "Silicon SCF->PH->Q2R->MATDYN phonon workflow.",
        "silicon_bands_dos_reference_workflow": (
            "nanoHUB-style Si SCF+DOS+bands reference workflow."
        ),
        "bulk_electronic_phonon_workflow": (
            "UI-like configurable bulk workflow (structure + DOS/bands/phonons toggles)."
        ),
    }
