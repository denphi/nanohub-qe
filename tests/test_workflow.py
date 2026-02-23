from __future__ import annotations

from nanohubqe import (
    QERunner,
    aluminum_dos_pdos_workflow,
    bulk_electronic_phonon_workflow,
    gaas_opticdft_epsilon_workflow,
    silicon_bands_dos_reference_workflow,
    silicon_bands_workflow,
    silicon_eos_workflow,
    silicon_phonon_dispersion_workflow,
)


def test_silicon_bands_workflow_runs_in_dry_mode(tmp_path) -> None:
    workflow = silicon_bands_workflow()
    runner = QERunner(default_backend="local", pw_executable="pw.x")

    results = runner.run_workflow(workflow, workdir=tmp_path, dry_run=True)

    assert set(results) == {"scf", "bands"}
    assert "pw.x -in scf.in" in results["scf"].stdout
    assert "pw.x -in bands.in" in results["bands"].stdout


def test_aluminum_dos_pdos_workflow_runs_mixed_steps(tmp_path) -> None:
    workflow = aluminum_dos_pdos_workflow()
    runner = QERunner(default_backend="local")

    results = runner.run_workflow(workflow, workdir=tmp_path, dry_run=True)

    assert set(results) == {"scf", "nscf", "dos", "projwfc"}
    assert "pw.x -in scf.in" in results["scf"].stdout
    assert "pw.x -in nscf.in" in results["nscf"].stdout
    assert "dos.x -in dos.in" in results["dos"].stdout
    assert "projwfc.x -in projwfc.in" in results["projwfc"].stdout


def test_silicon_eos_workflow_has_lattice_sweep_steps() -> None:
    workflow = silicon_eos_workflow(scale_factors=(0.98, 1.0, 1.02))

    assert workflow.order == ["scf_a0p980", "scf_a1p000", "scf_a1p020"]
    assert len(workflow.steps) == 3


def test_silicon_phonon_workflow_runs_postprocessing_steps(tmp_path) -> None:
    workflow = silicon_phonon_dispersion_workflow()
    runner = QERunner(default_backend="local")

    results = runner.run_workflow(workflow, workdir=tmp_path, dry_run=True)

    assert set(results) == {"scf", "ph", "q2r", "matdyn"}
    assert "ph.x -in ph.in" in results["ph"].stdout
    assert "q2r.x -in q2r.in" in results["q2r"].stdout
    assert "matdyn.x -in matdyn.in" in results["matdyn"].stdout


def test_silicon_bands_dos_reference_workflow_runs_expected_steps(tmp_path) -> None:
    workflow = silicon_bands_dos_reference_workflow()
    runner = QERunner(default_backend="local")

    results = runner.run_workflow(workflow, workdir=tmp_path, dry_run=True)

    assert set(results) == {"scf", "dos", "bands_pw", "bands_pp", "plotband"}
    assert "pw.x -in scf.in" in results["scf"].stdout
    assert "dos.x -in dos.in" in results["dos"].stdout
    assert "pw.x -in bands_pw.in" in results["bands_pw"].stdout
    assert "bands.x -in bands_pp.in" in results["bands_pp"].stdout
    assert "plotband.x" in results["plotband"].stdout

    dos_text = results["dos"].input_file.read_text(encoding="utf-8")
    assert "&DOS" in dos_text
    assert "fildos = 'qe.dos'" in dos_text

    bands_pp_text = results["bands_pp"].input_file.read_text(encoding="utf-8")
    assert "&BANDS" in bands_pp_text
    assert "filband = 'qe.bands.dat'" in bands_pp_text

    bands_pw_text = results["bands_pw"].input_file.read_text(encoding="utf-8")
    assert "occupations = 'fixed'" in bands_pw_text


def test_bulk_configurable_workflow_supports_structure_and_phonons(tmp_path) -> None:
    workflow = bulk_electronic_phonon_workflow(
        symbol="Al",
        structure="fcc",
        mass_amu=26.9815385,
        pseudo_file="Al.UPF",
        include_dos=False,
        include_bands=False,
        include_phonon=True,
    )
    runner = QERunner(default_backend="local")

    results = runner.run_workflow(workflow, workdir=tmp_path, dry_run=True)

    assert set(results) == {"scf", "ph", "q2r", "matdyn"}
    assert "pw.x -in scf.in" in results["scf"].stdout
    assert "ph.x -in ph.in" in results["ph"].stdout
    assert "q2r.x -in q2r.in" in results["q2r"].stdout
    assert "matdyn.x -in matdyn.in" in results["matdyn"].stdout

    scf_text = results["scf"].input_file.read_text(encoding="utf-8")
    assert "ibrav = 2," in scf_text


def test_bulk_configurable_workflow_uses_valid_dos_and_bands_namelists(tmp_path) -> None:
    workflow = bulk_electronic_phonon_workflow(
        symbol="Si",
        structure="diamond",
        include_dos=True,
        include_bands=True,
        include_plotband=False,
        include_phonon=False,
    )
    runner = QERunner(default_backend="local")

    results = runner.run_workflow(workflow, workdir=tmp_path, dry_run=True)

    dos_text = results["dos"].input_file.read_text(encoding="utf-8")
    assert "&DOS" in dos_text
    assert "fildos = 'qe.dos'" in dos_text

    bands_pp_text = results["bands_pp"].input_file.read_text(encoding="utf-8")
    assert "&BANDS" in bands_pp_text
    assert "filband = 'qe.bands.dat'" in bands_pp_text

    bands_pw_text = results["bands_pw"].input_file.read_text(encoding="utf-8")
    assert "occupations = 'fixed'" in bands_pw_text


def test_gaas_opticdft_workflow_runs_scf_then_optical(tmp_path) -> None:
    workflow = gaas_opticdft_epsilon_workflow()
    runner = QERunner(default_backend="local")

    results = runner.run_workflow(workflow, workdir=tmp_path, dry_run=True)

    assert set(results) == {"scf", "optical"}
    assert "pw.x -in scf.in" in results["scf"].stdout
    assert "epsilon.x -pd .true. -in optical.in" in results["optical"].stdout

    optical_text = results["optical"].input_file.read_text(encoding="utf-8")
    assert "&inputpp" in optical_text
    assert "calculation = 'eps'" in optical_text

    scf_text = results["scf"].input_file.read_text(encoding="utf-8")
    assert "disk_io = 'minimal'" in scf_text
    assert "prefix = 'optical'" in scf_text
    assert "pseudo_dir = './'" in scf_text
    assert "outdir = './'" in scf_text
    assert " nat = 8," in scf_text
    assert " ntyp = 2," in scf_text
    assert " occupations = 'smearing'" in scf_text
    assert " smearing = 'gauss'" in scf_text
    assert " noinv = .true." in scf_text
    assert " nosym = .true." in scf_text
    assert "K_POINTS automatic" in scf_text
    assert " 5 5 5 0 0 0" in scf_text
    assert "CELL_PARAMETERS angstrom" in scf_text
