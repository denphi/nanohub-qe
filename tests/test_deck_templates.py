from __future__ import annotations

from nanohubqe import (
    aluminum_vc_relax,
    available_templates,
    gaas_opticdft_epsilon_workflow,
    graphene_relax,
    silicon_scf,
)


def test_silicon_scf_renders_required_sections() -> None:
    deck = silicon_scf(ecutwfc=50.0)
    text = deck.to_string()

    assert "&CONTROL" in text
    assert "&SYSTEM" in text
    assert "ATOMIC_SPECIES" in text
    assert "ATOMIC_POSITIONS crystal" in text
    assert "K_POINTS automatic" in text
    assert " nat = 2," in text
    assert " ntyp = 1," in text


def test_graphene_template_relax_flags() -> None:
    deck = graphene_relax()
    text = deck.to_string()

    assert "calculation = 'relax'" in text
    assert "&IONS" in text
    assert "ion_dynamics = 'bfgs'" in text


def test_aluminum_vc_relax_contains_cell_namelist() -> None:
    deck = aluminum_vc_relax()
    text = deck.to_string()

    assert "calculation = 'vc-relax'" in text
    assert "&CELL" in text
    assert "press_conv_thr" in text


def test_available_templates_lists_physics_workflows() -> None:
    names = available_templates()

    assert "silicon_eos_workflow" in names
    assert "aluminum_dos_pdos_workflow" in names
    assert "silicon_phonon_dispersion_workflow" in names
    assert "silicon_bands_dos_reference_workflow" in names
    assert "bulk_electronic_phonon_workflow" in names
    assert "gaas_opticdft_epsilon_workflow" in names


def test_gaas_opticdft_template_contains_two_species_and_opticdft_env() -> None:
    workflow = gaas_opticdft_epsilon_workflow()
    scf = workflow.steps["scf"]
    optical = workflow.steps["optical"]

    assert workflow.order == ["scf", "optical"]
    assert scf.deck is not None
    assert [species.symbol for species in scf.deck.atomic_species] == ["Ga", "As"]
    assert scf.env["OPTICDFTFileAction"] == "CREATESTORE:SAVE"
    assert optical.env["OPTICDFTFileAction"] == "FETCH:DESTROY"
