Workflows
=========

Built-in Templates
------------------

- ``silicon_scf``
- ``graphene_relax``
- ``aluminum_vc_relax``
- ``silicon_bands_workflow``
- ``silicon_bands_dos_reference_workflow``
- ``bulk_electronic_phonon_workflow``
- ``silicon_eos_workflow``
- ``aluminum_dos_pdos_workflow``
- ``silicon_phonon_dispersion_workflow``

Configurable UI-like Workflow
-----------------------------

``bulk_electronic_phonon_workflow`` mirrors the common tool UI model:
choose a bulk crystal structure and enable/disable DOS, band structure,
and phonon branches.

.. code-block:: python

   from nanohubqe import QERunner, bulk_electronic_phonon_workflow

   workflow = bulk_electronic_phonon_workflow(
       symbol="Al",
       structure="fcc",      # sc, fcc, bcc, diamond
       mass_amu=26.9815385,
       pseudo_file="Al.UPF",
       include_dos=True,
       include_bands=True,
       include_phonon=True,
       phonon_q_grid=(2, 2, 2),
   )

   runner = QERunner(default_backend="local")
   results = runner.run_workflow(workflow, workdir="runs/al", dry_run=True)

Each step records expected/discovered outputs and workflow-level records:

- ``workflow_outputs.json``
