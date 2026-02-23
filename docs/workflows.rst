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
- ``gaas_opticdft_epsilon_workflow``

Configurable UI-like Workflow
-----------------------------

``bulk_electronic_phonon_workflow`` mirrors the common tool UI model:
choose a bulk crystal structure and enable/disable DOS, band structure,
and phonon branches.

.. code-block:: python

   from nanohubqe import bulk_electronic_phonon_workflow

   sim = bulk_electronic_phonon_workflow(
       symbol="Al",
       structure="fcc",      # sc, fcc, bcc, diamond
       mass_amu=26.9815385,
       pseudo_file="Al.UPF",
       include_dos=True,
       include_bands=True,
       include_phonon=True,
       phonon_q_grid=(2, 2, 2),
   )

   sim.run(workdir="runs/al", dry_run=True)

Each step records expected/discovered outputs and workflow-level records:

- ``workflow_outputs.json``

Remote Submit Workflow
----------------------

Use ``sim.run_submit(...)`` to execute a full workflow through HUBzero submit,
wait for completion, and sync outputs for plotting.

.. code-block:: python

   from nanohubqe import QERunner, SubmitConfig, silicon_bands_dos_reference_workflow

   sim = silicon_bands_dos_reference_workflow(include_plotband=False)
   sim.prepare_pseudopotentials(workdir="runs/si-remote")

   runner = QERunner(default_backend="submit", verbose=True)
   submit_cfg = SubmitConfig(
       nodes=4,
       walltime="00:30:00",
       manager="espresso-7.1_mpi-cleanup_pw",
       run_name="sireferenceremote",
       executable_prefix="espresso-7.1",
   )

   # If manager is omitted (or version-mismatched), it is aligned to executable_prefix.
   # If manager is set, step env defaults to:
   #   first  -> OPTICDFTFileAction=CREATESTORE:SAVE
   #   middle -> OPTICDFTFileAction=FETCH:SAVE
   #   last   -> OPTICDFTFileAction=FETCH:DESTROY
   # Disable with: submit_cfg.apply_manager_file_actions = False

   sim.run_submit(
       workdir="runs/si-remote",
       runner=runner,
       submit_config=submit_cfg,
       dry_run=False,
       verbose=True,
       wait=True,
       sync_outputs=True,
   )

OpticDFT Staged Submit Workflow
-------------------------------

``gaas_opticdft_epsilon_workflow`` captures the two-stage submit pattern:
``OPTICDFTFileAction=CREATESTORE:SAVE`` for SCF, then
``OPTICDFTFileAction=FETCH:DESTROY`` for epsilon.

.. code-block:: python

   from nanohubqe import QERunner, SubmitConfig, gaas_opticdft_epsilon_workflow

   sim = gaas_opticdft_epsilon_workflow(
       ga_pseudo_file="Ga.upf",
       as_pseudo_file="As.upf",
   )

   runner = QERunner(default_backend="submit", verbose=True)
   submit_cfg = SubmitConfig(
       nodes=64,
       walltime="480",
       manager="opticdft-espresso-7.1_mpi",
       executable_prefix="espresso-7.1",
       extra_args=["--noquota"],
   )

   sim.run_submit(
       workdir="runs/gaas-optical",
       runner=runner,
       submit_config=submit_cfg,
       wait=True,
       sync_outputs=False,
   )
