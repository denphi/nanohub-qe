# nanohub-qe

`nanohub-qe` is a Python library for Quantum ESPRESSO automation.

- Build parameterized `pw.x` input decks
- Use ready-to-run templates for common real simulation cases
- Execute locally (mixed QE executables) or remotely via HUBzero `submit`
- Parse and visualize QE outputs (energy convergence, bands, DOS, PDOS, phonons)
- Record workflow outputs in JSON

The package name is `nanohub-qe`, imported as `nanohubqe`.

## Install

```bash
pip install -e .
```

If you are on an older nanoHUB Python/pip stack and editable install fails, use:

```bash
pip install -e . --no-use-pep517
```

## Quick Start

```python
from nanohubqe import silicon_scf

# Create a bulk Si SCF input deck
si = silicon_scf(
    ecutwfc=50,
    k_points=(10, 10, 10, 1, 1, 1),
    pseudo_dir="./pseudo",
)

print(si.to_string())
si.write("runs/si_scf/si.in")
```

## Jupyter Tutorial

- Basic notebook: `tutorials/nanohubqe_tutorial.ipynb`
- Basic docs copy: `docs/notebooks/nanohubqe_tutorial.ipynb`
- Advanced notebook (`espresso-7.1`): `tutorials/nanohubqe_advanced_espresso71.ipynb`
- Advanced docs copy: `docs/notebooks/nanohubqe_advanced_espresso71.ipynb`

On nanoHUB you can load QE modules directly from Python:

```python
from nanohubqe import load_quantum_espresso, use

loaded = load_quantum_espresso()  # auto-detect best QE module
print("Loaded:", loaded)

# Or explicit module name:
# use("quantum-espresso-7.x")
```

## Real Templates Included

- `silicon_scf`: Diamond silicon SCF (primitive cell)
- `graphene_relax`: 2D graphene ionic relaxation with vacuum
- `aluminum_vc_relax`: FCC aluminum variable-cell relaxation
- `silicon_bands_workflow`: SCF + bands path workflow for silicon
- `silicon_bands_dos_reference_workflow`: nanoHUB-style Si SCF + DOS + bands flow
- `bulk_electronic_phonon_workflow`: choose bulk structure and toggle DOS/bands/phonons
- `silicon_eos_workflow`: lattice sweep for equation-of-state fitting
- `aluminum_dos_pdos_workflow`: SCF + NSCF + `dos.x` + `projwfc.x`
- `silicon_phonon_dispersion_workflow`: SCF + `ph.x` + `q2r.x` + `matdyn.x`

```python
from nanohubqe import available_templates
print(available_templates())
```

## Run Locally

```python
from nanohubqe import QERunner, silicon_scf

deck = silicon_scf()
runner = QERunner(
    pw_executable="pw.x",
    mpi_prefix=["mpirun", "-np", "8"],
    default_backend="local",
)

result = runner.run(deck, workdir="runs/si")
print(result.returncode, result.output_file)
```

## Auto-Provision Pseudopotentials

If `pseudo_dir` does not exist (or a required `*.UPF` file is missing),
you can provision pseudopotentials before running:

```python
from nanohubqe import ensure_workflow_pseudopotentials, silicon_bands_dos_reference_workflow

workflow = silicon_bands_dos_reference_workflow(
    pseudo_dir="./pseudo",
    pseudo_file="Si.UPF",
    include_plotband=False,
)

status = ensure_workflow_pseudopotentials(workflow, workdir="runs/si-reference")
for item in status:
    print(item.pseudo_file, item.action, item.target_path)
```

`ensure_workflow_pseudopotentials` checks local pseudo directories first
(`ESPRESSO_PSEUDO`, `QE_PSEUDO`, etc.), then downloads from Quantum ESPRESSO
UPF repositories if needed.

## Run Remotely with HUBzero `submit`

`nanohubqe` supports wrapping the QE command in a HUBzero `submit` command.
Common flags supported by the runner include:
`-n`, `-w`, `--manager`, `--runName`, `-i`, `--env`, and `--parameters`.

```python
from nanohubqe import QERunner, SubmitConfig, silicon_scf

deck = silicon_scf()
runner = QERunner(default_backend="submit")

submit_cfg = SubmitConfig(
    nodes=16,
    walltime="02:00:00",
    manager="espresso-7.1_mpi-cleanup_pw",
    run_name="si-scf",
    input_files=["pseudo/Si.UPF"],  # additional inputs; qe.in is staged automatically
    executable_prefix="espresso-7.1",  # pw.x -> espresso-7.1_pw
    env={"ESPRESSO_PSEUDO": "./pseudo"},
)

# If manager is omitted (or uses a different espresso version),
# nanohubqe aligns it to executable_prefix automatically.

# Set dry_run=False to submit for real
result = runner.run(deck, workdir="runs/remote-si", submit_config=submit_cfg, dry_run=True)
print(result.stdout)
# submit -n 16 -w 02:00:00 --manager espresso-7.1_mpi-cleanup_pw --runName si-scf \
#   -i pseudo/Si.UPF -i qe.in espresso-7.1_pw -i qe.in
```

For multi-step workflows, use `sim.run_submit(...)` to submit, wait, and sync:

```python
from nanohubqe import QERunner, SubmitConfig, silicon_bands_dos_reference_workflow

sim = silicon_bands_dos_reference_workflow(include_plotband=False)
sim.prepare_pseudopotentials(workdir="runs/si-remote")

runner = QERunner(default_backend="submit")
submit_cfg = SubmitConfig(
    nodes=4,
    walltime="00:30:00",
    manager="espresso-7.1_mpi-cleanup_pw",
    run_name="si-reference-remote",
    executable_prefix="espresso-7.1",
)

sim.run_submit(
    workdir="runs/si-remote",
    runner=runner,
    submit_config=submit_cfg,
    dry_run=False,     # True => command preview only
    wait=True,         # poll submit status for completion
    sync_outputs=True, # try submit download/fetch commands
)

print(sim.step_result("dos").remote_status)
sim.plot_dos(backend="plotly")

# To allow completion even when expected output files are missing locally:
# submit_cfg.require_expected_outputs = False
```

## Multi-step Workflow Example

```python
from nanohubqe import aluminum_dos_pdos_workflow

sim = aluminum_dos_pdos_workflow()
sim.run(workdir="runs/al-dos", dry_run=True)
print(sim.step_result("dos").stdout)      # dos.x -in dos.in
print(sim.step_result("projwfc").stdout)  # projwfc.x -in projwfc.in

# auto-generated records:
# - runs/al-dos/workflow_outputs.json
print(sim.step_result("dos").expected_outputs)
print([p.name for p in sim.step_result("dos").discovered_outputs])
```

## nanoHUB-style Si SCF+DOS+Bands Example

```python
from nanohubqe import silicon_bands_dos_reference_workflow

sim = silicon_bands_dos_reference_workflow(
    a=5.43,
    ecutwfc=16.0,
    ecutrho=96.0,
    nbnd=8,
    scf_k_points=(8, 8, 8, 0, 0, 0),
    dos_emin=-6.0,
    dos_emax=10.0,
    dos_deltae=0.1,
)

sim.run(workdir="runs/si-reference", dry_run=True)
print(sim.step_result("bands_pp").stdout)  # bands.x -in bands_pp.in
```

## UI-like Configurable Structure + Phonons Example

```python
from nanohubqe import bulk_electronic_phonon_workflow

sim = bulk_electronic_phonon_workflow(
    symbol="Al",
    structure="fcc",      # sc, fcc, bcc, diamond
    mass_amu=26.9815385,
    pseudo_file="Al.UPF",
    include_dos=True,
    include_bands=True,
    include_phonon=True,  # enable phonon branch
    phonon_q_grid=(2, 2, 2),
)

sim.run(workdir="runs/al-ui-style", dry_run=True)
print(sim.step_result("ph").stdout)      # ph.x -in ph.in
print(sim.step_result("matdyn").stdout)  # matdyn.x -in matdyn.in
```

## Parse QE Output

```python
from nanohubqe import read_dos, read_matdyn_freq, read_pdos, read_pw_output

summary = read_pw_output("runs/si/qe.out")
print(summary.final_total_energy_ry)
print(summary.fermi_energy_ev)
print(summary.completed)

dos = read_dos("runs/al-dos/al_dos.dos")
pdos = read_pdos("runs/al-dos/al_dos.pdos_atm#1(Al)_wfc#1(s)")
phonons = read_matdyn_freq("runs/si-ph/si_ph.freq")
```

## Plot Results

```python
from nanohubqe import silicon_bands_dos_reference_workflow

sim = silicon_bands_dos_reference_workflow(include_plotband=False)
sim.run(workdir="runs/si-reference", dry_run=False)

# convenience plotting from latest run outputs
sim.plot_total_energy(backend="matplotlib")
fig = sim.plot_dos(backend="plotly")
fig.show()
```

## Notes

- This package builds `pw.x` decks and can orchestrate mixed QE workflows (`pw.x`, `dos.x`, `projwfc.x`, `ph.x`, `q2r.x`, `matdyn.x`, etc.).
- Pseudopotential filenames in templates are defaults and should be updated to match your local files.
