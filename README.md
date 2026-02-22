# nanohub-qe

`nanohub-qe` is a Python library for Quantum ESPRESSO automation.

- Build parameterized `pw.x` input decks
- Use ready-to-run templates for common real simulation cases
- Execute locally (mixed QE executables) or remotely via HUBzero `submit`
- Parse and visualize QE outputs (energy convergence, bands, DOS, PDOS, phonons)
- Record workflow outputs in JSON and Rappture-style `run.xml`

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

- Notebook: `tutorials/nanohubqe_tutorial.ipynb`
- Docs copy: `docs/notebooks/nanohubqe_tutorial.ipynb`

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

## Run Remotely with HUBzero `submit`

`nanohubqe` supports wrapping the QE command in a HUBzero `submit` command.
Common flags supported by the runner include:
`--venue`, `--nCpus`, `--wallTime`, `--inputfile`, `--env`, and `--parameters`.

```python
from nanohubqe import QERunner, SubmitConfig, silicon_scf

deck = silicon_scf()
runner = QERunner(default_backend="submit")

submit_cfg = SubmitConfig(
    venue="rcac",
    n_cpus=16,
    wall_time="02:00:00",
    run_name="si-scf",
    input_files=["qe.in"],
    env={"ESPRESSO_PSEUDO": "./pseudo"},
)

# Set dry_run=False to submit for real
result = runner.run(deck, workdir="runs/remote-si", submit_config=submit_cfg, dry_run=True)
print(result.stdout)  # prints generated submit command
```

## Multi-step Workflow Example

```python
from nanohubqe import QERunner, aluminum_dos_pdos_workflow

workflow = aluminum_dos_pdos_workflow()
runner = QERunner(default_backend="local")
results = runner.run_workflow(workflow, workdir="runs/al-dos", dry_run=True)
print(results["dos"].stdout)      # dos.x -in dos.in
print(results["projwfc"].stdout)  # projwfc.x -in projwfc.in

# auto-generated records:
# - runs/al-dos/workflow_outputs.json
# - runs/al-dos/run.xml
print(results["dos"].expected_outputs)
print([p.name for p in results["dos"].discovered_outputs])
```

## nanoHUB-style Si SCF+DOS+Bands Example

```python
from nanohubqe import QERunner, silicon_bands_dos_reference_workflow

workflow = silicon_bands_dos_reference_workflow(
    a=5.43,
    ecutwfc=16.0,
    ecutrho=96.0,
    nbnd=8,
    scf_k_points=(8, 8, 8, 0, 0, 0),
    dos_emin=-6.0,
    dos_emax=10.0,
    dos_deltae=0.1,
)

runner = QERunner(default_backend="local")
results = runner.run_workflow(workflow, workdir="runs/si-reference", dry_run=True)
print(results["bands_pp"].stdout)  # bands.x -in bands_pp.in
```

## UI-like Configurable Structure + Phonons Example

```python
from nanohubqe import QERunner, bulk_electronic_phonon_workflow

workflow = bulk_electronic_phonon_workflow(
    symbol="Al",
    structure="fcc",      # sc, fcc, bcc, diamond
    mass_amu=26.9815385,
    pseudo_file="Al.UPF",
    include_dos=True,
    include_bands=True,
    include_phonon=True,  # enable phonon branch
    phonon_q_grid=(2, 2, 2),
)

runner = QERunner(default_backend="local")
results = runner.run_workflow(workflow, workdir="runs/al-ui-style", dry_run=True)
print(results["ph"].stdout)      # ph.x -in ph.in
print(results["matdyn"].stdout)  # matdyn.x -in matdyn.in
```

## Parse QE Output

```python
from nanohubqe import parse_run_xml, read_dos, read_matdyn_freq, read_pdos, read_pw_output

summary = read_pw_output("runs/si/qe.out")
print(summary.final_total_energy_ry)
print(summary.fermi_energy_ev)
print(summary.completed)

dos = read_dos("runs/al-dos/al_dos.dos")
pdos = read_pdos("runs/al-dos/al_dos.pdos_atm#1(Al)_wfc#1(s)")
phonons = read_matdyn_freq("runs/si-ph/si_ph.freq")
run = parse_run_xml("runs/al-dos/run.xml")
print(run.status, list(run.output_curves)[:3])
```

## Plot Results

```python
from nanohubqe import (
    plot_bands,
    plot_dos,
    plot_pdos,
    plot_phonon_dispersion,
    plot_run_curve,
    plot_total_energy,
)

# matplotlib backend (default)
plot_total_energy("runs/si/qe.out", backend="matplotlib")
plot_bands("runs/si_bands/bands.dat.gnu", fermi_energy_ev=5.8, backend="matplotlib")
plot_dos("runs/si_dos/si.dos", fermi_energy_ev=5.8, backend="matplotlib")
plot_pdos("runs/al-dos/al_dos.pdos_atm#1(Al)_wfc#1(s)", backend="matplotlib")
plot_phonon_dispersion("runs/si-ph/si_ph.freq", backend="matplotlib")

# plotly backend
fig = plot_dos("runs/si_dos/si.dos", backend="plotly")
fig.show()
fig2 = plot_run_curve("runs/al-dos/run.xml", "E_scf", backend="plotly")
fig2.show()
```

## Notes

- This package builds `pw.x` decks and can orchestrate mixed QE workflows (`pw.x`, `dos.x`, `projwfc.x`, `ph.x`, `q2r.x`, `matdyn.x`, etc.).
- Pseudopotential filenames in templates are defaults and should be updated to match your local files.
