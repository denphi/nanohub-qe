Tutorials
=========

Jupyter Notebook
----------------

A complete notebook tutorial is included in this repository:

- ``tutorials/nanohubqe_tutorial.ipynb``
- ``docs/notebooks/nanohubqe_tutorial.ipynb``

Download link:

- :download:`nanohubqe_tutorial.ipynb <notebooks/nanohubqe_tutorial.ipynb>`

The notebook covers:

- loading Quantum ESPRESSO on nanoHUB with ``use`` / ``load_quantum_espresso``
- provisioning missing pseudopotentials with ``sim.prepare_pseudopotentials(...)``
- building a nanoHUB-style Si SCF + DOS + bands workflow with ``sim.run(...)``
- inspecting workflow output records
- plotting total energy, DOS, and bands with ``sim.plot_*`` helpers
- preparing remote execution with HUBzero ``submit`` (dry-run command generation)
