Tutorials
=========

Jupyter Notebook
----------------

A complete notebook tutorial is included in this repository:

- ``tutorials/nanohubqe_tutorial.ipynb``
- ``docs/notebooks/nanohubqe_tutorial.ipynb``
- ``tutorials/nanohubqe_advanced_espresso71.ipynb``
- ``docs/notebooks/nanohubqe_advanced_espresso71.ipynb``

Download links:

- :download:`nanohubqe_tutorial.ipynb <notebooks/nanohubqe_tutorial.ipynb>`
- :download:`nanohubqe_advanced_espresso71.ipynb <notebooks/nanohubqe_advanced_espresso71.ipynb>`

The notebook covers:

- loading Quantum ESPRESSO on nanoHUB with ``use`` / ``load_quantum_espresso``
- provisioning missing pseudopotentials with ``sim.prepare_pseudopotentials(...)``
- building a nanoHUB-style Si SCF + DOS + bands workflow with ``sim.run(...)``
- inspecting workflow output records
- plotting total energy, DOS, and bands with ``sim.plot_*`` helpers
- preparing remote execution with HUBzero ``submit`` (dry-run command generation)

The advanced notebook additionally covers:

- explicit loading of ``espresso-7.1``
- configurable SCF + DOS + bands + phonon workflow construction
- plotting phonon dispersion and using mixed matplotlib/plotly backends
