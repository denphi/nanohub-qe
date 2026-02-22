Quickstart
==========

Install
-------

.. code-block:: bash

   pip install nanohub-qe

or for development:

.. code-block:: bash

   pip install -e .[dev,docs]

Create an Input Deck
--------------------

.. code-block:: python

   from nanohubqe import silicon_scf

   deck = silicon_scf(ecutwfc=50, k_points=(10, 10, 10, 1, 1, 1))
   deck.write("runs/si/si.in")

Load Quantum ESPRESSO on nanoHUB
--------------------------------

.. code-block:: python

   from nanohubqe import load_quantum_espresso, use

   loaded = load_quantum_espresso()   # auto-detect
   print("Loaded module:", loaded)

   # or explicit:
   # use("quantum-espresso-7.x")

Run a Workflow
--------------

.. code-block:: python

   from nanohubqe import silicon_bands_dos_reference_workflow

   sim = silicon_bands_dos_reference_workflow(include_plotband=False)
   sim.prepare_pseudopotentials(workdir="runs/si-reference")
   sim.run(workdir="runs/si-reference", dry_run=True)

   print(sim.step_result("dos").stdout)
   print(sim.step_result("dos").expected_outputs)

Parse and Plot
--------------

.. code-block:: python

   fig = sim.plot_dos(backend="plotly")
   fig.show()
