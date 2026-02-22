from __future__ import annotations

import importlib

from nanohubqe import list_available_modules as list_modules_public
from nanohubqe import load_qe, load_quantum_espresso as load_qe_public
from nanohubqe import use as use_public
from nanohubqe.use import (
    _substitutions,
    list_available_modules,
    load_quantum_espresso,
    use,
)

usemod = importlib.import_module("nanohubqe.use")


def test_use_loads_module_and_updates_environment(tmp_path, monkeypatch) -> None:
    module_file = tmp_path / "quantum-espresso-7.3"
    module_file.write_text(
        """
setenv QE_ROOT /opt/qe
prepend PATH /opt/qe/bin
setenv QE_BIN $QE_ROOT/bin
""".strip()
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.delenv("QE_ROOT", raising=False)
    monkeypatch.delenv("QE_BIN", raising=False)

    usemod.EPATH = [str(tmp_path)]
    _substitutions.clear()
    use("quantum-espresso-7.3")

    assert usemod.os.environ["QE_ROOT"] == "/opt/qe"
    assert usemod.os.environ["QE_BIN"] == "/opt/qe/bin"
    assert usemod.os.environ["PATH"].startswith("/opt/qe/bin:")


def test_load_quantum_espresso_auto_selects_best_candidate(tmp_path) -> None:
    for name in ["random-tool", "qe-7.2", "quantum-espresso-7.3"]:
        (tmp_path / name).write_text("setenv X 1\n", encoding="utf-8")

    usemod.EPATH = [str(tmp_path)]
    _substitutions.clear()

    selected = load_quantum_espresso()
    assert selected == "quantum-espresso-7.3"


def test_list_available_modules_with_pattern(tmp_path) -> None:
    for name in ["qe-7.2", "espresso-6.8", "other-tool"]:
        (tmp_path / name).write_text("setenv X 1\n", encoding="utf-8")

    usemod.EPATH = [str(tmp_path)]
    filtered = list_available_modules("qe")

    assert filtered == ["qe-7.2"]


def test_public_use_functions_are_importable() -> None:
    assert callable(use_public)
    assert callable(load_qe_public)
    assert callable(load_qe)
    assert callable(list_modules_public)
