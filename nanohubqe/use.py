"""Environment module loader for nanoHUB Quantum ESPRESSO workflows.

This mirrors the nanoHUB-style ``use`` module behavior in pure Python so
Jupyter notebooks can load Quantum ESPRESSO before running workflows.
"""

from __future__ import annotations

import os
import subprocess
import sys
from string import Template
from typing import Optional, Sequence

try:
    from IPython.core.magic import register_line_magic

    _IPYTHON_AVAILABLE = True
except ImportError:
    _IPYTHON_AVAILABLE = False

# Search paths for module configuration files.
EPATH: list[str] = os.environ.get("ENVIRON_CONFIG_DIRS", "").split()

# Variable substitutions used while parsing module files.
_substitutions: dict[str, str] = {}


def _expand_shell_value(value: str) -> str:
    """Expand shell expressions in a value using bash."""

    try:
        result = subprocess.run(
            ["/bin/bash", "-c", f"echo {value}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return value


def _set_substitution(name: str, value: str) -> None:
    expanded = Template(value).safe_substitute(_substitutions)
    _substitutions[name] = expanded


def _setenv(args: list[str]) -> None:
    if not args:
        return

    name = args[0]
    value = " ".join(args[1:]) if len(args) > 1 else ""
    expanded = _expand_shell_value(Template(value).safe_substitute(_substitutions))
    os.environ[name] = expanded
    _set_substitution(name, expanded)


def _prepend(args: list[str]) -> None:
    if len(args) < 2:
        return

    name, value = args[0], args[1]
    value = _expand_shell_value(Template(value).safe_substitute(_substitutions))

    if name in os.environ and os.environ[name]:
        os.environ[name] = f"{value}:{os.environ[name]}"
    else:
        os.environ[name] = value

    if name == "PYTHONPATH":
        for path in reversed(value.split(":")):
            if path and path not in sys.path:
                sys.path.insert(1, path)


def _parse_module_file(path: str, visited: set[str] | None = None) -> None:
    visited = visited or set()
    if path in visited:
        return
    visited.add(path)

    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split()
            directive = parts[0].lower()

            if directive == "setenv":
                _setenv(parts[1:])
            elif directive == "prepend":
                _prepend(parts[1:])
            elif directive == "use" and len(parts) > 1:
                _use(parts[-1], visited=visited)
            elif "=" in line:
                left, right = line.split("=", 1)
                _set_substitution(left.strip(), right.strip())


def _use(name: str, visited: set[str] | None = None) -> None:
    if not EPATH:
        raise RuntimeError(
            "ENVIRON_CONFIG_DIRS is not set. This loader is intended for nanoHUB "
            "module environments."
        )

    module_path: Optional[str] = None
    for search_dir in EPATH:
        candidate = os.path.join(search_dir, name)
        if os.path.isfile(candidate):
            module_path = candidate
            break

    if module_path is None:
        raise ValueError(f"Could not find module '{name}' in search paths: {EPATH}")

    _parse_module_file(module_path, visited=visited)


def use(name: str) -> None:
    """Load a nanoHUB environment module by name."""

    _use(name)


def list_available_modules(pattern: str | None = None) -> list[str]:
    """List available environment modules in ``ENVIRON_CONFIG_DIRS``."""

    modules: list[str] = []
    for search_dir in EPATH:
        if not os.path.isdir(search_dir):
            continue
        for entry in os.listdir(search_dir):
            if pattern is None or pattern.lower() in entry.lower():
                modules.append(entry)
    return sorted(set(modules))


def _qe_module_candidates(modules: Sequence[str]) -> list[str]:
    """Rank candidate module names likely related to Quantum ESPRESSO."""

    scored: list[tuple[int, str]] = []
    for module in modules:
        lowered = module.lower()
        score = 0
        if "quantum-espresso" in lowered:
            score += 100
        if "espresso" in lowered:
            score += 60
        if lowered.startswith("qe") or "-qe" in lowered:
            score += 40
        if "pw" in lowered:
            score += 10
        if score > 0:
            scored.append((score, module))

    scored.sort(key=lambda item: (-item[0], item[1]))
    return [item[1] for item in scored]


def load_quantum_espresso(module_name: str | None = None) -> str:
    """Load a Quantum ESPRESSO module and return the module name.

    If ``module_name`` is omitted, the function auto-detects the best candidate
    from available modules using name heuristics.
    """

    if module_name:
        use(module_name)
        return module_name

    modules = list_available_modules()
    candidates = _qe_module_candidates(modules)
    if not candidates:
        raise ValueError(
            "Could not auto-detect a Quantum ESPRESSO module. "
            "Pass module_name explicitly, e.g. load_quantum_espresso('espresso-7.x')."
        )

    selected = candidates[0]
    use(selected)
    return selected


def load_qe(module_name: str | None = None) -> str:
    """Alias for :func:`load_quantum_espresso`."""

    return load_quantum_espresso(module_name=module_name)


try:
    _ipython = get_ipython()  # noqa: F821
    if _IPYTHON_AVAILABLE:

        @register_line_magic
        def use_magic(line: str) -> None:
            """IPython magic: ``%use module_name``."""

            module_name = line.strip()
            if not module_name:
                print("Usage: %use <module_name>")
                return
            use(module_name)

        @register_line_magic
        def use_qe(line: str) -> None:
            """IPython magic: ``%use_qe [module_name]``."""

            selected = load_quantum_espresso(module_name=line.strip() or None)
            print(f"Loaded module: {selected}")

        _ipython.register_magic_function(use_magic, magic_name="use")
        _ipython.register_magic_function(use_qe, magic_name="use_qe")
except NameError:
    pass
except Exception:
    pass
