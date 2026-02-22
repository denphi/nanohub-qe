from pathlib import Path

from setuptools import find_packages, setup


def read_version() -> str:
    version_ns = {}
    version_file = Path(__file__).parent / "nanohubqe" / "_version.py"
    exec(version_file.read_text(encoding="utf-8"), version_ns)
    return version_ns["__version__"]


setup(
    name="nanohub-qe",
    version=read_version(),
    description=(
        "Python tools for building Quantum ESPRESSO input decks, "
        "running calculations, and visualizing outputs"
    ),
    long_description=(Path(__file__).parent / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    packages=find_packages(include=["nanohubqe", "nanohubqe.*"]),
    include_package_data=True,
    install_requires=[
        "matplotlib>=3.4.2",
        "plotly>=5.0",
    ],
)
