from __future__ import annotations

from pathlib import Path

from nanohubqe import (
    parse_dos_text,
    parse_matdyn_freq_text,
    parse_pdos_text,
    parse_pw_output,
    read_bands_gnu,
)

_SAMPLE_OUTPUT = """
     iteration #  1
!    total energy              =   -114.55200000 Ry
     iteration #  2
!    total energy              =   -114.56890123 Ry
     the Fermi energy is    5.7281 ev
     P=   -0.42 (kbar)
     JOB DONE.
"""


def test_parse_pw_output_extracts_summary() -> None:
    summary = parse_pw_output(_SAMPLE_OUTPUT)

    assert len(summary.total_energies_ry) == 2
    assert summary.final_total_energy_ry == -114.56890123
    assert summary.fermi_energy_ev == 5.7281
    assert summary.pressure_kbar == -0.42
    assert summary.completed


def test_parse_dos_text_extracts_dos_and_integrated() -> None:
    text = """
# E (eV) dos(E) Int dos(E)
-10.0 0.1 0.0
-9.5  0.3 0.2
-9.0  0.5 0.6
"""
    data = parse_dos_text(text)

    assert data.energies_ev == [-10.0, -9.5, -9.0]
    assert data.density == [0.1, 0.3, 0.5]
    assert data.integrated_density == [0.0, 0.2, 0.6]


def test_parse_pdos_text_defaults_two_channel_labels() -> None:
    text = """
# E (eV) ldos(E) pdos(E)
-5.0 0.2 0.1
-4.0 0.3 0.15
"""
    data = parse_pdos_text(text)

    assert list(data.channels) == ["ldos", "pdos"]
    assert data.channels["ldos"] == [0.2, 0.3]
    assert data.channels["pdos"] == [0.1, 0.15]


def test_parse_matdyn_freq_text_auto_detects_q_vectors() -> None:
    text = """
# qx qy qz w1 w2 w3
0.0 0.0 0.0 0.0 5.0 10.0
0.5 0.0 0.0 1.0 6.0 11.0
0.5 0.5 0.0 2.0 7.0 12.0
"""
    data = parse_matdyn_freq_text(text, q_mode="auto")

    assert data.q_points is not None
    assert data.q_path == [0.0, 0.5, 1.0]
    assert len(data.branches_cm1) == 3
    assert data.branches_cm1[0] == [0.0, 1.0, 2.0]


def test_read_bands_gnu_parses_qe_filband_format(tmp_path: Path) -> None:
    bands_text = """
 &plot nbnd=   2, nks=    3 /
            0.500000  0.500000  0.500000
   -3.474   -0.883
            0.400000  0.400000  0.400000
   -3.936   -0.178
            0.300000  0.300000  0.300000
   -4.699    1.305
"""
    path = tmp_path / "qe.bands.dat"
    path.write_text(bands_text, encoding="utf-8")

    segments = read_bands_gnu(path)

    assert len(segments) == 2
    assert len(segments[0][0]) == 3
    assert segments[0][0][0] == 0.0
    assert segments[0][1] == [-3.474, -3.936, -4.699]
    assert segments[1][1] == [-0.883, -0.178, 1.305]


def test_read_bands_gnu_parses_two_column_segments(tmp_path: Path) -> None:
    bands_text = """
0.0 -1.0
0.5 -0.5

0.0 1.0
0.5 1.5
"""
    path = tmp_path / "bands.dat.gnu"
    path.write_text(bands_text, encoding="utf-8")

    segments = read_bands_gnu(path)

    assert len(segments) == 2
    assert segments[0][0] == [0.0, 0.5]
    assert segments[0][1] == [-1.0, -0.5]
    assert segments[1][0] == [0.0, 0.5]
    assert segments[1][1] == [1.0, 1.5]
