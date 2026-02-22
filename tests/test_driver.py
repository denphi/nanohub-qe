from __future__ import annotations

from dataclasses import dataclass

from nanohubqe import build_run_xml_from_results, parse_run_xml


_SAMPLE_RUN_XML = """<?xml version=\"1.0\"?>
<run>
  <tool>
    <id>dftqe</id>
    <name>DFT calculations with Quantum ESPRESSO</name>
    <command>@tool/nMST_QEwr @driver</command>
  </tool>
  <input>
    <group id="InputModel">
      <group id="model">
        <string id="title"><current>Silicon band structure</current></string>
      </group>
    </group>
  </input>
  <output>
    <curve id="E_scf">
      <xaxis><label>SCF iterations</label></xaxis>
      <yaxis><label>Energy</label><units>Ry</units></yaxis>
      <component>
        <xy>1 -15.8
2 -15.9
</xy>
      </component>
    </curve>
    <string id="input_pwx"><current>&amp;control\n/</current></string>
    <status>ok</status>
  </output>
</run>
"""


@dataclass
class _DummyResult:
    command: list[str]
    input_file: object
    output_file: object
    expected_outputs: list[str]
    discovered_outputs: list[object]


def test_parse_run_xml_extracts_inputs_and_curves(tmp_path) -> None:
    xml_path = tmp_path / "run.xml"
    xml_path.write_text(_SAMPLE_RUN_XML, encoding="utf-8")

    run = parse_run_xml(xml_path)

    assert run.tool_id == "dftqe"
    assert run.input_values["InputModel/model/title"] == "Silicon band structure"
    assert "E_scf" in run.output_curves
    assert run.output_curves["E_scf"].x == [1.0, 2.0]
    assert run.output_curves["E_scf"].y == [-15.8, -15.9]


def test_build_run_xml_from_results_writes_roundtrip_xml(tmp_path) -> None:
    input_path = tmp_path / "scf.in"
    output_path = tmp_path / "scf.out"
    input_path.write_text("&control\n/\n", encoding="utf-8")
    output_path.write_text("JOB DONE.\n", encoding="utf-8")

    results = {
        "scf": _DummyResult(
            command=["pw.x", "-in", "scf.in"],
            input_file=input_path,
            output_file=output_path,
            expected_outputs=["scf.out"],
            discovered_outputs=[output_path],
        )
    }

    run = build_run_xml_from_results(results, workflow_name="si")
    run_path = run.write(tmp_path / "generated.xml")

    parsed = parse_run_xml(run_path)
    assert parsed.tool_id == "nanohubqe"
    assert parsed.input_values["workflow/name"] == "si"
    assert "step_scf_stdout" in parsed.output_strings
