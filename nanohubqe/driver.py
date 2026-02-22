"""Rappture/nanoHUB run XML parsing and generation helpers."""

from __future__ import annotations

import shlex
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


@dataclass
class RunCurve:
    """Curve data from a Rappture-style run XML output."""

    curve_id: str
    x: list[float]
    y: list[float]
    x_label: str | None = None
    y_label: str | None = None
    y_units: str | None = None


@dataclass
class NanoHUBRun:
    """Structured representation of a Rappture/nanoHUB run XML document."""

    tool_id: str | None = None
    tool_name: str | None = None
    tool_command: str | None = None
    input_values: dict[str, str] = field(default_factory=dict)
    output_strings: dict[str, str] = field(default_factory=dict)
    output_curves: dict[str, RunCurve] = field(default_factory=dict)
    status: str | None = None
    time: str | None = None

    def to_xml_element(self) -> ET.Element:
        """Convert this run record into an XML tree."""

        root = ET.Element("run")

        tool = ET.SubElement(root, "tool")
        if self.tool_id is not None:
            ET.SubElement(tool, "id").text = self.tool_id
        if self.tool_name is not None:
            ET.SubElement(tool, "name").text = self.tool_name
        if self.tool_command is not None:
            ET.SubElement(tool, "command").text = self.tool_command

        input_node = ET.SubElement(root, "input")
        for key, value in sorted(self.input_values.items()):
            entry = ET.SubElement(input_node, "string", id=key)
            ET.SubElement(entry, "current").text = value

        output = ET.SubElement(root, "output")
        for key, value in sorted(self.output_strings.items()):
            entry = ET.SubElement(output, "string", id=key)
            ET.SubElement(entry, "current").text = value

        for curve in self.output_curves.values():
            curve_node = ET.SubElement(output, "curve", id=curve.curve_id)
            if curve.x_label:
                xaxis = ET.SubElement(curve_node, "xaxis")
                ET.SubElement(xaxis, "label").text = curve.x_label
            if curve.y_label or curve.y_units:
                yaxis = ET.SubElement(curve_node, "yaxis")
                if curve.y_label:
                    ET.SubElement(yaxis, "label").text = curve.y_label
                if curve.y_units:
                    ET.SubElement(yaxis, "units").text = curve.y_units
            component = ET.SubElement(curve_node, "component")
            xy_text = "\n".join(f"{xv} {yv}" for xv, yv in zip(curve.x, curve.y))
            ET.SubElement(component, "xy").text = xy_text

        if self.time:
            ET.SubElement(output, "time").text = self.time
        if self.status:
            ET.SubElement(output, "status").text = self.status

        return root

    def to_xml_string(self) -> str:
        """Serialize this run record to XML text."""

        element = self.to_xml_element()
        ET.indent(element, space="    ")
        body = ET.tostring(element, encoding="unicode")
        return "<?xml version=\"1.0\"?>\n" + body + "\n"

    def write(self, path: str | Path) -> Path:
        """Write this run record as XML to *path*."""

        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(self.to_xml_string(), encoding="utf-8")
        return output_path


def _node_text(node: ET.Element | None) -> str | None:
    if node is None or node.text is None:
        return None
    text = node.text.strip()
    return text if text else None


def _parse_xy(text: str) -> tuple[list[float], list[float]]:
    x_values: list[float] = []
    y_values: list[float] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            x_values.append(float(parts[0]))
            y_values.append(float(parts[1]))
        except ValueError:
            continue
    return x_values, y_values


def _collect_inputs(node: ET.Element, path_parts: list[str], output: dict[str, str]) -> None:
    node_id = node.attrib.get("id")
    current_parts = path_parts + ([node_id] if node_id else [])

    current_node = node.find("current")
    current_text = _node_text(current_node)
    if current_text is not None and current_parts:
        output["/".join(current_parts)] = current_text

    for child in node:
        if child.tag in {"about", "option", "default", "current", "description", "label"}:
            continue
        if isinstance(child.tag, str):
            _collect_inputs(child, current_parts, output)


def parse_run_xml(path: str | Path) -> NanoHUBRun:
    """Parse a Rappture/nanoHUB run XML file."""

    tree = ET.parse(path)
    root = tree.getroot()

    run = NanoHUBRun(
        tool_id=_node_text(root.find("./tool/id")),
        tool_name=_node_text(root.find("./tool/name")),
        tool_command=_node_text(root.find("./tool/command")),
        status=_node_text(root.find("./output/status")),
        time=_node_text(root.find("./output/time")),
    )

    input_root = root.find("./input")
    if input_root is not None:
        for child in input_root:
            _collect_inputs(child, [], run.input_values)

    for string_node in root.findall("./output/string"):
        key = string_node.attrib.get("id")
        if not key:
            continue
        value = _node_text(string_node.find("current"))
        if value is not None:
            run.output_strings[key] = value

    for curve_node in root.findall("./output/curve"):
        curve_id = curve_node.attrib.get("id")
        if not curve_id:
            continue
        xy_text = _node_text(curve_node.find("./component/xy"))
        if not xy_text:
            continue
        x_vals, y_vals = _parse_xy(xy_text)
        run.output_curves[curve_id] = RunCurve(
            curve_id=curve_id,
            x=x_vals,
            y=y_vals,
            x_label=_node_text(curve_node.find("./xaxis/label")),
            y_label=_node_text(curve_node.find("./yaxis/label")),
            y_units=_node_text(curve_node.find("./yaxis/units")),
        )

    return run


def build_run_xml_from_results(
    results: Mapping[str, object],
    *,
    workflow_name: str | None = None,
    tool_id: str = "nanohubqe",
    tool_name: str = "nanohub-qe",
    tool_command: str | None = None,
    curves: Mapping[str, RunCurve] | None = None,
) -> NanoHUBRun:
    """Build a run XML record from workflow execution results."""

    run = NanoHUBRun(
        tool_id=tool_id,
        tool_name=tool_name,
        tool_command=tool_command,
        status="ok",
    )

    if workflow_name:
        run.input_values["workflow/name"] = workflow_name

    for step_name, raw_result in results.items():
        result = raw_result
        input_file = getattr(result, "input_file", None)
        output_file = getattr(result, "output_file", None)
        command = getattr(result, "command", None)
        expected_outputs = getattr(result, "expected_outputs", [])
        discovered_outputs = getattr(result, "discovered_outputs", [])

        if input_file and Path(input_file).exists():
            run.input_values[f"step/{step_name}/input"] = Path(input_file).read_text(
                encoding="utf-8"
            )

        if command:
            run.output_strings[f"step_{step_name}_command"] = shlex.join(command)
        if output_file and Path(output_file).exists():
            run.output_strings[f"step_{step_name}_stdout"] = Path(output_file).read_text(
                encoding="utf-8"
            )
        if expected_outputs:
            run.output_strings[f"step_{step_name}_expected_outputs"] = "\n".join(
                expected_outputs
            )
        if discovered_outputs:
            run.output_strings[f"step_{step_name}_discovered_outputs"] = "\n".join(
                str(path) for path in discovered_outputs
            )

    if curves:
        run.output_curves.update(curves)

    return run
