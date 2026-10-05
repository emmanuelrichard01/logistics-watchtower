"""The validation report's chart helper and the committed report's integrity."""

import re
import xml.etree.ElementTree as ET

from watchtower_simulator.routes import default_data_dir
from watchtower_simulator.validation_report import line_chart

DOCS = default_data_dir().parent / "docs" / "simulator"


def test_line_chart_is_well_formed_svg_with_a_legend_entry_per_series() -> None:
    svg = line_chart(
        "t", "y", [("a", [0, 1, 2], [1, 3, 2]), ("b", [0, 2], [0, 4])], bands=[(1.0, 2.0)]
    )
    root = ET.fromstring(svg)
    texts = [el.text for el in root.iter("{http://www.w3.org/2000/svg}text")]
    assert "a" in texts
    assert "b" in texts


def test_committed_report_references_charts_that_exist_and_no_check_fails() -> None:
    report = (DOCS / "validation.md").read_text(encoding="utf-8")
    images = re.findall(r"\]\((img/[^)]+\.svg)\)", report)
    assert len(images) == 3
    assert all((DOCS / i).exists() for i in images)
    assert "FAILS" not in report
    assert "## What this model does not capture" in report
