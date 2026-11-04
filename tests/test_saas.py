"""SaaS domain control that prevents ambiguous equal-revision publication."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from saas_pipeline import _initial_inputs, run
from spark_delta import create_spark


def test_conflicting_equal_revision_keeps_last_good_generation(tmp_path: Path) -> None:
    root = tmp_path / "saas-conflict"
    _initial_inputs(root)
    spark = create_spark("enterprise-data-platform-saas-conflict")
    try:
        first = run(root, spark)
        pointer_path = root / "tables" / "gold" / "current_generation.json"
        original = json.loads(pointer_path.read_text(encoding="utf-8"))["generation"]
        source_path = root / "landing" / "usage_batch_001.csv"
        with source_path.open(newline="", encoding="utf-8") as stream:
            first_event = next(csv.DictReader(stream))
        conflict = dict(first_event, units="121")
        with (root / "landing" / "usage_conflict.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(conflict))
            writer.writeheader()
            writer.writerow(conflict)
        with pytest.raises(ValueError, match="Conflicting equal revision in usage"):
            run(root, spark)
        assert first["generation"] == original
        assert json.loads(pointer_path.read_text(encoding="utf-8"))["generation"] == original
    finally:
        spark.stop()
