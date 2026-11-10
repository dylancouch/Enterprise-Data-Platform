"""SaaS domain control that prevents ambiguous equal-revision publication."""

from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

import pytest

from data_pipelines.saas_pipeline import _initial_inputs, run
from data_pipelines.spark_delta import create_spark


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


def test_late_invoice_is_reported_then_reconciled(tmp_path: Path) -> None:
    root = tmp_path / "saas-late-invoice"
    _initial_inputs(root)
    deferred = root / "deferred"
    deferred.mkdir()
    shutil.move(str(root / "landing/invoices_batch_001.csv"), str(deferred / "invoices_batch_001.csv"))
    spark = create_spark("enterprise-data-platform-saas-late-invoice")
    try:
        pending = run(root, spark)
        assert pending["reconciliation"][0]["exception"] == "MISSING_INVOICE"
        assert next(row for row in pending["source_freshness"] if row["feed"] == "invoices")["status"] == "MISSING_EXPECTED_FILE"
        previous = json.loads((root / "tables/gold/current_generation.json").read_text())["generation"]
        shutil.move(str(deferred / "invoices_batch_001.csv"), str(root / "landing/invoices_batch_001.csv"))
        arrived = run(root, spark)
        assert arrived["reconciliation"][0]["billed_units"] == 190
        assert arrived["reconciliation"][0]["exception"] == "BILLED_USAGE_MISMATCH"
        assert arrived["generation"] != previous
    finally:
        spark.stop()
