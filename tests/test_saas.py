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


def test_invoice_conflict_fails_until_higher_revision_arrives(tmp_path: Path) -> None:
    root = tmp_path / "saas-invoice-conflict"
    _initial_inputs(root)
    spark = create_spark("enterprise-data-platform-saas-invoice-conflict")
    try:
        first = run(root, spark)
        pointer = root / "tables/gold/current_generation.json"
        with (root / "landing/invoices_batch_001.csv").open(newline="", encoding="utf-8") as stream:
            invoice = next(csv.DictReader(stream))

        def write_invoice(path: Path, row: dict[str, str]) -> None:
            with path.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(row))
                writer.writeheader()
                writer.writerow(row)

        write_invoice(root / "landing/invoices_conflict.csv", dict(invoice, billed_units="191"))
        with pytest.raises(ValueError, match="Conflicting equal revision in invoices"):
            run(root, spark)
        assert json.loads(pointer.read_text())["generation"] == first["generation"]
        write_invoice(root / "landing/invoices_correction.csv", dict(invoice, revision="2", billed_units="200"))
        repaired = run(root, spark)
        assert repaired["reconciliation"][0]["billed_units"] == 200
        assert repaired["reconciliation"][0]["invoice_count"] == 1
        assert repaired["reconciliation"][0]["exception"] is None
        assert json.loads(pointer.read_text())["generation"] == repaired["generation"]
        ledger = [json.loads(line) for line in (root / "evidence/run_ledger.jsonl").read_text().splitlines()]
        assert [entry["status"] for entry in ledger] == ["SUCCESS", "FAILED", "SUCCESS"]
    finally:
        spark.stop()


def test_malformed_rows_and_multiple_invoices_continue_without_fanout(tmp_path: Path) -> None:
    root = tmp_path / "saas-mixed-input"
    _initial_inputs(root)
    with (root / "landing/usage_extra.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["event_id", "revision", "customer_id", "period", "units", "source_updated_at", "extra_note"])
        writer.writeheader()
        writer.writerow({"event_id": "evt-bad", "revision": "1", "customer_id": "ACME-SYNTH", "period": "2026-10", "units": "not-a-number", "source_updated_at": "2026-10-01T00:00:00+00:00", "extra_note": "added"})
        writer.writerow({"event_id": "evt-other", "revision": "1", "customer_id": "OTHER-SYNTH", "period": "2026-11", "units": "3", "source_updated_at": "2026-11-01T00:00:00+00:00", "extra_note": "added"})
        stream.write("evt-short,1\n")
    with (root / "landing/invoices_extra.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["invoice_id", "revision", "customer_id", "period", "billed_units", "unit_price", "currency", "source_updated_at"])
        writer.writeheader()
        writer.writerow({"invoice_id": "inv-9002", "revision": "1", "customer_id": "ACME-SYNTH", "period": "2026-10", "billed_units": "11", "unit_price": "2.50", "currency": "USD", "source_updated_at": "2026-10-01T00:00:00+00:00"})
        writer.writerow({"invoice_id": "inv-9002", "revision": "2", "customer_id": "ACME-SYNTH", "period": "2026-10", "billed_units": "10", "unit_price": "2.50", "currency": "USD", "source_updated_at": "2026-10-02T00:00:00+00:00"})
        writer.writerow({"invoice_id": "inv-other", "revision": "1", "customer_id": "OTHER-SYNTH", "period": "2026-11", "billed_units": "3", "unit_price": "2.50", "currency": "USD", "source_updated_at": "2026-11-01T00:00:00+00:00"})
    (root / "landing/usage_missing.csv").write_text(
        "event_id,revision,customer_id,period,source_updated_at\n"
        "evt-no-units,1,ACME-SYNTH,2026-10,2026-10-01T00:00:00+00:00\n", encoding="utf-8")
    spark = create_spark("enterprise-data-platform-saas-mixed-input")
    try:
        report = run(root, spark)
        rows = {row["customer_id"]: row for row in report["reconciliation"]}
        assert len(rows) == 2
        assert rows["ACME-SYNTH"]["observed_units"] == 200
        assert rows["ACME-SYNTH"]["billed_units"] == 200
        assert rows["ACME-SYNTH"]["invoice_count"] == 2
        assert rows["ACME-SYNTH"]["exception"] is None
        assert rows["OTHER-SYNTH"]["observed_units"] == 3
        assert report["quarantine_rows"] == 3
        assert {"INVALID_UNITS", "MALFORMED_CSV_ROW", "MISSING_REQUIRED_COLUMN"} <= set(report["quarantine_reasons"])
        quarantine = spark.read.format("delta").load(str(root / "tables/gold/generations" / report["generation"] / "input_quarantine"))
        assert {row["reason"] for row in quarantine.collect()} == {"INVALID_UNITS", "MALFORMED_CSV_ROW", "MISSING_REQUIRED_COLUMN"}
    finally:
        spark.stop()


@pytest.mark.parametrize(("unit_price", "currency", "expected"), [
    ("3.00", "USD", "MULTIPLE_UNIT_PRICES"),
    ("2.50", "EUR", "MIXED_CURRENCY"),
])
def test_multiple_invoice_pricing_ambiguity(tmp_path: Path, unit_price: str,
                                            currency: str, expected: str) -> None:
    root = tmp_path / "saas-pricing-ambiguity"
    _initial_inputs(root)
    with (root / "landing/invoices_extra.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["invoice_id", "revision", "customer_id", "period",
                                                     "billed_units", "unit_price", "currency", "source_updated_at"])
        writer.writeheader()
        writer.writerow({"invoice_id": "inv-9002", "revision": "1", "customer_id": "ACME-SYNTH",
                         "period": "2026-10", "billed_units": "10", "unit_price": unit_price,
                         "currency": currency, "source_updated_at": "2026-10-02T00:00:00+00:00"})
    spark = create_spark("enterprise-data-platform-saas-pricing-ambiguity")
    try:
        row = run(root, spark)["reconciliation"][0]
        assert row["invoice_count"] == 2
        assert row["billed_units"] == 200
        assert row["exception"] == expected
        assert row["expected_amount"] is None
        if currency == "EUR":
            assert row["currency"] is None and row["billed_amount"] is None
    finally:
        spark.stop()


def test_period_backfill_and_repair_preserve_unrelated_period(tmp_path: Path) -> None:
    root = tmp_path / "saas-backfill"
    _initial_inputs(root)
    with (root / "landing/usage_other.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["event_id", "revision", "customer_id", "period", "units", "source_updated_at"])
        writer.writeheader()
        writer.writerow({"event_id": "evt-other", "revision": "1", "customer_id": "OTHER-SYNTH", "period": "2026-09", "units": "5", "source_updated_at": "2026-09-01T00:00:00+00:00"})
    spark = create_spark("enterprise-data-platform-saas-backfill")
    try:
        baseline = run(root, spark)
        other = next(row for row in baseline["reconciliation"] if row["period"] == "2026-09")
        pointer = root / "tables/gold/current_generation.json"
        with (root / "landing/usage_correction.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=["event_id", "revision", "customer_id", "period", "units", "source_updated_at"])
            writer.writeheader()
            writer.writerow({"event_id": "evt-002", "revision": "2", "customer_id": "ACME-SYNTH", "period": "2026-10", "units": "70", "source_updated_at": "2026-10-02T00:00:00+00:00"})
        failed = run(root, spark, fail_before_publish=True, start_period="2026-10", end_period="2026-10")
        assert json.loads(pointer.read_text())["generation"] == baseline["generation"]
        recovered = run(root, spark, start_period="2026-10", end_period="2026-10", repair_run_id=failed["run_id"])
        assert recovered["generation"] != baseline["generation"]
        assert next(row for row in recovered["reconciliation"] if row["period"] == "2026-09") == other
        assert next(row for row in recovered["reconciliation"] if row["period"] == "2026-10")["observed_units"] == 190
        def ordered(rows: list[dict[str, object]]) -> list[dict[str, object]]:
            return sorted(rows, key=lambda row: (str(row["customer_id"]), str(row["period"])))

        replay = run(root, spark, start_period="2026-10", end_period="2026-10")
        assert ordered(replay["reconciliation"]) == ordered(recovered["reconciliation"])
        clean = tmp_path / "saas-clean-rebuild"
        shutil.copytree(root / "landing", clean / "landing")
        rebuilt = run(clean, spark)
        assert ordered(rebuilt["reconciliation"]) == ordered(recovered["reconciliation"])
        ledger = [json.loads(line) for line in (root / "evidence/run_ledger.jsonl").read_text().splitlines()]
        assert [row["status"] for row in ledger] == ["SUCCESS", "FAILED_INJECTED", "SUCCESS", "SUCCESS"]
        assert ledger[2]["repairs_run_id"] == failed["run_id"]
    finally:
        spark.stop()
