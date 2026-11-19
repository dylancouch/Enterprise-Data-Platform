"""Hosted SaaS validation and period-repair acceptance job."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--code-root", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.code_root))
    from data_pipelines.saas_pipeline import _initial_inputs, run

    root = args.root / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    _initial_inputs(root)
    landing = root / "landing"
    with (landing / "usage_extra.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["event_id", "revision", "customer_id", "period", "units", "source_updated_at"])
        writer.writeheader()
        writer.writerow({"event_id": "evt-invalid", "revision": "1", "customer_id": "ACME-SYNTH", "period": "2026-10", "units": "bad", "source_updated_at": datetime.now(UTC).isoformat()})
    with (landing / "invoices_extra.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["invoice_id", "revision", "customer_id", "period", "billed_units", "unit_price", "currency", "source_updated_at"])
        writer.writeheader()
        writer.writerow({"invoice_id": "inv-9002", "revision": "1", "customer_id": "ACME-SYNTH", "period": "2026-10", "billed_units": "10", "unit_price": "2.50", "currency": "USD", "source_updated_at": datetime.now(UTC).isoformat()})
    _initial_inputs(root / "backfill")
    old = datetime.now(UTC).isoformat()
    with (root / "backfill" / "landing" / "usage_prior.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["event_id", "revision", "customer_id", "period", "units", "source_updated_at"])
        writer.writeheader()
        writer.writerow({"event_id": "evt-prior", "revision": "1", "customer_id": "PRIOR-SYNTH", "period": "2026-09", "units": "5", "source_updated_at": old})
    spark = SparkSession.builder.getOrCreate()
    try:
        validation = run(root, spark)
        acme = next(row for row in validation["reconciliation"] if row["customer_id"] == "ACME-SYNTH")
        assert validation["quarantine_rows"] == 1
        assert acme["invoice_count"] == 2 and acme["billed_units"] == 200

        baseline = run(root / "backfill", spark)
        prior_before = next(row for row in baseline["reconciliation"] if row["period"] == "2026-09")["observed_units"]
        assert prior_before == 5
        with (root / "backfill" / "landing" / "usage_correction.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=["event_id", "revision", "customer_id", "period", "units", "source_updated_at"])
            writer.writeheader()
            writer.writerow({"event_id": "evt-002", "revision": "2", "customer_id": "ACME-SYNTH", "period": "2026-10", "units": "70", "source_updated_at": datetime.now(UTC).isoformat()})
        repaired = run(root / "backfill", spark, start_period="2026-10", end_period="2026-10")
        rows = repaired["reconciliation"]
        oct_row = next(row for row in rows if row["period"] == "2026-10")
        prior_after = next(row for row in rows if row["period"] == "2026-09")["observed_units"]
        assert oct_row["observed_units"] == 190 and prior_after == prior_before
    finally:
        spark.stop()
    evidence = {
        "synthetic": True,
        "verification": "Databricks hosted serverless job",
        "verified_at_utc": datetime.now(UTC).isoformat(),
        "root": str(root),
        "runs": {
            "saas_validation_multiple_invoice_and_quarantine": {"run_id": validation["run_id"], "generation": validation["generation"], "quarantine_rows": validation["quarantine_rows"], "acme_billed_units": acme["billed_units"], "acme_invoice_count": acme["invoice_count"]},
            "saas_period_backfill": {"baseline_run_id": baseline["run_id"], "repair_run_id": repaired["run_id"], "baseline_generation": baseline["generation"], "generation": repaired["generation"], "rebuilt_period": "2026-10", "observed_units_after_correction": oct_row["observed_units"], "unrelated_period": "2026-09", "unrelated_period_observed_units": prior_after},
        },
        "assertions_passed": True,
    }
    path = root / "evidence" / "hosted-saas-acceptance.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2, default=str))


if __name__ == "__main__":
    main()
