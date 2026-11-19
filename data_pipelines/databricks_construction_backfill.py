"""Hosted construction daily-date backfill and repair acceptance job."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession


def _write_row(path: Path, row: dict[str, str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def _first_row(path: Path) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8") as stream:
        return next(csv.DictReader(stream))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--code-root", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.code_root))
    from data_pipelines.construction_generator import generate
    from data_pipelines.construction_pipeline import run_pipeline

    root = args.root / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    generate(root, "golden")
    landing = root / "landing"
    ticket = _first_row(landing / "scale_tickets_batch_001.csv")
    receipt = _first_row(landing / "site_receipts_batch_001.csv")
    _write_row(landing / "scale_tickets_second.csv", dict(ticket,
        source_record_id="SCALE-A:T-9002", ticket_id="T-9002", delivery_id="D-9002",
        source_updated_at="2026-10-21T14:30:00Z", event_time="2026-10-21T14:30:00Z",
        weighed_at="2026-10-21T14:30:00Z"))
    _write_row(landing / "site_receipts_second.csv", dict(receipt,
        source_record_id="RCPT-9002", receipt_id="RCPT-9002", delivery_id="D-9002",
        source_updated_at="2026-10-21T14:30:00Z", event_time="2026-10-21T14:30:00Z",
        received_at="2026-10-21T14:30:00Z"))
    spark = SparkSession.builder.getOrCreate()
    try:
        baseline = run_pipeline(root, spark)
        prior = next(row for row in baseline["daily_delivered"]
                     if str(row["business_date"]) == "2026-10-21")
        _write_row(landing / "scale_tickets_correction.csv", dict(ticket,
            source_revision="2", source_updated_at="2026-10-22T08:00:00Z",
            gross_weight="11.000"))
        failed = run_pipeline(root, spark, fail_after_product="daily_delivered_quantities",
                              start_date="2026-10-20", end_date="2026-10-20")
        pointer_path = root / "tables" / "gold" / "current_generation.json"
        pointer_after_failure = json.loads(pointer_path.read_text(encoding="utf-8"))["generation"]
        assert failed["status"] == "FAILED_INJECTED"
        assert pointer_after_failure == baseline["generation"]
        repaired = run_pipeline(root, spark, start_date="2026-10-20", end_date="2026-10-20",
                                repair_run_id=failed["run_id"])
        corrected = next(row for row in repaired["daily_delivered"]
                         if str(row["business_date"]) == "2026-10-20")
        other = next(row for row in repaired["daily_delivered"]
                     if str(row["business_date"]) == "2026-10-21")
        assert str(corrected["accepted_net_quantity"]) == "9.000000"
        assert other == prior
        assert json.loads(pointer_path.read_text(encoding="utf-8"))["generation"] == repaired["generation"]
        ledger = [json.loads(line) for line in (root / "evidence" / "run_ledger.jsonl").read_text(encoding="utf-8").splitlines()]
        assert [row["status"] for row in ledger] == ["SUCCESS", "FAILED_INJECTED", "SUCCESS"]
        assert ledger[2]["repairs_run_id"] == failed["run_id"]
    finally:
        spark.stop()
    evidence = {
        "synthetic": True,
        "verification": "Databricks hosted serverless job",
        "verified_at_utc": datetime.now(UTC).isoformat(),
        "root": str(root),
        "job_cases": {
            "baseline": {"run_id": baseline["run_id"], "generation": baseline["generation"]},
            "interrupted_backfill": {"run_id": failed["run_id"], "status": failed["status"],
                                     "consumer_generation_after_failure": pointer_after_failure},
            "linked_repair": {"run_id": repaired["run_id"], "generation": repaired["generation"],
                              "repairs_run_id": failed["run_id"], "corrected_date": "2026-10-20",
                              "corrected_accepted_metric_tonnes": str(corrected["accepted_net_quantity"]),
                              "unrelated_date": "2026-10-21",
                              "unrelated_accepted_metric_tonnes": str(other["accepted_net_quantity"])},
        },
        "assertions_passed": True,
    }
    path = root / "evidence" / "hosted-construction-backfill.json"
    path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
