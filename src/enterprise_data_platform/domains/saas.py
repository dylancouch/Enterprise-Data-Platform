"""Synthetic SaaS usage-to-invoice pipeline and recovery walkthrough."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from enterprise_data_platform import PROJECT_DESCRIPTION, PROJECT_NAME
from enterprise_data_platform.core import (
    create_spark,
    freshness_status,
    latest_revision,
    merge_raw,
    publish_generation,
    write_delta,
)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _initial_inputs(root: Path) -> None:
    landing = root / "landing"
    old = (datetime.now(UTC) - timedelta(days=5)).isoformat()
    now = datetime.now(UTC).isoformat()
    usage = [
        {"event_id": "evt-001", "revision": "1", "customer_id": "ACME-SYNTH", "period": "2026-10", "units": "120", "source_updated_at": old},
        {"event_id": "evt-002", "revision": "1", "customer_id": "ACME-SYNTH", "period": "2026-10", "units": "80", "source_updated_at": old},
    ]
    invoices = [{"invoice_id": "inv-9001", "revision": "1", "customer_id": "ACME-SYNTH", "period": "2026-10", "billed_units": "190", "unit_price": "2.50", "currency": "USD", "source_updated_at": now}]
    _write_csv(landing / "usage_batch_001.csv", usage)
    _write_csv(landing / "invoices_batch_001.csv", invoices)


def _add_correction(root: Path) -> None:
    row = {"event_id": "evt-002", "revision": "2", "customer_id": "ACME-SYNTH", "period": "2026-10", "units": "70", "source_updated_at": datetime.now(UTC).isoformat()}
    _write_csv(root / "landing" / "usage_correction_002.csv", [row])


def _write_evidence(root: Path, report: dict[str, object]) -> None:
    path = root / "evidence" / "runs" / f"{report['run_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    (root / "evidence" / "run.json").write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")


def run(root: Path, spark: SparkSession, fail_before_publish: bool = False) -> dict[str, object]:
    root = root.resolve()
    run_id = str(uuid.uuid4())
    frames: dict[str, object] = {}
    freshness = []
    duplicates_suppressed = 0
    for feed, key in (("usage", "event_id"), ("invoices", "invoice_id")):
        bronze = root / "tables" / "bronze" / feed
        incoming = []
        for path in sorted((root / "landing").glob(f"{feed}_*.csv")):
            content = path.read_bytes()
            frame = spark.read.option("header", True).option("inferSchema", False).csv(str(path))
            payload_columns = frame.columns
            incoming.append(frame.withColumn("_source_file", F.lit(path.name))
                .withColumn("_content_hash", F.lit(hashlib.sha256(content).hexdigest()))
                .withColumn("_payload_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload_columns])), 256)))
        batch = incoming[0] if incoming else None
        for other in incoming[1:]:
            batch = batch.unionByName(other)
        combined, retry_duplicates = merge_raw(spark, batch, bronze, [key, "revision", "_payload_hash"])
        duplicates_suppressed += retry_duplicates
        if combined is None:
            freshness.append({"feed": feed, "status": "MISSING_EXPECTED_FILE", "files": 0, "rows": 0, "latest_update": None})
            continue
        conflicting = (combined.groupBy(key, "revision")
            .agg(F.countDistinct("_payload_hash").alias("payload_count"))
            .filter(F.col("payload_count") > 1))
        if conflicting.limit(1).count():
            raise ValueError(f"Conflicting equal revision in {feed}; no deterministic winner is publishable")
        current = latest_revision(combined, [key], "revision", "source_updated_at", "_payload_hash")
        frames[feed] = current
        latest = current.agg(F.max("source_updated_at")).first()[0]
        file_count = len(list((root / "landing").glob(f"{feed}_*.csv")))
        status = freshness_status(file_count, datetime.fromisoformat(latest) if latest else None, datetime.now(UTC))
        freshness.append({"feed": feed, "status": status, "files": file_count, "rows": current.count(), "latest_update": latest})

    usage_totals = frames["usage"].groupBy("customer_id", "period").agg(F.sum(F.col("units").cast("long")).alias("observed_units"))
    invoice = frames["invoices"].select("invoice_id", "customer_id", "period", F.col("billed_units").cast("long").alias("billed_units"), F.col("unit_price").cast("decimal(12,2)").alias("unit_price"), "currency")
    reconciliation = invoice.join(usage_totals, ["customer_id", "period"], "full_outer").withColumn("unit_difference", F.col("billed_units") - F.col("observed_units")).withColumn("expected_amount", (F.col("observed_units") * F.col("unit_price")).cast("decimal(14,2)")).withColumn("exception", F.when(F.col("billed_units").isNull(), "MISSING_INVOICE").when(F.col("observed_units").isNull(), "MISSING_USAGE").when(F.col("unit_difference") != 0, "BILLED_USAGE_MISMATCH"))
    result = [row.asDict(recursive=True) for row in reconciliation.collect()]
    generation = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ") + "-" + run_id[:8]
    output = root / "tables" / "gold" / "generations" / generation
    output.mkdir(parents=True, exist_ok=True)
    write_delta(reconciliation, output / "usage_invoice_reconciliation")
    pointer_path = root / "tables" / "gold" / "current_generation.json"
    last_good = json.loads(pointer_path.read_text(encoding="utf-8"))["generation"] if pointer_path.exists() else None
    published = not fail_before_publish
    if published:
        publish_generation(pointer_path, generation, run_id)
    report = {"synthetic": True, "project": PROJECT_NAME, "description": PROJECT_DESCRIPTION, "domain": "saas-usage-billing", "scenario": "saas-usage-to-invoice", "run_id": run_id, "status": "SUCCESS" if published else "FAILED_INJECTED", "published": published, "generation": generation if published else None, "staged_generation": generation, "last_good_generation": last_good, "publication_pointer": str(pointer_path), "duplicate_business_events_suppressed": duplicates_suppressed, "source_freshness": freshness, "reconciliation": result}
    _write_evidence(root, report)
    return report


def _brief(report: dict[str, object]) -> dict[str, object]:
    row = report["reconciliation"][0]
    return {"status": report["status"], "published": report["published"], "generation": report["generation"], "staged_generation": report["staged_generation"], "last_good_generation": report["last_good_generation"], "duplicates_suppressed": report["duplicate_business_events_suppressed"], "observed_units": row["observed_units"], "billed_units": row["billed_units"], "exception": row["exception"], "source_freshness": report["source_freshness"]}


def walkthrough(root: Path) -> dict[str, object]:
    run_root = root / f"walkthrough-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    _initial_inputs(run_root)
    spark = create_spark("saas-usage-billing-walkthrough")
    try:
        initial = run(run_root, spark)
        # A renamed retry has byte-identical content and the same business keys.
        shutil.copy2(run_root / "landing" / "usage_batch_001.csv", run_root / "landing" / "usage_retry_renamed.csv")
        replay = run(run_root, spark)
        pointer_path = run_root / "tables" / "gold" / "current_generation.json"
        pointer_before_failure = json.loads(pointer_path.read_text(encoding="utf-8"))["generation"]
        _add_correction(run_root)
        interrupted = run(run_root, spark, fail_before_publish=True)
        pointer_after_failure = json.loads(pointer_path.read_text(encoding="utf-8"))["generation"]
        recovered = run(run_root, spark)
    finally:
        spark.stop()
    assert initial["reconciliation"][0]["exception"] == "BILLED_USAGE_MISMATCH"
    assert replay["duplicate_business_events_suppressed"] >= 2
    assert interrupted["status"] == "FAILED_INJECTED" and pointer_before_failure == pointer_after_failure
    assert recovered["reconciliation"][0]["observed_units"] == 190 and recovered["reconciliation"][0]["exception"] is None
    assert json.loads(pointer_path.read_text(encoding="utf-8"))["generation"] == recovered["generation"]
    timeline = {"synthetic": True, "project": PROJECT_NAME, "description": PROJECT_DESCRIPTION, "domain": "saas-usage-billing", "scenario": "saas-usage-to-invoice-replay-correction-recovery", "root": str(run_root), "steps": {"initial_mismatch": _brief(initial), "renamed_file_replay": _brief(replay), "interrupted_correction": _brief(interrupted), "recovered_correction": _brief(recovered)}, "pointer_retained_during_failure": pointer_before_failure == pointer_after_failure, "pointer_after_recovery": recovered["generation"]}
    evidence = run_root / "evidence" / "walkthrough.json"
    evidence.write_text(json.dumps(timeline, indent=2, default=str) + "\n", encoding="utf-8")
    return timeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the synthetic SaaS usage-to-invoice recovery walkthrough")
    parser.add_argument("--root", type=Path, default=Path("data/saas-demo"))
    args = parser.parse_args()
    print(json.dumps(walkthrough(args.root), indent=2, default=str))


if __name__ == "__main__":
    main()
