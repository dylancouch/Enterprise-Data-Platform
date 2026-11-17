"""Synthetic SaaS usage-to-invoice pipeline and recovery walkthrough."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_pipelines import PROJECT_DESCRIPTION, PROJECT_NAME
from data_pipelines.spark_delta import (
    create_spark,
    freshness_status,
    latest_revision,
    merge_raw,
    publish_generation,
    read_delta,
    write_delta,
)

FEED_FIELDS = {
    "usage": ("event_id", "revision", "customer_id", "period", "units", "source_updated_at"),
    "invoices": ("invoice_id", "revision", "customer_id", "period", "billed_units", "unit_price", "currency", "source_updated_at"),
}


def _ledger_path(root: Path) -> Path:
    return root / "evidence" / "run_ledger.jsonl"


def _append_ledger(root: Path, entry: dict[str, object]) -> None:
    path = _ledger_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    entry = {"domain": "saas", **entry}
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(entry, default=str, sort_keys=True) + "\n")


def _ledger_entry(root: Path, run_id: str) -> dict[str, object]:
    path = _ledger_path(root)
    if not path.exists():
        raise ValueError(f"Unknown run ID: {run_id}")
    matches = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
               if json.loads(line)["run_id"] == run_id]
    if not matches:
        raise ValueError(f"Unknown run ID: {run_id}")
    return matches[-1]


def _incoming_file(spark: SparkSession, path: Path, feed: str) -> tuple[object | None, list[dict[str, object]]]:
    """Keep malformed CSV rows in evidence while accepting valid rows from a batch."""
    bad: list[dict[str, object]] = []
    try:
        with path.open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream, strict=True)
            fields = reader.fieldnames or []
            missing = set(FEED_FIELDS[feed]) - set(fields)
            rows = list(reader)
    except (csv.Error, UnicodeError):
        return None, [{"feed": feed, "source_file": path.name, "row_number": None,
                       "reason": "MALFORMED_CSV_FILE", "detail": "CSV parsing failed"}]
    if missing:
        return None, [{"feed": feed, "source_file": path.name, "row_number": 0,
                       "reason": "MISSING_REQUIRED_COLUMN", "detail": ",".join(sorted(missing))}]
    if len(fields) != len(set(fields)):
        return None, [{"feed": feed, "source_file": path.name, "row_number": 0,
                       "reason": "DUPLICATE_COLUMN", "detail": "CSV header repeats a column"}]
    accepted = []
    for number, row in enumerate(rows, start=2):
        if None in row or any(value is None for value in row.values()):
            bad.append({"feed": feed, "source_file": path.name, "row_number": number,
                        "reason": "MALFORMED_CSV_ROW", "detail": "Field count differs from header"})
        else:
            accepted.append(row)
    if not accepted:
        return None, bad
    schema = T.StructType([T.StructField(field, T.StringType()) for field in fields])
    frame = spark.createDataFrame([tuple(row[field] for field in fields) for row in accepted], schema)
    content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    frame = (frame.withColumn("_source_file", F.lit(path.name))
             .withColumn("_content_hash", F.lit(content_hash))
             .withColumn("_payload_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in fields])), 256)))
    return frame, bad


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


def run(root: Path, spark: SparkSession, fail_before_publish: bool = False,
        start_period: str | None = None, end_period: str | None = None,
        repair_run_id: str | None = None) -> dict[str, object]:
    """Publish a complete or period-scoped reconciliation from retained source evidence."""
    root = root.resolve()
    run_id = str(uuid.uuid4())
    started = datetime.now(UTC).isoformat()
    if (start_period is None) != (end_period is None):
        raise ValueError("Both start_period and end_period are required for backfill")
    if start_period is not None and (not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", start_period)
                                     or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", end_period)
                                     or start_period > end_period):
        raise ValueError("Backfill periods must be valid YYYY-MM values in ascending order")
    if repair_run_id is not None:
        failed = _ledger_entry(root, repair_run_id)
        if failed["status"] not in {"FAILED", "FAILED_INJECTED"}:
            raise ValueError("Only a failed run can be repaired")
        if (start_period, end_period) != (failed.get("start_period"), failed.get("end_period")):
            raise ValueError("Repair must use the failed run's period range")
    pointer_path = root / "tables" / "gold" / "current_generation.json"
    last_good = json.loads(pointer_path.read_text(encoding="utf-8"))["generation"] if pointer_path.exists() else None
    if start_period is not None and last_good is None:
        raise ValueError("Backfill requires a published baseline generation")
    try:
        report = _run_impl(root, spark, run_id, started, last_good, fail_before_publish,
                           start_period, end_period, repair_run_id)
    except Exception as exc:
        _append_ledger(root, {"run_id": run_id, "status": "FAILED", "started_at_utc": started,
                              "finished_at_utc": datetime.now(UTC).isoformat(),
                              "start_period": start_period, "end_period": end_period,
                              "last_good_generation": last_good, "repairs_run_id": repair_run_id,
                              "error_type": type(exc).__name__, "error": str(exc)})
        raise
    _append_ledger(root, {"run_id": run_id, "status": report["status"], "started_at_utc": started,
                          "finished_at_utc": datetime.now(UTC).isoformat(),
                          "start_period": start_period, "end_period": end_period,
                          "last_good_generation": last_good, "generation": report["generation"],
                          "staged_generation": report["staged_generation"],
                          "repairs_run_id": repair_run_id})
    return report


def _run_impl(root: Path, spark: SparkSession, run_id: str, started: str, last_good: str | None,
              fail_before_publish: bool, start_period: str | None, end_period: str | None,
              repair_run_id: str | None) -> dict[str, object]:
    frames: dict[str, object] = {}
    freshness = []
    duplicates_suppressed = 0
    malformed: list[dict[str, object]] = []
    for feed, key in (("usage", "event_id"), ("invoices", "invoice_id")):
        bronze = root / "tables" / "bronze" / feed
        incoming = []
        files = sorted((root / "landing").glob(f"{feed}_*.csv"))
        for path in files:
            frame, bad = _incoming_file(spark, path, feed)
            malformed.extend(bad)
            if frame is not None:
                incoming.append(frame)
        batch = incoming[0] if incoming else None
        for other in incoming[1:]:
            batch = batch.unionByName(other, allowMissingColumns=True)
        combined, retry_duplicates = merge_raw(spark, batch, bronze, [key, "revision", "_payload_hash"])
        duplicates_suppressed += retry_duplicates
        if combined is None:
            freshness.append({"feed": feed, "status": freshness_status(len(files), None, datetime.now(UTC)),
                              "files": len(files), "rows": 0, "latest_update": None})
            fields = [T.StructField(key, T.StringType()), T.StructField("customer_id", T.StringType()),
                      T.StructField("period", T.StringType())]
            fields.extend([T.StructField("units", T.StringType())] if feed == "usage" else
                          [T.StructField("billed_units", T.StringType()), T.StructField("unit_price", T.StringType()),
                           T.StructField("currency", T.StringType())])
            frames[feed] = spark.createDataFrame([], T.StructType(fields))
            continue
        revision = F.expr("try_cast(revision AS BIGINT)")
        units = F.expr("try_cast(units AS BIGINT)") if feed == "usage" else F.expr("try_cast(billed_units AS BIGINT)")
        valid_time = F.try_to_timestamp("source_updated_at").isNotNull() & F.col("source_updated_at").rlike(r"(Z|[+-]\d{2}:\d{2})$")
        bad_reason = (F.when(F.col(key).isNull() | (F.trim(F.col(key)) == "") |
                             F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""), "MISSING_BUSINESS_KEY")
                      .when(revision.isNull() | (revision < 1), "INVALID_REVISION")
                      .when(F.col("period").isNull() | ~F.col("period").rlike(r"^\d{4}-(0[1-9]|1[0-2])$"), "INVALID_PERIOD")
                      .when(units.isNull() | (units < 0), "INVALID_UNITS")
                      .when(~valid_time, "INVALID_SOURCE_UPDATED_AT"))
        if feed == "invoices":
            price = F.expr("try_cast(unit_price AS DECIMAL(12,2))")
            bad_reason = (bad_reason.when(price.isNull() | (price < 0), "INVALID_UNIT_PRICE")
                          .when(F.col("currency").isNull() | (F.trim(F.col("currency")) == ""), "MISSING_CURRENCY"))
        assessed = combined.withColumn("_quarantine_reason", bad_reason)
        malformed.extend({"feed": feed, "source_file": row["_source_file"], "row_number": None,
                          "reason": row["_quarantine_reason"], "detail": row["_payload_hash"]}
                         for row in assessed.filter(F.col("_quarantine_reason").isNotNull())
                         .select("_source_file", "_payload_hash", "_quarantine_reason").collect())
        combined = assessed.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        maximum = combined.groupBy(key).agg(F.max(F.col("revision").cast("long")).alias("_max_revision"))
        candidate = combined.join(maximum, key).filter(F.col("revision").cast("long") == F.col("_max_revision"))
        conflicting = (candidate.groupBy(key, "revision")
            .agg(F.countDistinct("_payload_hash").alias("payload_count"))
            .filter(F.col("payload_count") > 1))
        if conflicting.limit(1).count():
            raise ValueError(f"Conflicting equal revision in {feed}; no deterministic winner is publishable")
        current = latest_revision(combined, [key], "revision", "source_updated_at", "_payload_hash")
        frames[feed] = current
        latest = current.agg(F.max("source_updated_at")).first()[0]
        file_count = len(files)
        status = freshness_status(file_count, datetime.fromisoformat(latest) if latest else None, datetime.now(UTC))
        freshness.append({"feed": feed, "status": status, "files": file_count, "rows": current.count(), "latest_update": latest})

    usage_totals = frames["usage"].groupBy("customer_id", "period").agg(
        F.sum(F.col("units").cast("long")).alias("observed_units"))
    invoices = frames["invoices"].select(
        "invoice_id", "customer_id", "period", F.col("billed_units").cast("long").alias("billed_units"),
        F.col("unit_price").cast("decimal(12,2)").alias("unit_price"), "currency")
    invoice_totals = invoices.groupBy("customer_id", "period").agg(
        F.sum("billed_units").alias("billed_units"),
        F.countDistinct("invoice_id").alias("invoice_count"),
        F.sort_array(F.collect_set("invoice_id")).alias("invoice_ids"),
        F.countDistinct("unit_price").alias("price_count"),
        F.countDistinct("currency").alias("currency_count"),
        F.first("unit_price").alias("_unit_price"),
        F.first("currency").alias("_currency"),
        F.sum((F.col("billed_units") * F.col("unit_price")).cast("decimal(14,2)"))
        .cast("decimal(16,2)").alias("billed_amount"))
    reconciliation = (invoice_totals.join(usage_totals, ["customer_id", "period"], "full_outer")
        .withColumn("unit_price", F.when((F.col("price_count") == 1) & (F.col("currency_count") == 1), F.col("_unit_price")))
        .withColumn("currency", F.when(F.col("currency_count") == 1, F.col("_currency")))
        .withColumn("billed_amount", F.when(F.col("currency_count") == 1, F.col("billed_amount")))
        .withColumn("unit_difference", F.col("billed_units") - F.col("observed_units"))
        .withColumn("expected_amount", (F.col("observed_units") * F.col("unit_price")).cast("decimal(14,2)"))
        .withColumn("exception", F.when(F.col("billed_units").isNull(), "MISSING_INVOICE")
                    .when(F.col("observed_units").isNull(), "MISSING_USAGE")
                    .when(F.col("currency_count") > 1, "MIXED_CURRENCY")
                    .when(F.col("price_count") > 1, "MULTIPLE_UNIT_PRICES")
                    .when(F.col("unit_difference") != 0, "BILLED_USAGE_MISMATCH"))
        .drop("_unit_price", "_currency"))
    if start_period is not None:
        old_path = root / "tables" / "gold" / "generations" / last_good / "usage_invoice_reconciliation"
        previous = read_delta(spark, old_path)
        unchanged = previous.filter((F.col("period") < start_period) | (F.col("period") > end_period))
        refreshed = reconciliation.filter(F.col("period").between(start_period, end_period))
        reconciliation = unchanged.unionByName(refreshed, allowMissingColumns=True)
    quarantine_schema = T.StructType([
        T.StructField("feed", T.StringType()), T.StructField("source_file", T.StringType()),
        T.StructField("row_number", T.IntegerType()), T.StructField("reason", T.StringType()),
        T.StructField("detail", T.StringType()),
    ])
    quarantine = spark.createDataFrame([tuple(row.get(field.name) for field in quarantine_schema)
                                         for row in malformed], quarantine_schema)
    result = [row.asDict(recursive=True) for row in reconciliation.collect()]
    generation = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ") + "-" + run_id[:8]
    output = root / "tables" / "gold" / "generations" / generation
    output.mkdir(parents=True, exist_ok=True)
    write_delta(reconciliation, output / "usage_invoice_reconciliation")
    write_delta(quarantine, output / "input_quarantine")
    pointer_path = root / "tables" / "gold" / "current_generation.json"
    published = not fail_before_publish
    if published:
        publish_generation(pointer_path, generation, run_id)
    report = {"synthetic": True, "project": PROJECT_NAME, "description": PROJECT_DESCRIPTION, "domain": "saas-usage-billing", "scenario": "saas-usage-to-invoice", "run_id": run_id, "status": "SUCCESS" if published else "FAILED_INJECTED", "published": published, "generation": generation if published else None, "staged_generation": generation, "last_good_generation": last_good, "publication_pointer": pointer_path.relative_to(root).as_posix(), "duplicate_business_events_suppressed": duplicates_suppressed, "source_freshness": freshness, "quarantine_rows": len(malformed), "quarantine_reasons": sorted({row["reason"] for row in malformed}), "start_period": start_period, "end_period": end_period, "repairs_run_id": repair_run_id, "started_at_utc": started, "reconciliation": result}
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
