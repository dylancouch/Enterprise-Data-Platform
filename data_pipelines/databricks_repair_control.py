"""Synthetic hosted construction repair seed and scheduled sweep."""

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
    parser.add_argument("--mode", choices=("seed", "sweep"), required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.code_root))
    from data_pipelines.construction_generator import generate
    from data_pipelines.construction_pipeline import run_pipeline
    from data_pipelines.spark_delta import pending_failed_run

    spark = SparkSession.builder.getOrCreate()
    try:
        if args.mode == "seed":
            if (args.root / "tables" / "gold" / "current_generation.json").exists():
                raise RuntimeError("Synthetic operations root already has a baseline")
            generate(args.root, "golden")
            baseline = run_pipeline(args.root, spark)
            ticket_path = args.root / "landing" / "scale_tickets_batch_001.csv"
            with ticket_path.open(newline="", encoding="utf-8") as stream:
                ticket = next(csv.DictReader(stream))
            corrected = dict(ticket, source_revision="2", source_updated_at="2026-10-22T08:00:00Z",
                             gross_weight="11.000")
            with (args.root / "landing" / "scale_tickets_correction.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(corrected))
                writer.writeheader()
                writer.writerow(corrected)
            failed = run_pipeline(args.root, spark, fail_after_product="daily_delivered_quantities",
                                  start_date="2026-10-20", end_date="2026-10-20")
            pointer = json.loads((args.root / "tables" / "gold" / "current_generation.json").read_text(encoding="utf-8"))
            assert failed["status"] == "FAILED_INJECTED"
            assert pointer["generation"] == baseline["generation"]
            outcome = {"mode": "seed", "baseline_run_id": baseline["run_id"],
                       "failed_run_id": failed["run_id"], "pointer_retained": True}
        else:
            try:
                failed = pending_failed_run(args.root, "construction")
            except ValueError as exc:
                if str(exc) not in {"No local run ledger exists", "No unrepaired failed run exists"}:
                    raise
                outcome = {"mode": "sweep", "status": "NO_PENDING_REPAIR"}
            else:
                repaired = run_pipeline(args.root, spark, start_date=failed.get("start_date"),
                                        end_date=failed.get("end_date"), repair_run_id=failed["run_id"])
                outcome = {"mode": "sweep", "status": "REPAIRED", "failed_run_id": failed["run_id"],
                           "repair_run_id": repaired["run_id"], "generation": repaired["generation"],
                           "accepted_metric_tonnes": str(repaired["daily_delivered"][0]["accepted_net_quantity"])}
                assert outcome["accepted_metric_tonnes"] == "9.000000"
    finally:
        spark.stop()
    outcome.update({"synthetic": True, "verified_at_utc": datetime.now(UTC).isoformat(),
                    "root": str(args.root)})
    path = args.root / "evidence" / f"repair-control-{args.mode}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(outcome, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(outcome, indent=2))


if __name__ == "__main__":
    main()
