"""Synthetic hosted SaaS repair seed and scheduled sweep."""

from __future__ import annotations

import argparse
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
    from data_pipelines.saas_pipeline import _add_correction, _initial_inputs, run
    from data_pipelines.spark_delta import pending_failed_run

    spark = SparkSession.builder.getOrCreate()
    try:
        if args.mode == "seed":
            if (args.root / "tables" / "gold" / "current_generation.json").exists():
                raise RuntimeError("Synthetic operations root already has a baseline")
            _initial_inputs(args.root)
            baseline = run(args.root, spark)
            _add_correction(args.root)
            failed = run(args.root, spark, fail_before_publish=True,
                         start_period="2026-10", end_period="2026-10")
            pointer = json.loads((args.root / "tables" / "gold" / "current_generation.json").read_text(encoding="utf-8"))
            assert failed["status"] == "FAILED_INJECTED"
            assert pointer["generation"] == baseline["generation"]
            outcome = {"mode": "seed", "baseline_run_id": baseline["run_id"],
                       "failed_run_id": failed["run_id"], "pointer_retained": True}
        else:
            try:
                failed = pending_failed_run(args.root, "saas")
            except ValueError as exc:
                if str(exc) not in {"No local run ledger exists", "No unrepaired failed run exists"}:
                    raise
                outcome = {"mode": "sweep", "status": "NO_PENDING_REPAIR"}
            else:
                repaired = run(args.root, spark, start_period=failed.get("start_period"),
                               end_period=failed.get("end_period"), repair_run_id=failed["run_id"])
                row = repaired["reconciliation"][0]
                assert row["observed_units"] == 190 and row["exception"] is None
                outcome = {"mode": "sweep", "status": "REPAIRED", "failed_run_id": failed["run_id"],
                           "repair_run_id": repaired["run_id"], "generation": repaired["generation"],
                           "observed_units": row["observed_units"], "billed_units": row["billed_units"]}
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
