"""Command line interface for the synthetic construction and SaaS examples."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from data_pipelines.construction_generator import generate
from data_pipelines.construction_pipeline import run_pipeline
from data_pipelines.saas_pipeline import run as run_saas
from data_pipelines.saas_pipeline import walkthrough
from data_pipelines.spark_delta import create_spark, pending_failed_run


def main() -> None:
    parser = argparse.ArgumentParser(prog="edp", description="Enterprise Data Platform synthetic domain demos")
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="generate construction feeds and a separate expected fixture")
    gen.add_argument("--root", type=Path, default=Path("data/demo"))
    gen.add_argument("--scenario", choices=["golden", "ticket-correction", "duplicate-replay", "invalid-unit", "late-po", "late-receipt"], default="golden")
    gen.add_argument("--seed", type=int, default=1729)
    run = sub.add_parser("run", help="ingest and transform construction feeds with Spark and Delta")
    run.add_argument("--root", type=Path, default=Path("data/demo"))
    run.add_argument("--start-date")
    run.add_argument("--end-date")
    run.add_argument("--repair-run-id")
    demo = sub.add_parser("demo", help="generate and run a synthetic construction scenario")
    demo.add_argument("--root", type=Path, default=Path("data/demo"))
    demo.add_argument("--scenario", choices=["golden", "ticket-correction", "duplicate-replay", "invalid-unit", "late-po", "late-receipt"], default="golden")
    report = sub.add_parser("report", help="print the last published construction run report")
    report.add_argument("--root", type=Path, default=Path("data/demo"))
    saas = sub.add_parser("saas-demo", help="run the synthetic SaaS usage and billing recovery walkthrough")
    saas.add_argument("--root", type=Path, default=Path("data/saas-demo"))
    saas_run = sub.add_parser("saas-run", help="publish SaaS reconciliation or repair a period range")
    saas_run.add_argument("--root", type=Path, required=True)
    saas_run.add_argument("--start-period")
    saas_run.add_argument("--end-period")
    saas_run.add_argument("--repair-run-id")
    repair = sub.add_parser("repair-pending", help="repair the latest unresolved failed local run")
    repair.add_argument("--root", type=Path, required=True)
    repair.add_argument("--domain", choices=["construction", "saas"], required=True)
    args = parser.parse_args()
    if args.command == "saas-demo":
        print(json.dumps(walkthrough(args.root), indent=2, default=str))
        return
    if args.command == "repair-pending":
        failed = pending_failed_run(args.root, args.domain)
        if args.domain == "construction":
            result = run_pipeline(args.root, start_date=failed.get("start_date"),
                                  end_date=failed.get("end_date"), repair_run_id=failed["run_id"])
        else:
            spark = create_spark("saas-usage-billing-repair")
            try:
                result = run_saas(args.root, spark, start_period=failed.get("start_period"),
                                  end_period=failed.get("end_period"), repair_run_id=failed["run_id"])
            finally:
                spark.stop()
        print(json.dumps(result, indent=2, default=str))
        return
    if args.command == "saas-run":
        spark = create_spark("saas-usage-billing-run")
        try:
            result = run_saas(args.root, spark, start_period=args.start_period,
                              end_period=args.end_period, repair_run_id=args.repair_run_id)
        finally:
            spark.stop()
        print(json.dumps(result, indent=2, default=str))
        return
    if args.command == "report":
        pointer_path = args.root / "tables" / "gold" / "current_generation.json"
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        report_path = args.root / "evidence" / "runs" / f"{pointer['run_id']}.json"
        result = json.loads(report_path.read_text(encoding="utf-8"))
        result["report_generated_at_utc"] = datetime.now(UTC).isoformat()
        print(json.dumps(result, indent=2))
        return
    if args.command in {"generate", "demo"}:
        manifest = generate(args.root, args.scenario, getattr(args, "seed", 1729))
        print(json.dumps({"generated": True, "synthetic": True, "manifest": manifest}, indent=2))
    if args.command in {"run", "demo"}:
        print(json.dumps(run_pipeline(args.root, start_date=getattr(args, "start_date", None),
                                      end_date=getattr(args, "end_date", None),
                                      repair_run_id=getattr(args, "repair_run_id", None)), indent=2, default=str))


if __name__ == "__main__":
    main()
