"""Command line interface for the synthetic construction and SaaS examples."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from data_pipelines.construction_generator import generate
from data_pipelines.construction_pipeline import run_pipeline
from data_pipelines.saas_pipeline import walkthrough


def main() -> None:
    parser = argparse.ArgumentParser(prog="edp", description="Enterprise Data Platform synthetic domain demos")
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="generate construction feeds and a separate expected fixture")
    gen.add_argument("--root", type=Path, default=Path("data/demo"))
    gen.add_argument("--scenario", choices=["golden", "ticket-correction", "duplicate-replay", "invalid-unit", "late-po", "late-receipt"], default="golden")
    gen.add_argument("--seed", type=int, default=1729)
    run = sub.add_parser("run", help="ingest and transform construction feeds with Spark and Delta")
    run.add_argument("--root", type=Path, default=Path("data/demo"))
    demo = sub.add_parser("demo", help="generate and run a synthetic construction scenario")
    demo.add_argument("--root", type=Path, default=Path("data/demo"))
    demo.add_argument("--scenario", choices=["golden", "ticket-correction", "duplicate-replay", "invalid-unit", "late-po", "late-receipt"], default="golden")
    report = sub.add_parser("report", help="print the last published construction run report")
    report.add_argument("--root", type=Path, default=Path("data/demo"))
    saas = sub.add_parser("saas-demo", help="run the synthetic SaaS usage and billing recovery walkthrough")
    saas.add_argument("--root", type=Path, default=Path("data/saas-demo"))
    args = parser.parse_args()
    if args.command == "saas-demo":
        print(json.dumps(walkthrough(args.root), indent=2, default=str))
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
        print(json.dumps(run_pipeline(args.root), indent=2, default=str))


if __name__ == "__main__":
    main()
