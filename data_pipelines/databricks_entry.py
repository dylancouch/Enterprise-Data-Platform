"""Construction example Lakeflow Jobs entrypoint for synthetic files and Delta paths."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def _code_root() -> Path:
    if "--code-root" in sys.argv:
        option_index = sys.argv.index("--code-root")
        return Path(sys.argv[option_index + 1])
    return Path(__file__).resolve().parents[1]


sys.path.insert(0, str(_code_root()))

from data_pipelines.construction_generator import generate
from data_pipelines.construction_pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--code-root", type=Path)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--scenario", choices=("golden", "ticket-correction", "late-receipt", "invalid-ticket", "duplicate-replay"), default="golden")
    parser.add_argument("--fail-after-product", choices=("daily_delivered_quantities", "invoice_exceptions", "invoice_net_amounts", "source_freshness"))
    args = parser.parse_args()
    generate(args.root, args.scenario)
    # Serverless Jobs use Spark Connect; the workspace-provided session is used
    # directly and the local Spark/Delta builder is reserved for development.
    result = run_pipeline(
        args.root,
        spark=SparkSession.builder.getOrCreate(),
        fail_after_product=args.fail_after_product,
    )
    print(json.dumps(result, indent=2, default=str))
    if result["status"] != "SUCCESS":
        raise RuntimeError(f"Synthetic demo job did not publish successfully: {result['status']}")


if __name__ == "__main__":
    main()
