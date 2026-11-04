"""Construction example Lakeflow Jobs entrypoint for synthetic files and Delta paths."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from construction_generator import generate
from construction_pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--scenario", choices=("golden", "ticket-correction", "late-receipt", "invalid-ticket", "duplicate-replay"), default="golden")
    args = parser.parse_args()
    generate(args.root, args.scenario)
    # Serverless Jobs use Spark Connect; the workspace-provided session is used
    # directly and the local Spark/Delta builder is reserved for development.
    result = run_pipeline(args.root, spark=SparkSession.builder.getOrCreate())
    print(json.dumps(result, indent=2, default=str))
    if result["status"] != "SUCCESS":
        raise RuntimeError(f"Synthetic demo job did not publish successfully: {result['status']}")


if __name__ == "__main__":
    main()
