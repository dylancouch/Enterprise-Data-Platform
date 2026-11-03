"""Run a synthetic interrupted-publication and restart demonstration."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from enterprise_data_platform.core import create_spark
from enterprise_data_platform.domains.construction.generator import generate
from enterprise_data_platform.domains.construction.pipeline import run_pipeline


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="edp-recovery-demo-") as temporary:
        root = Path(temporary) / "synthetic"
        generate(root, "golden")
        spark = create_spark("enterprise-data-platform-recovery-demo")
        try:
            initial = run_pipeline(root, spark)
            generate(root, "ticket-correction")
            interrupted = run_pipeline(root, spark, fail_after_product="daily_delivered_quantities")
            pointer = json.loads((root / "tables/gold/current_generation.json").read_text())
            stayed_on_last_good = pointer["generation"] == initial["generation"]
            recovered = run_pipeline(root, spark)
            pointer = json.loads((root / "tables/gold/current_generation.json").read_text())
            report = {
                "synthetic": True,
                "scenario": "ticket correction with interruption and restart",
                "initial_generation": initial["generation"],
                "initial_accepted_metric_tonnes": str(initial["daily_delivered"][0]["accepted_net_quantity"]),
                "interrupted_status": interrupted["status"],
                "interrupted_published": interrupted["published"],
                "last_good_pointer_retained_after_interruption": stayed_on_last_good,
                "recovered_generation": recovered["generation"],
                "recovered_accepted_metric_tonnes": str(recovered["daily_delivered"][0]["accepted_net_quantity"]),
                "recovered_invoice_exception_count": recovered["product_rows"]["invoice_exceptions"],
                "final_pointer_matches_recovery": pointer["generation"] == recovered["generation"],
            }
            assert report["interrupted_published"] is False
            assert stayed_on_last_good
            assert report["initial_accepted_metric_tonnes"] == "10.000000"
            assert report["recovered_accepted_metric_tonnes"] == "9.000000"
            assert report["final_pointer_matches_recovery"]
            print(json.dumps(report, indent=2))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
