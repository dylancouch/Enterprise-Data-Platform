"""Verify both synthetic examples through replay, late/revised input, and recovery."""

from __future__ import annotations

import argparse
import html
import json
import shutil
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from data_pipelines import PROJECT_DESCRIPTION, PROJECT_NAME
from data_pipelines.construction_generator import generate
from data_pipelines.construction_pipeline import run_pipeline
from data_pipelines.saas_pipeline import _add_correction, _initial_inputs, run
from data_pipelines.spark_delta import create_spark, read_delta


def pointer(root: Path) -> str:
    return json.loads((root / "tables/gold/current_generation.json").read_text(encoding="utf-8"))["generation"]


def construction_quantity(report: dict) -> Decimal:
    return sum((Decimal(str(row["accepted_net_quantity"])) for row in report["daily_delivered"]), Decimal(0))


def source_states(report: dict) -> dict[str, str]:
    return {row.get("source_feed", row.get("feed")): row.get("freshness_status", row.get("status"))
            for row in report["source_freshness"]}


def step(report: dict, consumer_generation: str, measure: str, value: object) -> dict:
    if not report["published"]:
        decision = "Staged output withheld; previous complete generation remains current"
    elif "reconciliation" in report:
        row = report["reconciliation"][0]
        decision = f'Billed units: {row["billed_units"]}; exception: {row["exception"] or "none"}'
    else:
        decision = f'Invoice exceptions: {len(report["invoice_exceptions"])}'
    return {
        "decision": decision,
        "outcome": report["status"],
        "published": report["published"],
        "consumer_generation": consumer_generation,
        "staged_generation": report.get("staged_generation") or report["generation"],
        "measure": measure,
        "value": str(value),
        "replay_duplicates_suppressed": report.get(
            "duplicate_business_events_suppressed", report.get("duplicate_business_envelopes_suppressed", 0)),
        "sources": source_states(report),
    }


def verify_construction(root: Path, spark) -> dict:
    generate(root, "golden")
    initial = run_pipeline(root, spark)
    first_pointer = pointer(root)
    assert construction_quantity(initial) == Decimal(10)
    assert initial["product_rows"]["invoice_exceptions"] == 0

    replay = run_pipeline(root, spark)
    assert construction_quantity(replay) == construction_quantity(initial)
    assert replay["duplicate_business_envelopes_suppressed"] > 0
    assert read_delta(spark, root / "tables/gold/generations" / pointer(root) / "daily_delivered_quantities").count() == 1

    generate(root, "ticket-correction")
    failed = run_pipeline(root, spark, fail_after_product="daily_delivered_quantities")
    assert failed["published"] is False and pointer(root) == replay["generation"]
    failed_pointer = pointer(root)
    assert str(read_delta(spark, root / "tables/gold/generations" / failed_pointer / "daily_delivered_quantities")
               .first()["accepted_net_quantity"]) == "10.000000"

    recovered = run_pipeline(root, spark)
    assert pointer(root) == recovered["generation"]
    assert construction_quantity(recovered) == Decimal(9)
    assert recovered["product_rows"]["invoice_exceptions"] == 1
    assert str(read_delta(spark, root / "tables/gold/generations" / first_pointer / "daily_delivered_quantities")
               .first()["accepted_net_quantity"]) == "10.000000"

    late_root = root.parent / "construction-late"
    generate(late_root, "late-receipt")
    pending = run_pipeline(late_root, spark)
    assert construction_quantity(pending) == 0
    assert source_states(pending)["site_receipts"] == "MISSING_EXPECTED_FILE"
    shutil.move(str(late_root / "deferred/site_receipts_late_arrival.csv"),
                str(late_root / "landing/site_receipts_late_arrival.csv"))
    arrived = run_pipeline(late_root, spark)
    assert construction_quantity(arrived) == Decimal(10)
    assert source_states(arrived)["site_receipts"] != "MISSING_EXPECTED_FILE"
    assert pointer(late_root) == arrived["generation"]

    return {
        "domain": "construction",
        "checks": {
            "replay_does_not_duplicate_results": True,
            "revision_replaces_quantity": True,
            "late_receipt_updates_historical_result": True,
            "failed_stage_keeps_complete_consumer_generation": True,
            "recovery_publishes_complete_generation": True,
        },
        "timeline": {
            "initial": step(initial, first_pointer, "accepted metric tonnes", construction_quantity(initial)),
            "replay": step(replay, replay["generation"], "accepted metric tonnes", construction_quantity(replay)),
            "interrupted_revision": step(failed, failed_pointer, "consumer accepted metric tonnes", "10.000000"),
            "recovered_revision": step(recovered, recovered["generation"], "accepted metric tonnes", construction_quantity(recovered)),
            "late_receipt_pending": step(pending, pending["generation"], "accepted metric tonnes", construction_quantity(pending)),
            "late_receipt_arrived": step(arrived, arrived["generation"], "accepted metric tonnes", construction_quantity(arrived)),
        },
    }


def observed(report: dict) -> int:
    return sum(row["observed_units"] or 0 for row in report["reconciliation"])


def verify_saas(root: Path, spark) -> dict:
    _initial_inputs(root)
    initial = run(root, spark)
    assert observed(initial) == 200
    assert initial["reconciliation"][0]["exception"] == "BILLED_USAGE_MISMATCH"
    first_pointer = pointer(root)

    shutil.copy2(root / "landing/usage_batch_001.csv", root / "landing/usage_retry_renamed.csv")
    replay = run(root, spark)
    assert observed(replay) == 200
    assert replay["duplicate_business_events_suppressed"] > 0
    assert read_delta(spark, root / "tables/gold/generations" / pointer(root) / "usage_invoice_reconciliation").count() == 1

    _add_correction(root)
    failed = run(root, spark, fail_before_publish=True)
    assert failed["published"] is False and pointer(root) == replay["generation"]
    failed_pointer = pointer(root)
    assert read_delta(spark, root / "tables/gold/generations" / failed_pointer /
                      "usage_invoice_reconciliation").first()["observed_units"] == 200

    recovered = run(root, spark)
    assert pointer(root) == recovered["generation"]
    assert observed(recovered) == 190
    assert recovered["reconciliation"][0]["exception"] is None
    assert read_delta(spark, root / "tables/gold/generations" / first_pointer /
                      "usage_invoice_reconciliation").first()["observed_units"] == 200

    late_root = root.parent / "saas-late"
    _initial_inputs(late_root)
    deferred = late_root / "deferred"
    deferred.mkdir()
    shutil.move(str(late_root / "landing/invoices_batch_001.csv"),
                str(deferred / "invoices_batch_001.csv"))
    pending = run(late_root, spark)
    assert observed(pending) == 200
    assert pending["reconciliation"][0]["exception"] == "MISSING_INVOICE"
    assert source_states(pending)["invoices"] == "MISSING_EXPECTED_FILE"
    shutil.move(str(deferred / "invoices_batch_001.csv"),
                str(late_root / "landing/invoices_batch_001.csv"))
    arrived = run(late_root, spark)
    assert arrived["reconciliation"][0]["billed_units"] == 190
    assert arrived["reconciliation"][0]["exception"] == "BILLED_USAGE_MISMATCH"
    assert source_states(arrived)["invoices"] != "MISSING_EXPECTED_FILE"
    assert pointer(late_root) == arrived["generation"]

    return {
        "domain": "saas-usage-billing",
        "checks": {
            "replay_does_not_duplicate_results": True,
            "revision_replaces_usage": True,
            "late_invoice_resolves_missing_feed": True,
            "failed_stage_keeps_complete_consumer_generation": True,
            "recovery_publishes_complete_generation": True,
        },
        "timeline": {
            "initial": step(initial, first_pointer, "observed units", observed(initial)),
            "replay": step(replay, replay["generation"], "observed units", observed(replay)),
            "interrupted_revision": step(failed, failed_pointer, "consumer observed units", 200),
            "recovered_revision": step(recovered, recovered["generation"], "observed units", observed(recovered)),
            "late_invoice_pending": step(pending, pending["generation"], "observed units", observed(pending)),
            "late_invoice_arrived": step(arrived, arrived["generation"], "observed units", observed(arrived)),
        },
    }


def render(evidence: dict) -> str:
    sections = []
    for domain in evidence["domains"]:
        rows = []
        for name, item in domain["timeline"].items():
            sources = ", ".join(f"{feed}: {state}" for feed, state in item["sources"].items())
            cells = [name.replace("_", " "), item["outcome"], "yes" if item["published"] else "no",
                     item["consumer_generation"], f'{item["measure"]}: {item["value"]}',
                     item["decision"], str(item["replay_duplicates_suppressed"]), sources]
            rows.append("<tr>" + "".join(f"<td>{html.escape(str(cell))}</td>" for cell in cells) + "</tr>")
        checks = ", ".join(k.replace("_", " ") for k, passed in domain["checks"].items() if passed)
        sections.append(
            f'<section><h2>{html.escape(domain["domain"])}</h2><p>Verified: {html.escape(checks)}.</p>'
            '<div class="scroll"><table><thead><tr><th>Step</th><th>Run</th><th>Published</th>'
            '<th>Consumer generation</th><th>Business result</th><th>Decision state</th><th>Replay duplicates suppressed</th>'
            '<th>Source state</th></tr></thead><tbody>' + "".join(rows) + "</tbody></table></div></section>")
    return f'''<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{PROJECT_NAME} — synthetic local reliability report</title>
<style>body{{font:16px/1.45 system-ui,sans-serif;background:#f4f6f8;color:#17212b;margin:0}}
main{{max-width:1200px;margin:2rem auto;padding:0 1rem}}header,section{{background:white;border:1px solid #dce2e8;
border-radius:10px;padding:1rem 1.25rem;margin-bottom:1rem}}.banner{{background:#fff0df;color:#753500;font-weight:700}}
.scroll{{overflow-x:auto}}table{{border-collapse:collapse;width:100%}}th,td{{padding:.6rem;text-align:left;
vertical-align:top;border-bottom:1px solid #e4e8ed}}th{{background:#eef2f5}}td:nth-child(4){{font-family:monospace;
overflow-wrap:anywhere}}small{{color:#4a5968}}</style><main>
<header class="banner">SYNTHETIC DEMONSTRATION DATA — local Spark and Delta only</header>
<header><h1>{PROJECT_NAME}: local reliability</h1><p>{PROJECT_DESCRIPTION}</p>
<p>Both examples passed replay, revision, late arrival, interrupted publication, and recovery checks.
Consumers read only the generation named by <code>tables/gold/current_generation.json</code>.</p>
<small>Verified {html.escape(evidence["verified_at_utc"])}. This report describes local filesystem behavior.
Databricks workspace behavior remains unverified.</small></header>
{"".join(sections)}
</main></html>'''


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, default=Path("evidence/local-reliability.json"))
    parser.add_argument("--report", type=Path, default=Path("reports/synthetic-reliability.html"))
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="edp-local-reliability-") as temporary:
        root = Path(temporary)
        spark = create_spark("enterprise-data-platform-local-reliability")
        try:
            domains = [verify_construction(root / "construction", spark),
                       verify_saas(root / "saas", spark)]
        finally:
            spark.stop()
    evidence = {
        "synthetic": True,
        "project": PROJECT_NAME,
        "description": PROJECT_DESCRIPTION,
        "verification": "local Spark/Delta",
        "verified_at_utc": datetime.now(UTC).isoformat(),
        "domains": domains,
    }
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.evidence.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    args.report.write_text(render(evidence), encoding="utf-8")
    print(json.dumps({"evidence": str(args.evidence), "report": str(args.report),
                      "checks_passed": sum(sum(d["checks"].values()) for d in domains)}, indent=2))


if __name__ == "__main__":
    main()
