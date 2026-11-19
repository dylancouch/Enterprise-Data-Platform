# Project status

Enterprise Data Platform is a synthetic Spark/Delta demonstration with two local examples and a bounded hosted construction run. Construction purchasing, delivery, and invoices produce accepted-quantity and invoice-exception reports. SaaS usage and billing produce a usage-to-invoice reconciliation. Both use shared replay, revision, freshness, and complete-generation publication mechanics in `data_pipelines/spark_delta.py`.

## Reproduce locally

```bash
./scripts/bootstrap-local.sh
make demo
make saas-demo
make verify-reliability
make test
make lint
```

The runnable entrypoints are `data_pipelines/cli.py` (installed command `edp`) and `data_pipelines/databricks_entry.py` for the construction job. `make demo` writes under ignored `data/demo/`; `make saas-demo` creates a timestamped ignored folder under `data/saas-demo/`. Source truth and expected values stay outside landing inputs.

## Evidence

- [Construction run](../evidence/enterprise-construction-demo.json): 10.000000 accepted metric tonnes, USD 125.00 net invoice amount, zero invoice exceptions, six freshness rows, and a complete published generation. The [synthetic HTML report](../reports/synthetic-dashboard.html) shows the consumer pointer, source arrival and staleness, and comparison with the prior successful generation.
- [SaaS walkthrough](../evidence/enterprise-saas-walkthrough.json): 200 observed versus 190 billed units initially; replay suppressed; an interrupted correction retained the previous pointer; recovery reconciled 190 versus 190.
- [Recovery run](../evidence/archive/milestone3-local-recovery.json): an interrupted construction correction retained the last-good generation; restart published nine tonnes and one invoice exception.
- [Allocation experiment](../evidence/archive/milestone4-allocation-benchmark.json): reduced join input, but no established runtime improvement. Earlier milestone snapshots are retained in `evidence/archive/`.
- [Local reliability evidence](../evidence/local-reliability.json) and [synthetic operational report](../reports/synthetic-reliability.html): ten checks passed across both domains. Each shows unchanged replay, a revised source, a late expected feed, a staged failure that kept the last complete consumer generation, and successful recovery. Construction late receipt changes accepted quantity from 0 to 10 tonnes; SaaS late invoice changes MISSING_INVOICE to a billed-usage mismatch. Construction failed runs now save source and publication evidence.
- [Databricks construction acceptance](../evidence/databricks-construction-acceptance.json): the Free Edition `dev` bundle validated and deployed to the isolated `workspace` catalog. Hosted golden and replay runs preserved 10 tonnes, USD 125.00 net invoices, and zero exceptions; replay suppressed six duplicate envelopes. A ticket revision published nine tonnes and one quantity exception. A separate late receipt sequence changed zero accepted tonnes and a pending invoice to ten tonnes and no exception. An injected failure after staging one product left the previous complete generation as the consumer pointer; the recovery run published nine tonnes and one exception.
- [Databricks SaaS acceptance](../evidence/databricks-saas-acceptance.json): hosted validation quarantined one malformed usage row while aggregating two invoices to 200 billed units. Period backfill corrected October observed usage to 190 and preserved September at five units.
- [Databricks construction backfill acceptance](../evidence/databricks-construction-backfill.json): an injected date-range failure kept the retained baseline generation active; the linked repair published nine tonnes for October 20 while preserving ten tonnes for October 21.
- [Scheduled repair evidence](../evidence/databricks-repair-schedules.json): construction and SaaS repair sweeps fired with `PERIODIC` triggers, repaired seeded failures, and remain active hourly for separate synthetic roots.
- [Milestone 6 evaluation](MILESTONE6_EVALUATION.md): project-path lineage and current grants were inspected, object-store pointer limits were assessed, and an 800,000-allocation local workload was measured.

On 2026-11-17, the current `data_pipelines` layout passed the full local suite: `SPARK_LOCAL_DIRS="$PWD/.cache/spark-tmp" make test` reported 20 passed in 1075.08 seconds. `SPARK_LOCAL_DIRS="$PWD/.cache/spark-tmp" make verify-reliability` passed all ten Spark/Delta checks across both examples and refreshed the linked evidence and report. `make lint` passed. The environment was rebuilt with `./scripts/bootstrap-local.sh` and used Python 3.15.2, Temurin Java 17.0.20.1, PySpark 4.0.1, Delta Spark 4.0.0, pytest 8.4.2, and Ruff 0.17.5. No local failures required code changes. The local and hosted results are recorded separately. The earlier [hosted CI run](https://github.com/dylancouch/enterprise-data-platform/actions/runs/41766154888) covered the previous package layout.

## Limits and next task

On 2026-11-17, Databricks CLI v1.21.0 authenticated to the Free Edition workspace through the ignored project-local `.home/.databrickscfg` profile `enterprise-data-dev`. The `dev` target validated and deployed a project schema, managed volume, and serverless construction job in the isolated `workspace` catalog. The first hosted attempts exposed two configuration defects: serverless script execution did not define `__file__`, and development mode prefixed the schema name while the job used the unprefixed path. The entrypoint now receives the bundle file path, and the job receives the deployed volume resource path. Subsequent hosted runs passed the bounded cases in the linked evidence. The CLI read the consumer pointer from the volume after success and after the injected failure; prior generation directories remained present. No unrelated resources were changed by the deployment.

These construction checks establish observed behavior in one Free Edition workspace. Hosted SaaS and backfill checks are documented separately below. Path-level lineage and current grants have now been inspected, but access roles are not configured or tested. The results do not establish arbitrary object-store pointer behavior or production-scale throughput. The full local test suite was not repeated in Databricks. There is no live source connection or production data. Local milestones 1 and 2 and bounded Databricks construction milestone 3 are complete.

## Current local implementation work

Milestone 4 now has chosen SaaS rules and code for malformed-row quarantine, valid-row continuation, multiple invoices at customer-period grain, latest revisions, and refusal to publish conflicting latest revisions. The rules and exceptions are in [the SaaS contract](SAAS_CONTRACT.md). Milestone 5 now has inclusive local backfill ranges, retained generations, per-root JSONL run ledgers, explicit repair links, and a `repair-pending` command that selects the newest unresolved failure. SaaS ranges apply to billing periods; construction ranges apply to daily-delivery dates, while construction invoice and source-freshness products recompute globally. Local runs now hold an exclusive per-root process lock; hosted jobs use one-run-at-a-time settings and isolated roots.

Focused local checks passed for SaaS malformed and multiple-invoice cases, invoice conflicts, pricing ambiguity, SaaS period repair, and construction daily-date repair; see [sanitized acceptance evidence](../evidence/local-saas-backfill-acceptance.json). The eight focused SaaS/ledger checks passed, as did the focused construction backfill check and the ten-check `make verify-reliability` gate. The gate refreshed [local reliability evidence](../evidence/local-reliability.json) and its HTML report. A final `SPARK_LOCAL_DIRS="$PWD/.cache/spark-tmp" make test` run passed all 27 tests in 1356.56 seconds on 2026-11-17. `make lint` passed. The earlier full run found an assertion that compared unordered SaaS rows as lists; the repaired and clean-root business values matched, and the corrected assertion now sorts by customer-period. Local milestones 4 and 5 are complete for the bounded cases in the roadmap.

## Hosted SaaS acceptance

On 2026-11-19, the deployed `saas_acceptance` serverless job passed in the isolated development volume. It verified malformed-row quarantine, multiple-invoice aggregation, and a 2026-10 period repair that preserved the unrelated 2026-09 result. Sanitized run IDs and values are in [hosted SaaS evidence](../evidence/databricks-saas-acceptance.json). Databricks volume append-mode file access failed in the first attempt; both domains now replace a complete small per-root JSONL ledger file after each entry, and the subsequent hosted run passed.

## Hosted construction backfill and evaluation

The `construction_backfill_acceptance` job passed against a retained two-date baseline. It staged a failed October 20 backfill without moving the consumer pointer, then linked a successful repair to that failed run. October 20 changed from ten to nine tonnes and the unrelated October 21 row stayed at ten tonnes. See [hosted backfill evidence](../evidence/databricks-construction-backfill.json). The [Milestone 6 evaluation](MILESTONE6_EVALUATION.md) records measured lineage, grant, object-store, and local allocation-scale findings and their limits.

## Writer coordination and scheduled repair

Local construction and SaaS runs now hold a per-root exclusive process lock through pointer publication and ledger write. A focused two-process check showed a second writer was rejected and could proceed after the first released the lock. The three focused ledger/lock checks and the construction and SaaS backfill repair checks passed locally; `make lint` passed. Both hosted repair jobs have `max_concurrent_runs: 1` and operate on separate fixed synthetic roots. Their hourly schedules are `UNPAUSED`; periodic triggers repaired seeded failures, with run IDs in [schedule evidence](../evidence/databricks-repair-schedules.json). A subsequent SaaS sweep returned `NO_PENDING_REPAIR`. The project does not coordinate independent Databricks jobs writing the same root. Local commands still need an external process scheduler for unattended execution.

## Milestone 7 — initial local CSV workflow (complete for the supported path)

A local browser interface is available in code as `edp app` / `make app`. It offers synthetic examples or user-provided CSVs, guided column mapping, required-field checks, a purchasing-to-delivery-to-invoice reconciliation, downloadable reports, and source filenames/content hashes on imported records. User-provided files are labeled as business data; bundled sample files stay labeled synthetic, including when uploaded through the business-data path. The optional browser dependency is installed with `pip install -e '.[app]'`.

The recommended fictional-company sample is available as export-shaped CSVs in [`examples/representative_business_data/exports`](../examples/representative_business_data/exports). It has multiple PO lines, accepted, pending, and rejected receipts, invoice discrepancies, a missing delivery allocation, a duplicate ticket row, an explicit corrected scale-ticket revision, and a ticket with an unsupported unit. Both the app's recommended-sample path and its CSV-upload path passed parsing, alias-based suggested mapping, validation/normalization, reconciliation, and report generation. Each published a synthetic report with 20.7 accepted metric tonnes, $409.00 net invoices, four expected invoice exceptions, one duplicate row ignored, and one `UNKNOWN_WEIGHT_UNIT` quarantined row; results match the separate expected outcomes. The app now explains exception reasons and suggested next steps, and reports duplicate and quarantined-row counts. The local browser server also returned HTTP 200 on its loopback address. See [workflow evidence](../evidence/business-sample-workflow.json), [business data structure research](BUSINESS_DATA_RESEARCH.md), and the [CSV import guide](CSV_IMPORT.md). No private company export was available or required. Direct connectors, Excel files, and automatic source refresh are not included in this first path.
