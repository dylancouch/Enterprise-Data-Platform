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

On 2026-11-17, the current `data_pipelines` layout passed the full local suite: `SPARK_LOCAL_DIRS="$PWD/.cache/spark-tmp" make test` reported 20 passed in 1075.08 seconds. `SPARK_LOCAL_DIRS="$PWD/.cache/spark-tmp" make verify-reliability` passed all ten Spark/Delta checks across both examples and refreshed the linked evidence and report. `make lint` passed. The environment was rebuilt with `./scripts/bootstrap-local.sh` and used Python 3.15.2, Temurin Java 17.0.20.1, PySpark 4.0.1, Delta Spark 4.0.0, pytest 8.4.2, and Ruff 0.17.5. No local failures required code changes. The local and hosted results are recorded separately. The earlier [hosted CI run](https://github.com/dylancouch/enterprise-data-platform/actions/runs/41766154888) covered the previous package layout.

## Limits and next task

On 2026-11-17, Databricks CLI v1.21.0 authenticated to the Free Edition workspace through the ignored project-local `.home/.databrickscfg` profile `enterprise-data-dev`. The `dev` target validated and deployed a project schema, managed volume, and serverless construction job in the isolated `workspace` catalog. The first hosted attempts exposed two configuration defects: serverless script execution did not define `__file__`, and development mode prefixed the schema name while the job used the unprefixed path. The entrypoint now receives the bundle file path, and the job receives the deployed volume resource path. Subsequent hosted runs passed the bounded cases in the linked evidence. The CLI read the consumer pointer from the volume after success and after the injected failure; prior generation directories remained present. No unrelated resources were changed by the deployment.

These hosted checks establish the observed behavior of this construction job and Unity Catalog volume in one Free Edition workspace. They do not establish behavior for arbitrary object stores, native lineage, access-control boundaries, or production-scale workloads. The full local test suite was not repeated in Databricks, and the SaaS example has not run there. There is no live source connection or production data. Local milestones 1 and 2 and the bounded Databricks milestone 3 are complete.

## Current local implementation work

Milestone 4 now has chosen SaaS rules and code for malformed-row quarantine, valid-row continuation, multiple invoices at customer-period grain, latest revisions, and refusal to publish conflicting latest revisions. The rules and exceptions are in [the SaaS contract](SAAS_CONTRACT.md). Milestone 5 now has inclusive local backfill ranges, retained generations, per-root JSONL run ledgers, explicit repair links, and a `repair-pending` command that selects the newest unresolved failure. SaaS ranges apply to billing periods; construction ranges apply to daily-delivery dates, while construction invoice and source-freshness products recompute globally. No background scheduler or concurrent writer lock is installed.

Focused local checks passed for SaaS malformed and multiple-invoice cases, invoice conflicts, pricing ambiguity, SaaS period repair, and construction daily-date repair; see [sanitized acceptance evidence](../evidence/local-saas-backfill-acceptance.json). The eight focused SaaS/ledger checks passed, as did the focused construction backfill check and the ten-check `make verify-reliability` gate. The gate refreshed [local reliability evidence](../evidence/local-reliability.json) and its HTML report. A final `SPARK_LOCAL_DIRS="$PWD/.cache/spark-tmp" make test` run passed all 27 tests in 1356.56 seconds on 2026-11-17. `make lint` passed. The earlier full run found an assertion that compared unordered SaaS rows as lists; the repaired and clean-root business values matched, and the corrected assertion now sorts by customer-period. Local milestones 4 and 5 are complete for the bounded cases in the roadmap. These additions have not run in Databricks.
