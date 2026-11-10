# Project status

Enterprise Data Platform is a synthetic local Spark/Delta demonstration with two examples. Construction purchasing, delivery, and invoices produce accepted-quantity and invoice-exception reports. SaaS usage and billing produce a usage-to-invoice reconciliation. Both use shared replay, revision, freshness, and complete-generation publication mechanics in `data_pipelines/spark_delta.py`.

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

Before the layout changes, local PySpark 4.0.1 and Delta Lake 4.0.0 checks passed 18 construction acceptance tests plus one focused SaaS conflict test. The [hosted run](https://github.com/dylancouch/enterprise-data-platform/actions/runs/41766154888) passed all 19 tests and the SaaS CLI smoke check for the earlier package layout. For the current data_pipelines layout, python3 scripts/verify-local-reliability.py (with the local Spark environment variables and cached dependencies) passed ten Spark/Delta assertions on both examples; two focused pytest cases for failed-run evidence and late SaaS invoices passed. Ruff, Python compilation, and git diff --check passed. These checks used Python 3.15 with locally cached PySpark 4.0.1 and Delta Lake 4.0.0 packages; the copied older virtual environment needs rebuilding before the documented make commands are used in this checkout. The full Spark suite has not been rerun for the current layout.

## Limits and next task

The Databricks bundle is configured for a bounded construction demo, but no authenticated workspace validation, deployment, or run has occurred. Local filesystem pointer behavior is verified; Databricks volume/object-store publication, native lineage, permissions, and production-scale behavior remain unverified. There is no live source connection or production data. Date-range backfill and automated repair/run-ledger tooling are not implemented. The local reliability roadmap is complete. The next platform task is an authenticated isolated Databricks run when access is available; SaaS malformed-input and multiple-invoice semantics remain future domain work.
