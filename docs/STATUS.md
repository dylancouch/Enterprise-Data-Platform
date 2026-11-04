# Project status

Enterprise Data Platform is a synthetic local Spark/Delta demonstration with two examples. Construction purchasing, delivery, and invoices produce accepted-quantity and invoice-exception reports. SaaS usage and billing produce a usage-to-invoice reconciliation. Both use shared replay, revision, freshness, and complete-generation publication mechanics in `spark_delta.py`.

## Reproduce locally

```bash
./scripts/bootstrap-local.sh
make demo
make saas-demo
make test
make lint
```

The runnable entrypoints are `cli.py` (installed command `edp`) and `databricks_entry.py` for the construction job. `make demo` writes under ignored `data/demo/`; `make saas-demo` creates a timestamped ignored folder under `data/saas-demo/`. Source truth and expected values stay outside landing inputs.

## Evidence

- [Construction run](../evidence/enterprise-construction-demo.json): 10.000000 accepted metric tonnes, USD 125.00 net invoice amount, zero invoice exceptions, six freshness rows, and a complete published generation. The [synthetic HTML report](../reports/synthetic-dashboard.html) shows the consumer pointer, source arrival and staleness, and comparison with the prior successful generation.
- [SaaS walkthrough](../evidence/enterprise-saas-walkthrough.json): 200 observed versus 190 billed units initially; replay suppressed; an interrupted correction retained the previous pointer; recovery reconciled 190 versus 190.
- [Recovery run](../evidence/archive/milestone3-local-recovery.json): an interrupted construction correction retained the last-good generation; restart published nine tonnes and one invoice exception.
- [Allocation experiment](../evidence/archive/milestone4-allocation-benchmark.json): reduced join input, but no established runtime improvement. Earlier milestone snapshots are retained in `evidence/archive/`.

Before the current file-layout cleanup, local PySpark 4.0.1 and Delta Lake 4.0.0 checks passed 18 construction acceptance tests plus one focused SaaS conflict test. The [hosted run](https://github.com/dylancouch/enterprise-data-platform/actions/runs/41766154888) passed all 19 tests and the SaaS CLI smoke check for the earlier package layout. For the current flat layout, the editable install, Ruff, Python compilation, installed CLI help, Databricks entrypoint help, entrypoint metadata, bundle path assertion, and `git diff --check` passed. The Spark test suite has not been rerun for this layout change.

## Limits and next task

The Databricks bundle is configured for a bounded construction demo, but no authenticated workspace validation, deployment, or run has occurred. Local filesystem pointer behavior is verified; Databricks volume/object-store publication, native lineage, permissions, and production-scale behavior remain unverified. There is no live source connection or production data. Date-range backfill and automated repair/run-ledger tooling are not implemented. The next platform task is an authenticated isolated Databricks run when access is available; SaaS malformed-input and multiple-invoice semantics remain future domain work.
