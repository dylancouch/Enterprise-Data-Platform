# Enterprise Data Platform

A Spark and Delta data platform for integrating operational sources, publishing trustworthy analytics, and monitoring pipeline health across industries.

The local browser app lets a business user import purchasing, delivery, receipt, and invoice CSV exports and receive a reconciliation report. Users without exports can choose realistic synthetic data through the same app. The project also includes SaaS and construction data-processing examples, reliability handling, and bounded Databricks runs. Business files and reports stay on the computer running the local app; no live accounting, procurement, or ERP connector is included yet. See the [CSV import guide](docs/CSV_IMPORT.md) for supported data and limits.

The shared Spark/Delta mechanics are in `data_pipelines/spark_delta.py`. `data_pipelines/construction_pipeline.py` and `data_pipelines/saas_pipeline.py` contain the example-specific logic; `data_pipelines/construction_generator.py` creates synthetic construction inputs. Run them through `data_pipelines/cli.py`. See [the shared contract](docs/PLATFORM_CONTRACT.md), [construction contracts](docs/SOURCE_CONTRACTS.md), [SaaS contract](docs/SAAS_CONTRACT.md), and [the extension guide](docs/EXTENDING.md).

| Synthetic example | Sources and decision product | Operational behavior shown |
| --- | --- | --- |
| Construction purchasing and delivery | Purchase orders, tickets, receipts, invoices, and allocations → accepted quantities and invoice exceptions | Revisions, late links, quarantine, freshness, replay, and complete publication |
| SaaS usage and billing | Metered usage and invoices → billed-versus-observed units | Stale input, malformed-row quarantine, multiple invoices, renamed replay, correction, conflict refusal, and recovery after an interrupted publication |

## Run the local examples

Requires Python 3.11 or newer, Java 17, and network access to Maven Central the first time Delta dependencies are resolved. In this environment the Java 17 runtime is kept in `.tools/java` and Python dependencies in `.venv`.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[app,dev]'
# If Java is not installed on the host, provision a project-scoped Java 17 runtime as described in docs/STATUS.md.
make app
```

The browser app runs on this computer. Choose the recommended synthetic company example or import your own purchasing, delivery, receipt, and invoice CSV exports. The example uses multiple export-shaped files with familiar ERP-style headers and includes both matching and problem records. The app previews files, suggests column mappings, checks required values, and creates a reconciliation report with invoice exceptions. Imported data and results are saved locally under `data/user-runs/`. See the [CSV import guide](docs/CSV_IMPORT.md) for the sample files, accepted data, and current limits. Start the lower-level command line examples with `.venv/bin/edp demo --root data/demo` or `.venv/bin/edp saas-demo --root data/saas-demo`.

Run `make verify-reliability` for a credential-free local Spark/Delta check of replay, revisions, late arrivals, interrupted publication, and recovery in both examples. It writes [synthetic reliability evidence](evidence/local-reliability.json) and a [synthetic operational report](reports/synthetic-reliability.html).

`make demo` runs the construction reference example. It creates files under `data/demo/landing/`; the independent oracle and scenario manifest are stored separately under `data/demo/expected/` and are never pipeline inputs. Spark writes raw, validated and reporting Delta data under `data/demo/tables/`. Consumers resolve all reporting products through `tables/gold/current_generation.json`. The latest sanitized run is committed at [evidence/enterprise-construction-demo.json](evidence/enterprise-construction-demo.json).

Other commands:

```bash
.venv/bin/python -m data_pipelines.cli generate --root data/correction --scenario ticket-correction
.venv/bin/python -m data_pipelines.cli run --root data/correction
.venv/bin/python -m data_pipelines.cli generate --root data/late-receipt --scenario late-receipt
.venv/bin/python -m data_pipelines.cli run --root data/late-receipt
mv data/late-receipt/deferred/site_receipts_late_arrival.csv data/late-receipt/landing/
.venv/bin/python -m data_pipelines.cli run --root data/late-receipt
make test
make lint
```

See [the roadmap](docs/ROADMAP.md) and [project status](docs/STATUS.md) for planned work, executed checks, outcomes, and limitations.

## Databricks construction demo

The `dev` bundle target uses the isolated `workspace` catalog available in the tested Free Edition workspace. With the Databricks CLI installed and the project-local profile configured, run:

```bash
export DATABRICKS_CONFIG_FILE="$PWD/.home/.databrickscfg"
databricks auth login --host <workspace-url> --profile enterprise-data-dev
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run construction_demo -t dev
```

The job writes synthetic files and Delta products to its managed Unity Catalog volume. Development mode prefixes the schema name; the job resolves the deployed volume path from the bundle resource. The [hosted evidence](evidence/databricks-construction-acceptance.json) covers golden output, replay, ticket revision, late receipt, interrupted publication, and recovery. Other workspace behavior remains subject to the limits in [project status](docs/STATUS.md).

The bundle also includes `saas_acceptance`, `construction_backfill_acceptance`, and separate scheduled synthetic repair sweeps for construction and SaaS. Their bounded results and current limits are in [Milestone 6 evaluation](docs/MILESTONE6_EVALUATION.md), [schedule evidence](evidence/databricks-repair-schedules.json), and [project status](docs/STATUS.md).

## Reports and recovery

The latest synthetic run report is available with `.venv/bin/python -m data_pipelines.cli report --root data/demo`. Export a self-contained snapshot of the current published run with `.venv/bin/python scripts/render-report.py --root data/demo`; the report leads with run outcome, the consumer publication pointer, arrived/missing/stale sources, and changes from the prior successful generation. See [`reports/synthetic-dashboard.html`](reports/synthetic-dashboard.html). Run `.venv/bin/python scripts/demo-recovery.py` (or `make recovery-demo`) to demonstrate a ticket correction, interruption between product writes, retention of the previous publication, and successful restart.

## SaaS usage and billing example

Run `make saas-demo` to reconcile metered software usage against a customer invoice. It demonstrates a stale feed and invoice mismatch, a renamed-file replay with duplicate suppression, a corrected event revision, and an interrupted publication that leaves the last-good generation active. The final correction restores agreement. Each command writes to a new timestamped folder beneath `data/saas-demo/`; its `evidence/walkthrough.json` records the steps and generations. A sanitized result is committed at [evidence/enterprise-saas-walkthrough.json](evidence/enterprise-saas-walkthrough.json). Data is fictional and outputs are not production scale.

For an existing local root, `edp saas-run --root <root> --start-period 2026-10 --end-period 2026-10` rebuilds one or more billing periods while preserving unrelated periods. `edp run --root <root> --start-date 2026-10-20 --end-date 2026-10-20` rebuilds construction daily-delivery dates. Both require a baseline generation and write `evidence/run_ledger.jsonl`. A failed run can be linked to a repair with `--repair-run-id <id>` and the same range, or `edp repair-pending --domain saas --root <root>` can select the newest unresolved failure automatically (`--domain construction` for construction). Construction invoice and freshness products are recomputed globally during a daily-date backfill. See the [local acceptance evidence](evidence/local-saas-backfill-acceptance.json), [contracts](docs/PLATFORM_CONTRACT.md), and [runbook](docs/RUNBOOK.md) for details.

See the [recovery runbook](docs/RUNBOOK.md), [governance limits](docs/GOVERNANCE.md), [architecture](docs/ARCHITECTURE.md), and [design decisions](docs/DESIGN_DECISIONS.md).

Reproduce the local allocation experiment with `BENCHMARK_ARGS='--allocations-per-delivery 16' make benchmark`; its sanitized output is in [`evidence/archive/milestone4-allocation-benchmark.json`](evidence/archive/milestone4-allocation-benchmark.json). It records an input-reduction result but does not demonstrate a meaningful runtime gain.
