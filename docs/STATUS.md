# Project status

## Milestone 1 — local baseline complete

The deterministic generator writes six independently versioned synthetic source feeds and keeps deferred late-arriving files outside the pipeline landing area. The Spark/Delta pipeline preserves source provenance, deduplicates business envelopes across replay and renamed files, validates and quarantines invalid ticket/receipt/invoice/allocation records, resolves effective purchase order revisions, and writes raw, validated, quarantine, and reporting Delta data. Reporting includes daily accepted delivered quantities, invoice reconciliation exceptions, and a run report. A generation pointer publishes a complete local output only after product writes succeed; failed generations remain unpublished and can be retried. The independent oracle is a test artifact under `fixtures/`, never a pipeline input.

The latest local synthetic demo run is recorded in [`evidence/milestone1-local-demo.json`](../evidence/milestone1-local-demo.json). It ingested six feeds × one source row and published **P-001 / AGG-1 / 2026-10-20 = 10.000000 metric tonnes**, with **USD 125.00** reconciled to the effective purchase-order price, **zero invoice exceptions**, and **zero quarantine rows**. Replay suppressed six duplicate business envelopes. The generator, demo output, and report are explicitly labeled synthetic.

## Checks actually run

- Before the Enterprise Data Platform refactor, `make test` — **18 passed in 976.03 seconds** using local PySpark 4.0.1 and Delta Lake 4.0.0. Current-package verification is recorded below.
- `make demo` — completed successfully again after Milestone 3 changes using real local Spark and Delta. The newest synthetic generation reports 10.000000 tonnes, USD 125.00 net invoice amount, zero invoice exceptions, and six source freshness rows; the PO update is flagged stale under the 72-hour demo threshold.
- `.venv/bin/ruff check src tests scripts` — passed.
- `.venv/bin/python -m compileall -q src tests scripts` — passed.
- Parsed `databricks.yml` as YAML and asserted bounded concurrency, timeout, retry, serverless environment, scenario parameter wiring, dev scenario default, and entrypoint path — passed.
- `.venv/bin/python -m enterprise_data_platform.databricks_entry --help` — passed; confirmed the workspace entrypoint exposes the configured scenario choices.
- Before the rename, `.tools/databricks/databricks bundle validate -t dev` read the configuration, then stopped because the old `construction-data-dev` profile had no authentication. No remote validation, deployment, or job run occurred. The installed CLI can emit the official bundle schema, but full validation is workspace-aware and requires authentication.
- `make recovery-demo` — passed; interrupted correction retained the prior pointer, restart published 9 tonnes and one invoice exception. Sanitized output: `evidence/milestone3-local-recovery.json`.
- `BENCHMARK_ARGS='--allocations-per-delivery 16' make benchmark` — passed output conservation checks and emitted `evidence/milestone4-allocation-benchmark.json` with row counts, bytes, configuration, and timings.
- `.venv/bin/python -m enterprise_data_platform.cli report --root data/demo` plus `scripts/render-report.py` — passed; generated the actual synthetic snapshot at `reports/synthetic-dashboard.html` and saved its run report at `evidence/milestone4-dashboard-run.json`.
- `git diff --check` — passed.

The 18 Spark acceptance tests cover clean oracle reconciliation; same-name and renamed-file replay; interrupted publication and restart; corrections and out-of-order revisions; invalid-row quarantine and higher-revision repair; additive fields and unsupported schema versions; late PO/receipt arrival; missing/stale source status; ticket cancellation; linked and unlinked credits; amount/price discrepancies; unit normalization; PO effective pricing; allocation conservation; tolerance boundaries; and invalid timestamps/units.

## Environment and reproduction

The observed execution host is an x86_64 Fedora Linux 44 container with 32 CPUs and 31 GiB available memory; Python is 3.14.8. The project uses isolated `.venv` dependencies, project-scoped Temurin Java 17.0.20.1 at `.tools/java`, PySpark 4.0.1, Delta Lake 4.0.0, and Databricks CLI 1.21.0. The demo/tests were executed locally against Spark/Delta, not in a Databricks workspace. No Databricks CLI profile or workspace is configured.

```bash
./scripts/bootstrap-local.sh   # project-scoped Java 17 and Python dependencies
make test                      # local Spark + Delta acceptance suite
make demo                      # rebuild data/demo and publish synthetic golden output
.venv/bin/ruff check src tests scripts
.venv/bin/python -m compileall -q src tests scripts
make recovery-demo
```

The recovery scenario is saved in [`evidence/milestone3-local-recovery.json`](../evidence/milestone3-local-recovery.json). It retained the 10-tonne last-good generation after an interrupted correction run, then published 9 tonnes and one invoice exception after restart. The first Spark start may resolve the Delta Lake JVM package through the existing Maven configuration and cache it under ignored project state. `make demo` prints the synthetic manifest and sanitized run report. The independent oracle can be compared with published values in the acceptance tests; it is not loaded by the pipeline.

## Databricks readiness and blocker

Milestone 2 local deployment preparation is in progress. The bundle declares a project-owned schema and managed volume and a bounded serverless Lakeflow Job (one concurrent run, one task retry, 3600-second timeout). Its serverless environment uses version 6 and its task takes an explicit synthetic scenario and Unity Catalog volume root. The entrypoint uses the workspace-provided Spark session. Current official documentation supports Declarative Automation Bundles resources and serverless Python script tasks with a required versioned task environment ([Bundles resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources), [bundle serverless examples](https://docs.databricks.com/aws/en/dev-tools/bundles/examples), [serverless dependencies](https://docs.databricks.com/aws/en/compute/serverless/dependencies)). The bundle YAML parses locally; workspace compatibility is not verified. The local pipeline stores Delta by path and publishes through a local JSON pointer; Unity Catalog object registration, volume permissions, serverless execution behavior, and object-store pointer semantics have not been remotely tested.

Local task settings can be varied without editing the bundle, for example `--var="demo_scenario=ticket-correction"`; `late-po` and `late-receipt` are also generator scenarios. Scenario outputs remain explicitly synthetic. The full scenario must be run in an authenticated isolated dev workspace before this milestone can be marked complete.

When a workspace is available, authenticate locally without sharing credentials in chat, then validate:

```bash
.tools/databricks/databricks auth login --host https://<workspace-url> --profile enterprise-data-dev
.tools/databricks/databricks bundle validate -t dev
```

Review the isolated dev resources and workspace quotas before deploying/running. Free Edition is serverless-only and quota-limited; confirm it supports the declared Unity Catalog schema, managed volume, and serverless Job in the selected workspace. Local checks are not Databricks verification.

## Milestone 3 — local failure scenarios implemented; workspace controls unverified

The local pipeline now handles out-of-order revisions, additive CSV fields, unsupported schema-version quarantine, corrected higher revisions, ticket cancellations, linked positive-magnitude credits, and missing/stale source status. Credits must reference `supplier_id:invoice_id:invoice_line_id`; valid credits are negated once in the signed `invoice_net_amounts` product and are not compared to delivery allocation quantities. Ticket cancellation removes current accepted quantity and flags allocated invoice evidence. A `source_freshness` product reports missing feeds and flags latest source updates older than a synthetic 72-hour threshold. Raw files and revisions remain available; current reporting generations are retained without automatic pruning. The local recovery runbook covers outage, schema issues, replay, quarantine correction, partial publication, backfill limits, and rollback.

GitHub Actions now defines credential-free local Spark/Delta CI. The workflow is committed as configuration but has not yet run on GitHub. Governance roles and lineage limits are documented in `docs/GOVERNANCE.md`; no workspace principals, grants, native lineage, or Databricks integration tests are available here. The rebuild operates over retained files and recomputes full current state. A date-range-only backfill API is not implemented. Workspace verification remains outstanding.

## Milestone 4 — portfolio artifacts and measured experiment; workspace delivery pending

The project now includes a Mermaid architecture view, data dictionary, design decisions, a self-contained synthetic HTML report, and the executable recovery demonstration. The HTML report renders the latest saved successful local synthetic run; it is a reproducible snapshot, not a live dashboard.

The local allocation benchmark uses 10,000 invoice-line keys and 160,000 synthetic allocation rows (4,160,040 input bytes), with local Spark 4.0.1 on `local[2]` and two shuffle partitions. Before/after results are equal at 10,000 output rows / 10,000 tonnes. Grouping before the delivery join reduces join input from 160,000 to 10,000 rows, while warmed medians are 190.40 ms before and 188.08 ms after across three alternating runs. That small difference is not a demonstrated performance gain, so the optimization is not retained in production code. Output size is 103,200 Parquet bytes. First-pass measurements are order-sensitive (before always runs first) and are not treated as cold-run comparisons. Evidence: `evidence/milestone4-allocation-benchmark.json`.

No Databricks tuning, dashboard, workspace screenshot, cold-cluster comparison, or production-scale evidence is available. Milestone 4 local artifacts are prepared, but the measured optimization did not establish a runtime win and workspace-specific portfolio evidence remains pending.

## Remaining work

Milestone 1 local acceptance is complete. Milestone 3 local failure scenarios and recovery documentation are implemented. The full local acceptance suite passed with 18 tests in 976.03 seconds; the quarantine-repair case also passed a focused rerun after its test fixture was corrected. Remote Databricks execution and governance tests remain unverified; date-range backfill API and persisted repair/run-ledger automation remain future work. Milestone 4 has local portfolio artifacts and a measured experiment, but no runtime gain was established; cold-cluster and workspace evidence are still missing. Workspace URL/profile setup is the external blocker. These synthetic local artifacts demonstrate correctness only; they make no live-source, lineage enforcement, production-performance, or enterprise-scale claim.

## Portfolio follow-up — operator report and transfer example

The HTML report now resolves the current run through the publication pointer and leads with run status, whether publication succeeded, the consumer pointer and generation, counts of arrived/missing/stale feeds, and changes in accepted quantity, exception count, and quarantine count from the previous successful generation. Detailed feed rows remain available below the summary. The comparison is a concise current-versus-prior view; it does not yet classify individual records as added, changed, or removed.

`make saas-demo` now runs a full local Spark/Delta walkthrough: initial 10-unit billing mismatch with stale usage, renamed-file replay with business-event deduplication, a corrected usage revision, an injected interruption before pointer swap, and successful recovery. The walkthrough asserts that the pointer stays on the last good generation during failure and that the correction reconciles 190 observed and billed units. The verified run is recorded at `data/saas-demo/walkthrough-20261028T192343693456Z/evidence/walkthrough.json`. Every invocation uses a new timestamped folder beneath `data/saas-demo/`; its step summaries and generation IDs are saved in `evidence/walkthrough.json`. This remains an independent synthetic transfer example, not a generalized configurable framework. Workspace verification remains the existing Milestone 2 blocker.

## 2026-11-03 — Enterprise Data Platform migration

The project-facing name and description are now **Enterprise Data Platform**: "A Spark and Delta data platform for integrating operational sources, publishing trustworthy analytics, and monitoring pipeline health across industries." The Python distribution is `enterprise-data-platform` 0.2.0, package is `enterprise_data_platform`, CLI is `edp`, and the Databricks bundle and isolated resource names use the new slug. The GitHub repository and description were renamed to `dylancouch/enterprise-data-platform`; local `origin` points there. This active workspace directory still has its original local folder name.

Shared Spark/Delta mechanics live in `src/enterprise_data_platform/core.py`. The construction and SaaS examples call the same raw replay merge, revision selection, freshness assessment, and local generation publisher. Their separate contracts are in `docs/SOURCE_CONTRACTS.md` and `docs/SAAS_CONTRACT.md`; `docs/EXTENDING.md` records how to add a domain. The second example is sufficient to demonstrate transfer, so no third fixture was added. The Databricks bundle still runs only the construction example and has not been workspace validated.

Local checks completed: `.venv/bin/pip install -e '.[dev]'`; `edp --help`; renamed entrypoint `--help`; YAML parse plus bounded job/path assertions; Ruff and Python compilation; construction fixture generation with a separate oracle; `make demo`; and `make saas-demo`. The construction run published 10.000000 metric tonnes, zero invoice exceptions, and six source freshness rows; its sanitized report is `evidence/enterprise-construction-demo.json`, and its HTML snapshot is `reports/synthetic-dashboard.html`. The SaaS run showed 200 observed versus 190 billed units, replay suppression, a failed staged correction retaining the last-good pointer, and recovery at 190 versus 190. Sanitized evidence is `evidence/enterprise-saas-walkthrough.json`.

`make test` passed all **18 construction acceptance tests in 1051.98 seconds** after the package refactor. `make test TEST_ARGS=tests/test_saas.py` passed **1 focused SaaS conflict test in 11.97 seconds**, confirming that an equal-revision payload conflict refuses publication and retains the prior pointer. The first hosted GitHub Actions run passed all **19 tests in 1777.06 seconds**, then reached its 30-minute job limit before the SaaS CLI smoke step. After raising the workflow limit to 60 minutes, the [hosted rerun](https://github.com/dylancouch/enterprise-data-platform/actions/runs/41766154888) completed successfully: all 19 tests and the SaaS CLI smoke check passed. No Databricks authentication or workspace run was performed. The next platform milestone remains authenticated Databricks verification; SaaS malformed-input handling and multiple-invoice semantics remain future domain work.

## Repository layout cleanup

The installable package now lives at top-level `edp/`; the redundant `src/enterprise_data_platform/` tree was removed. Packaging discovery, CLI and script imports, the Databricks job path, local cache and fixture path calculations, CI commands, and current documentation use the new location. `AGENTS.md` records this layout preference for future work. This is a structural change only; the project name, distribution name, and domain contracts remain the same. Local checks passed: editable install without dependency changes, Ruff, Python compilation, both CLI help commands, package entrypoint metadata, bundle path assertion, and `git diff --check`. The Spark test suite and Databricks workspace run have not been executed for this layout change.
