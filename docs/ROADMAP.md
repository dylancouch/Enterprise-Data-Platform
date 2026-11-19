# Roadmap

Milestones are numbered so completed work, current verification gaps, and future platform work are easy to distinguish. The project is intended to help business users connect operational data and get useful results. Realistic synthetic data is available as a starting option for users who do not yet have data to connect. Status describes this checkout and its available environment; an access-dependent task stays pending until it has actually run.

## Milestone 1 — Local reliability scenarios [complete]

- [x] Run both examples through an identical export replay and confirm business totals stay unchanged.
- [x] Apply a higher source revision and deliver a late expected feed; confirm each published result changes once and source freshness records the arrival.
- [x] Stop each pipeline after a reporting product is staged; confirm the consumer pointer still names the previous complete generation and its values remain readable.
- [x] Rerun after the interruption; confirm the pointer changes to a complete corrected generation while prior generations remain inspectable.
- [x] Publish synthetic operational reports for both examples.
- [x] Record local Spark/Delta reliability results and limitations.

## Milestone 2 — Reverify the current local checkout [complete]

The package layout changed after the previous full Spark suite run. The current checkout was reverified on 2026-11-17; see [project status](STATUS.md) for commands, versions, and results.

- [x] Rebuild the local virtual environment using the documented setup.
- [x] Run the full test suite, reliability verification, and lint checks against the current package layout.
- [x] Record commands, dependency versions, results, and remaining limits in [project status](STATUS.md).

## Milestone 3 — Databricks construction proof [complete for bounded cases]

- [x] Confirm the available free-plan workspace has an isolated project catalog and configure the dev target to use it.
- [x] Validate and deploy the Databricks bundle.
- [x] Run the construction example and repeat golden, replay, revision, late receipt, interrupted publication, and recovery cases in the workspace.
- [x] Record the bounded hosted results in [project status](STATUS.md) and [sanitized evidence](../evidence/databricks-construction-acceptance.json).
- [x] Document remaining gaps for other object stores, lineage, permissions, and scale; do not claim unverified behavior.

## Milestone 4 — SaaS input and invoice rules [complete locally]

The chosen local policy quarantines malformed records while valid rows continue. Current invoices aggregate once per customer-period; conflicting latest revisions block publication, and ambiguous prices or currencies get explicit exceptions.

- [x] Define malformed-input handling, including validation, quarantine, and whether valid rows continue.
- [x] Define behavior for multiple invoices for one customer and billing period, including revisions and duplicate or conflicting records.
- [x] Add the agreed rules to the [SaaS contract](SAAS_CONTRACT.md) and acceptance scenarios.
- [x] Implement the rules and add tests for the agreed cases.
- [x] Record results and limitations in [project status](STATUS.md).

## Milestone 5 — Backfill and automated repair [complete for bounded local cases]

- [x] Define date-range backfill behavior, including repeat runs, historical generations, and isolation from unrelated dates.
- [x] Define repair and run-ledger semantics, including how interrupted, failed, and replayed runs are represented and recovered.
- [x] Add the agreed behavior to the platform contract, runbook, and acceptance scenarios.
- [x] Implement local backfill, run ledger, and automatic selection of the newest unresolved failed run for SaaS periods and construction daily-delivery dates.
- [x] Verify clean rebuilds, reruns, failure recovery, and preservation of unrelated history; record evidence in [project status](STATUS.md) and [local acceptance evidence](../evidence/local-saas-backfill-acceptance.json).

The local repair command can use a failed run ID or automatically select the newest unresolved failure and its range. Local runs now hold an exclusive per-root process lock. Separate scheduled hosted repair sweeps operate on isolated synthetic construction and SaaS roots; local background scheduling and cross-job distributed locking remain outside the current implementation. Construction invoice and freshness products are rebuilt globally during a daily-date backfill.

## Milestone 6 — Hosted and operational hardening [complete for bounded cases]

- [x] Run the SaaS validation, multiple-invoice, and period-repair cases in Databricks and record separate hosted evidence.
- [x] Exercise hosted construction date-range backfill and repair against a retained baseline generation.
- [x] Define and verify concurrent-writer protection and unattended repair scheduling before treating the local ledger as an operational control.
- [x] Evaluate lineage, access-control boundaries, other object-store pointer semantics, and larger workloads; record measured results and limits.

## Milestone 7 — Connect business data and deliver a useful result [complete for the initial local CSV workflow]

Make the project useful to a business user who needs to reconcile purchasing, delivery, and invoice data. The user chooses their own data or realistic synthetic data and receives a useful business result. Understanding pipeline internals is not a prerequisite or the product goal.

### First workflow: purchasing-to-delivery-to-invoice reconciliation

- [x] Let a user import their own CSV exports and select files for purchase orders, deliveries or receipts, and invoices. Support invoice-to-delivery allocations when available.
- [x] Provide a guided field-mapping step from the user's column names to the fields required by the workflow; explain required and optional data before processing.
- [x] Validate selected files before running. Show missing required data, unusable fields, and common data issues in language the user can act on.
- [x] Reconcile ordered quantities, accepted delivered quantities, and invoiced quantities or amounts. Surface actionable exceptions such as mismatches, missing records, duplicate entries, and data needing attention, with source details to investigate them.
- [x] Provide a readable report and a downloadable or shareable result. Include the run outcome and any data issues that affect the result.
- [x] Offer realistic synthetic company data through the same selection, mapping, validation, and reporting flow. Clearly label it as synthetic; include ordinary records and representative problems so users can see how results are produced.
- [x] Make setup, data selection, field mapping, run status, exceptions, and recovery understandable without requiring code changes or knowledge of pipeline internals for this supported workflow.
- [x] Document accepted CSV structures, required and optional fields, produced results, and limitations before the user imports data. Ground field choices in public ERP documentation, import templates, and open procurement records; clearly label domain-specific assumptions that public data cannot establish.
- [x] Create a representative synthetic company export set with realistic multi-record purchasing, delivery-ticket, receipt, and invoice data. Include ordinary variation and actionable problems; keep expected outcomes separate from source files.
- [x] Run the complete import, suggested mapping, validation, reconciliation, and reporting paths on the representative dataset through both the recommended-example and CSV-upload flows; document setup and observed results. User-provided company data is welcome for optional validation, but is not a release prerequisite.

**Acceptance criterion met for the initial local CSV workflow:** A business user who does not need to read the code can select CSV exports, map and validate the data, and receive a useful reconciliation report. The supported structures and both app entry paths are verified with representative synthetic company exports grounded in public business-system schemas and documented domain assumptions. No private business export was available or required.

For this first workflow, “connect” means importing real business CSV exports and selecting what to analyze. Direct connections to accounting, procurement, or ERP systems can be prioritized later based on user demand and the systems users need.

## Status and evidence

See [project status](STATUS.md) for what has actually run, evidence links, and current limits. See [acceptance scenarios](ACCEPTANCE_TESTS.md) for implemented and future construction cases. Keep local and Databricks results separate, and label unexecuted scenarios as unverified.
