# Roadmap

The local examples use synthetic data. Milestones are numbered so completed work, current verification gaps, and future platform work are easy to distinguish. Status describes this checkout and its available environment; an access-dependent task stays pending until it has actually run.

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

The local repair command can use a failed run ID or automatically select the newest unresolved failure and its range. Invocation remains operator initiated; background scheduling and concurrent writer coordination remain future work. Construction invoice and freshness products are rebuilt globally during a daily-date backfill.

## Milestone 6 — Hosted and operational hardening [pending]

- [ ] Run the SaaS validation, multiple-invoice, and period-repair cases in Databricks and record separate hosted evidence.
- [ ] Exercise hosted construction date-range backfill and repair against a retained baseline generation.
- [ ] Define and verify concurrent-writer protection and unattended repair scheduling before treating the local ledger as an operational control.
- [ ] Evaluate lineage, access-control boundaries, other object-store pointer semantics, and larger workloads; record measured results and limits.

## Status and evidence

See [project status](STATUS.md) for what has actually run, evidence links, and current limits. See [acceptance scenarios](ACCEPTANCE_TESTS.md) for implemented and future construction cases. Keep local and Databricks results separate, and label unexecuted scenarios as unverified.
