# Roadmap

The local examples are synthetic. Completed milestones established the construction pipeline, replay and recovery mechanics, a second SaaS example, an operational report, and the current repository layout. See [project status](STATUS.md) for evidence and limits.

## Local reliability verification

- [x] Run both examples through an identical export replay and confirm business totals stay unchanged.
- [x] Apply a higher source revision and deliver a late expected feed; confirm each published result changes once and source freshness records the arrival.
- [x] Stop each pipeline after a reporting product is staged; confirm the consumer pointer still names the previous complete generation and its values remain readable.
- [x] Rerun after the interruption; confirm the pointer changes to a complete corrected generation while prior generations remain inspectable.
- [x] Publish a synthetic operational report for both examples showing each run outcome, source state, replay suppression, business result, and consumer generation.
- [x] Execute the local Spark/Delta verification and record the command, result, and limits in [project status](STATUS.md).

## Later platform work

- Authenticate to an isolated Databricks workspace, validate the bundle, and repeat the acceptance cases there.
- Define and implement date-range backfill and automated repair/run-ledger semantics.
- Define SaaS malformed-input and multiple-invoice rules before extending that example.
