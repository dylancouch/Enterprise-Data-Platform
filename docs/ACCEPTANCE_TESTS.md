# Construction example acceptance scenarios

Executable assertions live in `tests/test_pipeline.py`, with sanitized local run evidence under `evidence/`. This checklist includes implemented and future scenarios; `docs/STATUS.md` records which have actually run. Golden output, replay, revision, late receipt, interrupted publication, and recovery have also been exercised in Databricks; see [hosted evidence](../evidence/databricks-construction-acceptance.json). Hosted date-range backfill and repair are recorded separately in [backfill evidence](../evidence/databricks-construction-backfill.json). Other cases remain unverified there. Keep source scenario truth out of ingestion inputs.

| Scenario | Expected behavior |
| --- | --- |
| Clean purchase, 10-tonne accepted delivery, exact invoice | Delivered quantity 10; no reconciliation discrepancy |
| Same export rerun; duplicate event in a newly named file | Business counts/amounts unchanged; retry/dedup accounting visible |
| Ticket revision from 10 to 9 tonnes | Current delivery becomes 9, never 19; original invoice becomes a quantity exception; prior publication remains inspectable |
| Older revision arrives after newer revision | Current state remains newer; valid history retained |
| Same business key/revision, conflicting content | Visible conflict with deterministic handling; no nondeterministic winner |
| Receipt arrives two days late | Original receipt-date total is corrected; pending invoice resolves where appropriate |
| PO arrives after ticket | Unresolved reference becomes linked without manual raw-data edits |
| Mixed kg, metric tonne, US short ton | Known decimal conversions match independent expected values |
| Ambiguous ton or unsupported cubic-yard input | Quarantined with clear reason; no guessed conversion |
| Gross weight below tare; invalid timestamp | Invalid row isolated with provenance; valid rows continue under documented policy |
| Partial billing; several deliveries per invoice line | Allocations conserve quantities; no join fan-out; only supported amount reconciles |
| Two invoice IDs share a supplier invoice number | Suspected duplicate warning; both raw records preserved |
| Invoice precedes receipt | Pending evidence rather than confirmed mismatch; transition auditable |
| PO price amendment | Correct effective price selected; prior deliveries not silently repriced |
| Ticket cancellation / linked credit | Current quantity and net billed value corrected once; history retained |
| Additive field vs incompatible schema change | Defined compatible evolution vs quarantine/failure behavior; no silent reinterpretation |
| Process stops between reporting-table writes | Consumers see last complete generation; restart converges to the correct new generation |
| Rebuild a historical date and rerun it | Same results as clean rebuild; unrelated dates and history preserved |
| Lost checkpoint / replay old files | Recovery policy prevents duplicate business effects or refuses unsafe operation with a documented repair path |
| Source misses scheduled arrival | Freshness/completeness breach visible; last-good data time shown |
| Positive-magnitude credit references original invoice line | Net invoice amount is reduced exactly once; credit is excluded from delivery allocation quantity checks |
| Credit has no valid original reference | Invoice-line exception is visible and source evidence is retained |
| Ticket cancellation arrives as a revision | Current accepted quantity drops; an allocated invoice line is flagged; prior generation remains inspectable |
| Additive CSV field and unsupported schema version | Additive field is retained; unsupported version is quarantined while prior supported record remains current |
| Required source feed absent from a run | Published source completeness product reports `MISSING_EXPECTED_FILE` |

Milestone 1 must execute clean flow, duplicate replay, ticket correction, late/missing-reference handling, unit/amount validation, and interrupted publication tests. Later milestones extend the full matrix.

## Evidence requirements
- Test command, environment/dependency versions, seed, row counts and fixture version.
- Expected and actual normalized quantities, money, exception counts and states.
- Before/after current and historical outputs for corrections and reruns.
- Run identifiers, source provenance, quarantine reasons, publication generation.
- Local versus Databricks checks explicitly separated; unexecuted tests labeled unverified.
- Performance measurements are accepted only with correctness invariants passing on the same data.

## Local reliability gate

Run `make verify-reliability` after the local environment is installed. The command exercises both synthetic domains through unchanged replay, a higher revision, a late expected feed, interrupted product publication, and recovery. It reads the consumer pointer and the Delta products it names, then writes `evidence/local-reliability.json` and `reports/synthetic-reliability.html`. Any failed assertion exits before those artifacts are replaced. The focused regression tests for the construction failed-run record and SaaS late invoice are in `tests/test_pipeline.py` and `tests/test_saas.py`.

## Local SaaS and backfill additions

| Scenario | Expected behavior |
| --- | --- |
| SaaS malformed units beside valid rows | Invalid row appears in `input_quarantine`; valid rows still reconcile |
| Several invoice IDs for one customer-period | Billed units aggregate once; observed usage is not multiplied; invoice count and IDs remain visible |
| Higher invoice revision or identical replay | Latest invoice contributes once; identical evidence does not change business totals |
| Equal invoice ID/revision with conflicting content | Run fails before publication; last-good pointer remains active and failure is in the ledger |
| Mixed invoice prices or currencies | Explicit exception; ambiguous expected amount or currency remains null |
| SaaS period backfill or construction daily-date backfill | Inclusive scoped rows rebuild; unrelated period/date rows remain unchanged |
| Interrupted scoped backfill and repair | Pointer stays on baseline, failed run is recorded, repair links to it, and rerun converges |
| Select newest pending local repair | Domain-specific ledger lookup skips failures already closed by a successful repair chain |

The construction date range scopes only `daily_delivered_quantities`; invoice and freshness products are recomputed globally. Local cases are recorded in [local acceptance evidence](../evidence/local-saas-backfill-acceptance.json). Hosted SaaS validation, multiple-invoice aggregation, and period repair passed in the Databricks development workspace; see [hosted SaaS evidence](../evidence/databricks-saas-acceptance.json). Hosted construction date-range failure, retained pointer, and linked repair passed against a two-date baseline; see [hosted backfill evidence](../evidence/databricks-construction-backfill.json).
