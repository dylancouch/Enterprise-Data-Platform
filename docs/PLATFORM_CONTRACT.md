# Shared platform contract

Enterprise Data Platform is a Spark/Delta demonstration with local execution for both examples and a bounded Databricks construction run. Every example uses synthetic data. The shared code in `data_pipelines/spark_delta.py` supplies Spark setup, Delta reads/writes, raw replay merging, revision selection, a 72-hour demonstration freshness assessment, and a generation pointer.

## Domain boundary

A domain supplies its own source schemas, business keys, revision fields, validation and reconciliation rules, report grains, and fixtures. The examples are `data_pipelines/construction_pipeline.py` and `data_pipelines/saas_pipeline.py`. A new domain can reuse the shared mechanics, but must implement and verify its own business semantics. The existing two domains are examples, not a no-code ingestion framework.

## Run and publication behavior

Source files land under each example's root. Raw Delta retains distinct source envelopes across replay. Current records use business keys and numeric revisions; equal-revision conflicts require explicit domain handling. Reporting products are staged under a unique generation. Consumers resolve the current generation through `tables/gold/current_generation.json`, which changes only after all products are written. The pointer swap is verified locally and in the tested Databricks Unity Catalog volume; other cloud object-store semantics are unverified.

Freshness statuses describe source presence and update age in the synthetic demo: `RECEIVED`, `MISSING_EXPECTED_FILE`, `MISSING_OR_INVALID_SOURCE_UPDATED_AT`, and `STALE_SOURCE_UPDATE_OVER_72H`. The 72-hour threshold is an example rule, not a promised service level. A successful pipeline run can still report stale sources, so consumers should read run outcome and source status separately.

## Local backfill and run ledger

Both examples accept an inclusive range after a complete baseline generation exists. SaaS scopes by billing period (`YYYY-MM`); construction scopes the `daily_delivered_quantities` product by project-local receipt date (`YYYY-MM-DD`). The scoped product is rebuilt from all retained source evidence for dates in the range and combined with unchanged rows outside the range from the prior generation. A replay with unchanged inputs should produce the same business rows while retaining earlier generations. Construction invoice and source-freshness products have no date partition in this demonstration, so they are recomputed from all retained inputs during a construction date-range run; the date range guarantees isolation only for daily delivered quantities.

Each local root has `evidence/run_ledger.jsonl`. It records success, injected failure, and raised errors with the domain, requested range, prior generation, staged or published generation, and a link from a repair run to the failed run. Repair accepts only a failed run ID with the same range and recomputes from retained source evidence. `repair-pending` automatically selects the newest unresolved failed run for a domain and reuses its range. The pointer remains the consumer contract. This is a local operational record, not a transactional scheduler, concurrent writer lock, or hosted ledger implementation.

## Data and security limits

No real customer or contractor data is included. Fixtures and known truth stay outside landing inputs. No live connectors, access-control enforcement, or production-scale performance are claimed. The bounded Databricks deployment covers the construction example only. Construction-specific contracts are in `docs/SOURCE_CONTRACTS.md` and `docs/BUSINESS_RULES.md`; SaaS-specific contracts are in `docs/SAAS_CONTRACT.md`.
