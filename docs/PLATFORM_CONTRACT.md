# Shared platform contract

Enterprise Data Platform is a local Spark/Delta demonstration with Databricks as its intended execution target. Every example uses synthetic data. The shared code in `src/enterprise_data_platform/core.py` supplies Spark setup, Delta reads/writes, raw replay merging, revision selection, a 72-hour demonstration freshness assessment, and a local generation pointer.

## Domain boundary

A domain supplies its own source schemas, business keys, revision fields, validation and reconciliation rules, report grains, and fixtures. Construction lives in `domains/construction`; SaaS usage and billing lives in `domains/saas.py`. A new domain can reuse the shared mechanics, but must implement and verify its own business semantics. The existing two domains are examples, not a no-code ingestion framework.

## Run and publication behavior

Source files land under each example's root. Raw Delta retains distinct source envelopes across replay. Current records use business keys and numeric revisions; equal-revision conflicts require explicit domain handling. Reporting products are staged under a unique generation. Consumers resolve the current generation through `tables/gold/current_generation.json`, which changes only after all products are written. The pointer swap is verified on the local filesystem; cloud object-store semantics are unverified.

Freshness statuses describe source presence and update age in the synthetic demo: `RECEIVED`, `MISSING_EXPECTED_FILE`, `MISSING_OR_INVALID_SOURCE_UPDATED_AT`, and `STALE_SOURCE_UPDATE_OVER_72H`. The 72-hour threshold is an example rule, not a promised service level. A successful pipeline run can still report stale sources, so consumers should read run outcome and source status separately.

## Data and security limits

No real customer or contractor data is included. Fixtures and known truth stay outside landing inputs. No live connectors, Databricks deployment, access-control enforcement, or production-scale performance are claimed. Construction-specific contracts are in `docs/SOURCE_CONTRACTS.md` and `docs/BUSINESS_RULES.md`; SaaS-specific contracts are in `docs/SAAS_CONTRACT.md`.
