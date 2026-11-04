# Add an operational data example

The two current examples show how to reuse platform mechanics while keeping business rules visible. A new domain is Python code under `edp/domains/`, accompanied by an explicit source contract and synthetic fixture. The shared core is a small library, not a configuration-only pipeline generator.

1. Define each feed's grain, business key, revision, update time, units, expected arrival, and acceptable schema changes. Keep expected answers outside landing inputs.
2. Read only declared landing prefixes. Attach source file and content hash before calling `merge_raw` in `edp.core`. Choose replay keys that retain distinct business revisions and do not erase conflicting evidence.
3. Resolve current records with `latest_revision` only after the domain has checked equal-revision conflicts and invalid values. Keep these domain checks explicit.
4. Aggregate at the declared grain before joins. Reconcile with decimal units and amounts where applicable. Describe any unresolved reference or missing feed as a visible state.
5. Use `freshness_status` for the synthetic arrival-age indicator, `write_delta` to stage all reporting products under one generation, and `publish_generation` only after every product and hard invariant succeeds.
6. Exercise initial load, identical replay, revised data, missing or late input, and interrupted publication. Save a sanitized synthetic run summary and document what local Spark/Delta verified. Databricks behavior needs a separate workspace run.

The construction example includes richer quarantine and allocation logic. The SaaS example demonstrates a smaller usage-to-invoice flow. Neither set of domain rules should be copied into a new example without checking its source semantics.
