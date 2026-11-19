# Milestone 6 evaluation

This is bounded evidence for the synthetic development deployment on 2026-11-19. It does not authorize production use.

## Native lineage

A read-only query of `system.access.table_lineage` found 1,716 events in the last seven days with a source or target path containing `enterprise_data`. Of these, 1,693 had a source path and 686 had a target path. Neither source nor target was a registered Unity Catalog table in those events. The most recent event in the query was at 18:15 UTC; the hosted construction backfill finished later, so this snapshot cannot establish lineage for that particular run. Query details are in [hosted governance evidence](../evidence/hosted-governance-evaluation.json). [Databricks lineage documentation](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage) describes path lineage and the limits of path-based column lineage. The pipeline's CSV and Delta outputs are path based; a registered table design would make table ownership and column lineage easier to inspect.

## Access boundaries

The project schema and volume are owned by the deployment user. The grant API returned no direct grants on either object, and no effective grants in the volume listing. The workspace catalog has an inherited `USE_CATALOG` grant for workspace users. These observations are recorded in [hosted governance evidence](../evidence/hosted-governance-evaluation.json). They do not prove that the three proposed reader/engineer roles in [governance](GOVERNANCE.md) are enforced. No approved role principals are configured, so positive and negative access tests could not be run. Applying role grants and testing them requires named principals in a workspace intended for that purpose.

## Pointer semantics on other object stores

`publish_generation` writes a temporary JSON file and uses `Path.replace` to switch the current pointer. This worked in local filesystems and the tested Unity Catalog volume. It is not an object-store portability contract. [AWS documents that S3 renames copy then delete](https://docs.aws.amazon.com/AmazonS3/latest/userguide/copy-object.html), and [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) use `If-Match` or `If-None-Match` to reject conflicting updates. No independent S3, Azure Blob, or GCS bucket was available to test. A direct object-store adapter needs a single-object conditional pointer write, versioned/immutable product paths, and a reader check that the referenced generation is complete before claiming the same publication guarantee.

## Larger local allocation workload

[Local scale evidence](../evidence/local-scale-evaluation.json) used 50,000 invoice lines and 16 allocations per delivery: 800,000 allocation rows, five times the row count of the earlier experiment. Pre-aggregation reduced join input from 800,000 to 50,000 rows, and both plans returned equal results. Three warmed repetitions measured median 378.10 ms before and 328.70 ms after pre-aggregation on local Spark with two worker threads. This is a synthetic join microbenchmark; the difference does not establish end-to-end throughput or hosted performance. Full pipeline load, storage cost, and concurrency remain unmeasured.
