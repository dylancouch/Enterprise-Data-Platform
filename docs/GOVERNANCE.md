# Construction example governance and access verification

The following is an intended access matrix for future Unity Catalog permission work. A project schema and volume have been deployed for the bounded synthetic jobs, but workspace principals and grants have not been configured or tested for these roles. This matrix is a design proposal, not evidence of enforced access controls. A read-only grant inspection on 2026-11-19 found no direct grants on the project schema or volume; the `workspace` catalog grants `USE_CATALOG` to workspace users. See [the evaluation](MILESTONE6_EVALUATION.md).

| Role | Intended access | Restrictions | Verification |
|---|---|---|---|
| Operations reader | Read published daily quantity and completeness reports | No raw invoice line amounts or write access | Workspace principal test pending |
| Finance reader | Read invoice exceptions, allocations, amounts, and source evidence | No payment actions or source mutation | Workspace principal test pending |
| Data engineer | Read raw/validated and operational status; write project pipeline tables | Project-owned schema/volume only; no unrelated resources | Workspace principal test pending |

Before claiming these access boundaries, map the roles to approved workspace groups, apply least-privilege Unity Catalog grants to the project schema and volume, then test positive and negative read/write cases using approved principals. Do not infer security from this document or bundle names.

## Lineage evidence

Native Databricks lineage was queried on 2026-11-19. Path events were present, while no registered Unity Catalog table names appeared in the project-matching events. See [the evaluation](MILESTONE6_EVALUATION.md) for counts and the query window. Local evidence carries source feed, source file, content hash, source record ID/revision, payload hash, ingestion time, batch ID, and published generation. These fields provide application-level traceability; they are not a substitute claim for complete platform-native lineage.
