# Import business CSV data

The local browser app supports a purchasing-to-delivery-to-invoice reconciliation. Start it with `make app`, then choose **Use my business data**. The app runs on `127.0.0.1` and stores normalized input files and results locally under `data/user-runs/`.

Import UTF-8 CSV exports separately for each data type. Multiple files can be selected for one type when their headers match. The app previews each file and suggests mappings from its column names to the fields below. The business name or ID is supplied once and applied to imported rows.

These fields describe this workflow's supported input contract, not a universal ERP export format. The project is grounding representative examples in public ERP documentation and public procurement records while marking scale-ticket and site-acceptance details as workflow-specific assumptions; see [business data structure research](BUSINESS_DATA_RESEARCH.md).

The app's recommended example is a multi-record fictional company. Its export-shaped CSV files are in [`examples/representative_business_data/exports`](../examples/representative_business_data/exports), and expected business outcomes are kept separately in `expected/business_outcomes.json`. The example uses varied headers such as `Purchase Order`, `Item Code`, `Qty`, `Rate`, and `Transaction Date` to exercise the same suggested mapping and normalization used for user files. Users can also select those CSVs in **Use my business data** to inspect the mapping experience; the app recognizes the unchanged sample files and continues to label their result as synthetic.

Use ISO 8601 dates and timestamps where possible (for example, `2026-10-20` or `2026-10-20T14:30:00Z`). The report retains the imported filename and content hash on source rows so exceptions can be traced back to the provided export.

## Required data

| Data type | Required values |
| --- | --- |
| Purchase order lines | Purchase order number and line, project/location, supplier, material, ordered quantity and unit, unit price and price unit, currency, effective date, status |
| Delivery weights or tickets | Scale/source ID, ticket, delivery ID, linked purchase order and line, material, gross and tare weights, weight unit, weighed date/time, status |
| Delivery receipts | Delivery ID, project/location, received date/time, receipt status (`accepted`, `rejected`, or `pending`) |
| Invoice headers | Supplier, invoice ID, invoice date, currency, status, invoice number |
| Invoice lines | Supplier, invoice ID and line, linked purchase order and line, material, billed quantity and unit, unit price and price unit, line amount |

Invoice-to-delivery links are optional. When supplied, each row needs supplier, invoice and line IDs, allocation ID, delivery ID, allocated quantity, and unit. Without links, invoice lines remain visible as `MISSING_ALLOCATION` exceptions; the app does not guess which delivery an invoice belongs to.

Optional columns include source revision and source updated time. If revision is missing, the app assigns revision 1. If source updated time is missing, it uses the business event date when available, otherwise the import time. This affects freshness reporting, so include the source's update timestamp when the export provides one.

## Current limits

- CSV is the supported input format. Excel workbooks and live system connections are not included in this first import flow.
- The reconciliation supports USD and the weight units `kg`, `metric_tonne`, and `us_short_ton`. Tax and freight are excluded.
- Each imported run is saved separately. The app does not modify the source export files.
- The app is designed for local use. Files and reports remain in the local project directory; do not expose the app port to a public network.
- Synthetic example data is available in the same app for users who do not have exports ready.
