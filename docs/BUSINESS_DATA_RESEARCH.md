# Business data structure research

The first workflow must be testable without access to a private company's exports. This note records public evidence used to shape representative input files, what that evidence can establish, and what remains a project assumption.

## Public evidence

- [ERPNext data import documentation](https://docs.frappe.io/erpnext/data-import) describes importing CSV or Excel using a template generated for a selected document type. It distinguishes required and optional fields, supports mapping renamed columns, and represents line items as child records associated with a parent document. This supports accepting separate document/header and line exports and mapping user-specific column names; it also shows why one universal ERP column layout should not be assumed.
- [ERPNext purchase order documentation](https://docs.frappe.io/erpnext/purchase-order), [purchase receipt documentation](https://docs.frappe.io/erpnext/purchase-receipt), and [purchase invoice documentation](https://docs.frappe.io/erpnext/purchase-invoice) describe the operational links between what was ordered, what was received, and what a supplier billed. The system's documents carry item, quantity, rate, supplier, and source-document references through the process.
- [UK public purchase-order transaction data](https://www.data.gov.uk/dataset/40c1b9d0-999c-46b2-bc9a-7c4a91a074e1/purchase-order-transactions) provides real disclosed purchasing records and describes a PO-to-delivery-to-invoice process. It is useful evidence that public transaction extracts exist and for checking purchasing vocabulary, but does not provide a complete linked set of private delivery tickets, receipts, and invoice lines for this product's reconciliation.
- The [Procurement purchase orders and receipts sample dataset](https://databasesample.com/datasets/procurement-purchase-orders-and-receipts-dataset) is explicitly generated synthetic data. It includes linked PO lines, partial goods receipts, and supplier invoices. It is useful as a secondary example of plausible relational shapes and edge cases, not as evidence of real business distributions.

## What we can and cannot claim

Public documentation and disclosed procurement records support common concepts such as document IDs, line IDs, suppliers, items, quantities, units, dates, rates, currencies, and links from receipts and invoices back to order lines. They do not establish a single export format, common identifiers across a company's scale system and ERP, or the exact meanings of a particular company's acceptance statuses.

The project-specific fields for scale tickets (gross and tare weights), delivery IDs, site receipt acceptance, accepted fractions, and invoice-to-delivery allocations are a modeled contract for businesses with those operational records. Validate and explain them as assumptions. Do not describe the synthetic company files as actual customer data or a universal ERP standard.

## Representative sample and verification

The representative sample now contains multiple orders and lines, partial delivered quantities, accepted/rejected/pending receipts, multiple tickets, a duplicate row, an invoice amount and quantity discrepancy, a missing delivery allocation, an unsupported unit, and a corrected ticket revision. Its source CSVs use separate feeds and plausible ERP-style column labels rather than relying only on canonical pipeline names. This makes the supported workflow exercise normal results and user-actionable problems without claiming the sample is a real company's data.

Keep the synthetic source files and expected business outcomes separate. Run the same CSV parsing, guided mapping, validation, normalization, reconciliation, and report generation used for an imported customer file. Verify that totals and exceptions agree with independently recorded expected outcomes. Offer the sample in the app for users who have no exports yet. If a business later volunteers anonymized or redacted exports, use them as optional compatibility feedback, with their permission; the first release must not depend on that access.
