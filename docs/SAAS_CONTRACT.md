# SaaS usage and billing example contract

This is a fictional synthetic example. It demonstrates the platform pattern on a second domain and does not model a real billing system.

| Feed | Grain and key | Revision | Measures and rules |
| --- | --- | --- | --- |
| `usage` | One metered event, keyed by `event_id` | Numeric `revision` is a full event image; latest revision wins | Integer `units` for one `customer_id` and `period`; aggregate before joining an invoice. |
| `invoices` | One invoice, keyed by `invoice_id` | Numeric `revision` is a full invoice image | Integer `billed_units`, decimal `unit_price`, and `currency` for one customer and period. |

Both feeds carry `source_updated_at` as an ISO UTC timestamp. Raw Delta retains source file name, file content hash, and row payload hash. A byte-identical renamed retry is suppressed as the same source evidence; a corrected usage event has a higher revision and remains in raw history. Equal key and revision with different payload is rejected before publication. The current event revision contributes once to monthly observed units. The example compares those units with invoice billed units and reports `BILLED_USAGE_MISMATCH`, `MISSING_USAGE`, or `MISSING_INVOICE` when applicable. `expected_amount` is observed units multiplied by unit price, represented as decimal money; no payment action is taken.

The walkthrough starts with 200 observed and 190 billed units. A revision changes one event from 80 to 70 units. An injected failure occurs after a new reconciliation Delta product is staged but before the pointer changes. Readers retain the preceding generation; rerunning publishes the 190-unit result. The example does not yet quarantine malformed rows or cover multiple invoices for one customer-period. Those require explicit rules before broader use.
