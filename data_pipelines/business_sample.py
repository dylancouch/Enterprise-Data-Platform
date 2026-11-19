"""Representative export-shaped business data for the local user workflow."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from pathlib import Path

SAMPLE_NAME = "Northstar Civil"
SAMPLE_COMPANY_ID = "NORTHSTAR-CIVIL"


def _ticket(
    ticket_number: str,
    delivery_number: str,
    po_number: str,
    material: str,
    gross: str,
    tare: str,
    weighed_at: str,
    revision: str = "1",
    updated_at: str | None = None,
    weight_unit: str = "metric_tonne",
) -> dict[str, str]:
    return {
        "Scale ID": "YARD-SCALE-1",
        "Ticket Number": ticket_number,
        "Delivery Number": delivery_number,
        "Purchase Order": po_number,
        "Line Number": "1",
        "Item Code": material,
        "Gross Weight": gross,
        "Tare Weight": tare,
        "Weight UOM": weight_unit,
        "Ticket Date": weighed_at,
        "Status": "Valid",
        "Revision": revision,
        "Updated At": updated_at or weighed_at,
    }


# These are fictional records using common ERP-style purchasing fields plus
# workflow-specific scale ticket and site receipt details. See docs/BUSINESS_DATA_RESEARCH.md.
EXPORTS: dict[str, list[dict[str, str]]] = {
    "purchase_order_lines": [
        {"Purchase Order": "PO-1001", "Line Number": "1", "Project": "SITE-01", "Supplier": "SUP-01", "Item Code": "AGG-1", "Qty": "20", "UOM": "metric_tonne", "Rate": "12.50", "Price UOM": "metric_tonne", "Currency": "USD", "Transaction Date": "2026-10-19T12:00:00Z", "Status": "Open"},
        {"Purchase Order": "PO-1002", "Line Number": "1", "Project": "SITE-01", "Supplier": "SUP-02", "Item Code": "AGG-2", "Qty": "15", "UOM": "metric_tonne", "Rate": "18.00", "Price UOM": "metric_tonne", "Currency": "USD", "Transaction Date": "2026-10-19T12:00:00Z", "Status": "Open"},
        {"Purchase Order": "PO-1003", "Line Number": "1", "Project": "SITE-02", "Supplier": "SUP-01", "Item Code": "AGG-1", "Qty": "10", "UOM": "metric_tonne", "Rate": "12.50", "Price UOM": "metric_tonne", "Currency": "USD", "Transaction Date": "2026-10-20T12:00:00Z", "Status": "Open"},
        {"Purchase Order": "PO-1004", "Line Number": "1", "Project": "SITE-02", "Supplier": "SUP-02", "Item Code": "AGG-2", "Qty": "10", "UOM": "metric_tonne", "Rate": "18.00", "Price UOM": "metric_tonne", "Currency": "USD", "Transaction Date": "2026-10-20T12:00:00Z", "Status": "Open"},
    ],
    "scale_tickets": [
        _ticket("WT-2001", "DEL-2001", "PO-1001", "AGG-1", "12", "2", "2026-10-20T14:30:00Z"),
        _ticket("WT-2001", "DEL-2001", "PO-1001", "AGG-1", "12", "2", "2026-10-20T14:30:00Z"),
        _ticket("WT-2002", "DEL-2002", "PO-1002", "AGG-2", "9.5", "2", "2026-10-20T15:10:00Z"),
        _ticket("WT-2003", "DEL-2003", "PO-1003", "AGG-1", "4", "1", "2026-10-21T09:05:00Z"),
        _ticket("WT-2003", "DEL-2003", "PO-1003", "AGG-1", "4.2", "1", "2026-10-21T09:05:00Z", "2", "2026-10-21T11:00:00Z"),
        _ticket("WT-2004", "DEL-2004", "PO-1004", "AGG-2", "6", "1", "2026-10-21T10:20:00Z"),
        _ticket("WT-2005", "DEL-2005", "PO-1004", "AGG-2", "5", "1", "2026-10-22T11:40:00Z"),
        _ticket("WT-2006", "DEL-2006", "PO-1001", "AGG-1", "1", "0", "2026-10-22T13:00:00Z", weight_unit="ton"),
    ],
    "site_receipts": [
        {"Delivery Number": "DEL-2001", "Project": "SITE-01", "Received Date": "2026-10-20T15:00:00Z", "Receipt Status": "Accepted", "Receipt Number": "GR-3001", "Accepted Fraction": "1"},
        {"Delivery Number": "DEL-2002", "Project": "SITE-01", "Received Date": "2026-10-20T15:35:00Z", "Receipt Status": "Accepted", "Receipt Number": "GR-3002", "Accepted Fraction": "1"},
        {"Delivery Number": "DEL-2003", "Project": "SITE-02", "Received Date": "2026-10-21T09:30:00Z", "Receipt Status": "Accepted", "Receipt Number": "GR-3003", "Accepted Fraction": "1"},
        {"Delivery Number": "DEL-2004", "Project": "SITE-02", "Received Date": "2026-10-21T10:45:00Z", "Receipt Status": "Pending", "Receipt Number": "GR-3004", "Accepted Fraction": "1"},
        {"Delivery Number": "DEL-2005", "Project": "SITE-02", "Received Date": "2026-10-22T12:00:00Z", "Receipt Status": "Rejected", "Receipt Number": "GR-3005", "Accepted Fraction": "1"},
        {"Delivery Number": "DEL-2006", "Project": "SITE-01", "Received Date": "2026-10-22T13:15:00Z", "Receipt Status": "Accepted", "Receipt Number": "GR-3006", "Accepted Fraction": "1"},
    ],
    "invoices": [
        {"Supplier": "SUP-01", "Invoice ID": "AP-4001", "Invoice Date": "2026-10-21", "Currency": "USD", "Status": "Posted", "Invoice Number": "NMI-8841"},
        {"Supplier": "SUP-02", "Invoice ID": "AP-4002", "Invoice Date": "2026-10-21", "Currency": "USD", "Status": "Posted", "Invoice Number": "NMI-8842"},
        {"Supplier": "SUP-01", "Invoice ID": "AP-4003", "Invoice Date": "2026-10-22", "Currency": "USD", "Status": "Posted", "Invoice Number": "NMI-8843"},
        {"Supplier": "SUP-02", "Invoice ID": "AP-4004", "Invoice Date": "2026-10-22", "Currency": "USD", "Status": "Posted", "Invoice Number": "NMI-8844"},
    ],
    "invoice_lines": [
        {"Supplier": "SUP-01", "Invoice ID": "AP-4001", "Invoice Line Number": "1", "Purchase Order": "PO-1001", "Line Number": "1", "Item Code": "AGG-1", "Qty": "10", "UOM": "metric_tonne", "Rate": "12.50", "Price UOM": "metric_tonne", "Amount": "125.00"},
        {"Supplier": "SUP-02", "Invoice ID": "AP-4002", "Invoice Line Number": "1", "Purchase Order": "PO-1002", "Line Number": "1", "Item Code": "AGG-2", "Qty": "8", "UOM": "metric_tonne", "Rate": "18.00", "Price UOM": "metric_tonne", "Amount": "144.00"},
        {"Supplier": "SUP-01", "Invoice ID": "AP-4003", "Invoice Line Number": "1", "Purchase Order": "PO-1003", "Line Number": "1", "Item Code": "AGG-1", "Qty": "3", "UOM": "metric_tonne", "Rate": "12.50", "Price UOM": "metric_tonne", "Amount": "50.00"},
        {"Supplier": "SUP-02", "Invoice ID": "AP-4004", "Invoice Line Number": "1", "Purchase Order": "PO-1004", "Line Number": "1", "Item Code": "AGG-2", "Qty": "4", "UOM": "metric_tonne", "Rate": "18.00", "Price UOM": "metric_tonne", "Amount": "72.00"},
        {"Supplier": "SUP-02", "Invoice ID": "AP-4004", "Invoice Line Number": "2", "Purchase Order": "PO-1004", "Line Number": "1", "Item Code": "AGG-2", "Qty": "1", "UOM": "metric_tonne", "Rate": "18.00", "Price UOM": "metric_tonne", "Amount": "18.00"},
    ],
    "invoice_delivery_allocations": [
        {"Supplier": "SUP-01", "Invoice ID": "AP-4001", "Invoice Line Number": "1", "Allocation": "1", "Delivery Number": "DEL-2001", "Allocated Quantity": "10", "UOM": "metric_tonne"},
        {"Supplier": "SUP-02", "Invoice ID": "AP-4002", "Invoice Line Number": "1", "Allocation": "1", "Delivery Number": "DEL-2002", "Allocated Quantity": "8", "UOM": "metric_tonne"},
        {"Supplier": "SUP-01", "Invoice ID": "AP-4003", "Invoice Line Number": "1", "Allocation": "1", "Delivery Number": "DEL-2003", "Allocated Quantity": "3", "UOM": "metric_tonne"},
        {"Supplier": "SUP-02", "Invoice ID": "AP-4004", "Invoice Line Number": "1", "Allocation": "1", "Delivery Number": "DEL-2004", "Allocated Quantity": "4", "UOM": "metric_tonne"},
    ],
}

EXPECTED = {
    "accepted_delivered_metric_tonnes": "20.700000",
    "net_invoice_amount_usd": "409.00",
    "invoice_exception_count": 4,
    "duplicate_business_envelopes_suppressed": 1,
    "quarantine_rows": 1,
    "quarantine_reasons": {"UNKNOWN_WEIGHT_UNIT": 1},
    "invoice_exception_reasons": {
        "QUANTITY_MISMATCH": 1,
        "LINE_AMOUNT_MISMATCH": 1,
        "MISSING_ALLOCATION": 1,
        "PENDING_SUPPORTING_DELIVERY": 1,
    },
    "rejected_delivery_metric_tonnes_excluded": "4.000000",
    "pending_delivery_metric_tonnes_excluded": "5.000000",
}


def export_bytes(feed: str) -> bytes:
    """Serialize a sample feed exactly as the downloadable CSV is written."""
    rows = EXPORTS[feed]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


SAMPLE_EXPORT_HASHES = {
    feed: hashlib.sha256(export_bytes(feed)).hexdigest() for feed in EXPORTS
}


def write_sample(root: Path) -> None:
    """Write source exports and separate expected outcomes beneath ``root``."""
    exports_root = root / "exports"
    for feed in EXPORTS:
        path = exports_root / f"{feed}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(export_bytes(feed))
    expected_path = root / "expected" / "business_outcomes.json"
    expected_path.parent.mkdir(parents=True, exist_ok=True)
    expected_path.write_text(json.dumps(EXPECTED, indent=2) + "\n", encoding="utf-8")
