"""CSV intake and canonical field mapping for the purchasing reconciliation flow."""

from __future__ import annotations

import csv
import hashlib
import io
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class FeedSpec:
    title: str
    required: tuple[str, ...]
    optional: tuple[str, ...] = ()


FEEDS: dict[str, FeedSpec] = {
    "purchase_order_lines": FeedSpec(
        "Purchase order lines",
        ("po_id", "po_line_id", "project_id", "supplier_id", "material_id", "ordered_quantity",
         "quantity_unit", "unit_price", "price_unit", "currency", "effective_at", "status"),
        ("source_revision", "source_updated_at"),
    ),
    "scale_tickets": FeedSpec(
        "Delivery weights or tickets",
        ("scale_system_id", "ticket_id", "delivery_id", "po_id", "po_line_id", "material_id",
         "gross_weight", "tare_weight", "weight_unit", "weighed_at", "status"),
        ("source_revision", "source_updated_at"),
    ),
    "site_receipts": FeedSpec(
        "Delivery receipts",
        ("delivery_id", "project_id", "received_at", "acceptance_status"),
        ("receipt_id", "accepted_fraction", "source_revision", "source_updated_at"),
    ),
    "invoices": FeedSpec(
        "Invoice headers",
        ("supplier_id", "invoice_id", "invoice_date", "currency", "status", "invoice_number"),
        ("normalized_invoice_number", "source_revision", "source_updated_at"),
    ),
    "invoice_lines": FeedSpec(
        "Invoice lines",
        ("supplier_id", "invoice_id", "invoice_line_id", "po_id", "po_line_id", "material_id",
         "billed_quantity", "quantity_unit", "unit_price", "price_unit", "line_amount"),
        ("document_type", "original_invoice_line_ref", "invoice_date", "source_revision", "source_updated_at"),
    ),
    "invoice_delivery_allocations": FeedSpec(
        "Invoice-to-delivery links (optional)",
        ("supplier_id", "invoice_id", "invoice_line_id", "allocation_id", "delivery_id",
         "allocated_quantity", "quantity_unit"),
        ("source_revision", "source_updated_at"),
    ),
}

BUSINESS_KEYS: dict[str, tuple[str, ...]] = {
    "purchase_order_lines": ("company_id", "po_id", "po_line_id"),
    "scale_tickets": ("company_id", "scale_system_id", "ticket_id"),
    "site_receipts": ("company_id", "receipt_id"),
    "invoices": ("company_id", "supplier_id", "invoice_id"),
    "invoice_lines": ("company_id", "supplier_id", "invoice_id", "invoice_line_id"),
    "invoice_delivery_allocations": (
        "company_id", "supplier_id", "invoice_id", "invoice_line_id", "allocation_id"
    ),
}

FIELD_LABELS = {
    "po_id": "Purchase order number",
    "po_line_id": "Purchase order line",
    "project_id": "Project or location ID",
    "supplier_id": "Supplier ID or name",
    "material_id": "Product or material ID",
    "ordered_quantity": "Ordered quantity",
    "quantity_unit": "Quantity unit",
    "unit_price": "Unit price",
    "price_unit": "Price unit",
    "currency": "Currency",
    "status": "Status",
    "effective_at": "Order effective date",
    "scale_system_id": "Scale or source ID",
    "ticket_id": "Ticket number",
    "delivery_id": "Delivery ID",
    "gross_weight": "Gross weight",
    "tare_weight": "Tare weight",
    "weight_unit": "Weight unit",
    "weighed_at": "Weighed date and time",
    "received_at": "Received date and time",
    "acceptance_status": "Receipt status",
    "receipt_id": "Receipt ID",
    "accepted_fraction": "Accepted fraction (usually 1 or 0)",
    "invoice_id": "Invoice ID",
    "invoice_date": "Invoice date",
    "invoice_number": "Invoice number shown to the business",
    "normalized_invoice_number": "Normalized invoice number",
    "invoice_line_id": "Invoice line number",
    "billed_quantity": "Billed quantity",
    "line_amount": "Invoice line amount",
    "document_type": "Document type (invoice or credit)",
    "original_invoice_line_ref": "Original invoice line reference for credits",
    "allocation_id": "Allocation ID",
    "allocated_quantity": "Quantity assigned to this delivery",
    "source_revision": "Revision number (optional; defaults to 1)",
    "source_updated_at": "Source updated date and time (optional)",
}

ALIASES = {
    "po_id": ("po", "po_number", "purchase_order", "purchase_order_number", "purchase_order_id", "order_number"),
    "po_line_id": ("po_line", "po_line_number", "purchase_order_line", "purchase_order_item", "line_number"),
    "project_id": ("project", "job", "job_number", "location", "site"),
    "supplier_id": ("supplier", "vendor", "vendor_id", "supplier_name"),
    "material_id": ("material", "product", "item", "sku", "item_id", "item_code"),
    "ordered_quantity": ("order_quantity", "qty_ordered", "quantity_ordered", "qty"),
    "quantity_unit": ("unit", "uom", "quantity_uom"),
    "unit_price": ("price", "po_unit_price", "rate"),
    "price_unit": ("price_uom",),
    "currency": ("currency_code",),
    "effective_at": ("order_date", "effective_date", "po_date", "transaction_date"),
    "scale_system_id": ("scale_id", "scale", "weigh_station"),
    "ticket_id": ("ticket", "ticket_number", "scale_ticket", "scale_ticket_number"),
    "delivery_id": ("delivery", "delivery_number", "shipment_id", "receipt_delivery_id"),
    "gross_weight": ("gross", "gross_qty", "gross_quantity"),
    "tare_weight": ("tare", "tare_qty", "tare_quantity"),
    "weight_unit": ("weight_uom",),
    "weighed_at": ("weighed_date", "ticket_date", "delivery_date", "delivered_at"),
    "received_at": ("received_date", "receipt_date", "accepted_at"),
    "acceptance_status": ("receipt_status", "accepted", "status"),
    "receipt_id": ("receipt", "receipt_number"),
    "invoice_id": ("invoice", "invoice_key"),
    "invoice_date": ("document_date", "bill_date"),
    "invoice_number": ("vendor_invoice_number", "document_number", "bill_number"),
    "invoice_line_id": ("invoice_line", "invoice_line_number", "line_number"),
    "billed_quantity": ("quantity_billed", "qty", "quantity"),
    "line_amount": ("amount", "extended_amount", "extended_price"),
    "allocation_id": ("allocation",),
    "allocated_quantity": ("allocation_quantity", "allocated_qty"),
    "source_updated_at": ("updated_at", "last_modified", "modified_at"),
    "source_revision": ("revision", "version"),
}

EVENT_FIELDS = {
    "purchase_order_lines": "effective_at",
    "scale_tickets": "weighed_at",
    "site_receipts": "received_at",
    "invoices": "invoice_date",
    "invoice_lines": "invoice_date",
}


def _normal(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _canonical_unit(value: str) -> str:
    key = _normal(value)
    return {
        "kilogram": "kg",
        "kilograms": "kg",
        "kgs": "kg",
        "metric_tonne": "metric_tonne",
        "metric_tonnes": "metric_tonne",
        "tonne": "metric_tonne",
        "tonnes": "metric_tonne",
        "mt": "metric_tonne",
        "us_short_ton": "us_short_ton",
        "short_ton": "us_short_ton",
        "short_tons": "us_short_ton",
    }.get(key, key)


def suggest_column(field: str, headers: list[str]) -> str | None:
    candidates = {_normal(field), *(_normal(alias) for alias in ALIASES.get(field, ()))}
    return next((header for header in headers if _normal(header) in candidates), None)


def read_csv_upload(payload: bytes) -> tuple[list[str], list[dict[str, str]]]:
    try:
        content = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("This file is not UTF-8 CSV. Save or export it as a UTF-8 CSV and try again.") from exc
    try:
        dialect = csv.Sniffer().sniff(content[:8192], delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(content, newline=""), dialect=dialect)
    if not reader.fieldnames:
        raise ValueError("The CSV needs a header row naming its columns.")
    headers = [header.strip() if header else "" for header in reader.fieldnames]
    if not all(headers) or len(set(headers)) != len(headers):
        raise ValueError("Column names must be present and unique.")
    rows = []
    for row_number, row in enumerate(reader, start=2):
        if None in row:
            raise ValueError(f"Row {row_number} has more values than the header row.")
        rows.append({str(key).strip(): (value or "").strip() for key, value in row.items()})
    if not rows:
        raise ValueError("The CSV has a header but no data rows.")
    return headers, rows


def normalize_feed(
    feed: str,
    uploads: Iterable[bytes],
    mappings: dict[str, str],
    company_id: str,
    filenames: Iterable[str] | None = None,
) -> tuple[list[str], list[dict[str, str]]]:
    """Map one or more same-shape CSV exports into the pipeline's canonical feed."""
    if feed not in FEEDS:
        raise ValueError(f"Unsupported data type: {feed}")
    uploads = list(uploads)
    if not uploads:
        raise ValueError(f"Choose a CSV file for {FEEDS[feed].title.lower()}.")
    if not company_id.strip():
        raise ValueError("Enter a company or business name before importing.")

    targets = [target for target in mappings.values() if target]
    if len(targets) != len(set(targets)):
        raise ValueError(f"A source column for {FEEDS[feed].title.lower()} is mapped more than once.")
    missing = [field for field in FEEDS[feed].required if field not in targets]
    if missing:
        labels = ", ".join(FIELD_LABELS.get(field, field) for field in missing)
        raise ValueError(f"Map these required fields for {FEEDS[feed].title.lower()}: {labels}.")

    all_rows: list[dict[str, str]] = []
    expected_headers: set[str] | None = None
    imported_at = datetime.now(UTC).isoformat()
    upload_names = list(filenames) if filenames is not None else ["business-export.csv"] * len(uploads)
    if len(upload_names) != len(uploads):
        raise ValueError("Each uploaded file needs a filename.")
    for payload, filename in zip(uploads, upload_names, strict=True):
        headers, rows = read_csv_upload(payload)
        if expected_headers is None:
            expected_headers = set(headers)
        elif set(headers) != expected_headers:
            raise ValueError("Files for one data type need the same columns. Import different formats separately.")
        if set(mappings) - set(headers):
            raise ValueError("The selected column mapping does not match every uploaded file.")
        mapped_fields = {source: target for source, target in mappings.items() if target}
        for row_number, source_row in enumerate(rows, start=2):
            row = {target: source_row.get(source, "") for source, target in mapped_fields.items()}
            missing_values = [field for field in FEEDS[feed].required if not row.get(field, "").strip()]
            if missing_values:
                fields = ", ".join(FIELD_LABELS.get(field, field) for field in missing_values)
                raise ValueError(f"Row {row_number} has empty required values: {fields}.")
            row["company_id"] = company_id.strip()
            row["source_system"] = "business_csv_import"
            row["schema_version"] = "1.0"
            row["source_revision"] = row.get("source_revision", "").strip() or "1"
            for unit_field in ("quantity_unit", "weight_unit", "price_unit"):
                if row.get(unit_field):
                    row[unit_field] = _canonical_unit(row[unit_field])
            if row.get("currency"):
                row["currency"] = row["currency"].strip().upper()
            if row.get("acceptance_status"):
                row["acceptance_status"] = row["acceptance_status"].strip().lower()
            if row.get("document_type"):
                row["document_type"] = row["document_type"].strip().lower()
            event_field = EVENT_FIELDS.get(feed)
            event_time = row.get(event_field, "").strip() if event_field else ""
            updated_at = row.get("source_updated_at", "").strip() or event_time or imported_at
            row["source_updated_at"] = updated_at
            row["event_time"] = event_time or updated_at
            row["imported_file"] = filename
            row["imported_file_content_hash"] = hashlib.sha256(payload).hexdigest()
            row.setdefault("status", "")
            if feed == "site_receipts":
                row["receipt_id"] = row.get("receipt_id", "").strip() or row["delivery_id"]
                row["accepted_fraction"] = row.get("accepted_fraction", "").strip() or "1"
            if feed == "invoices":
                row["normalized_invoice_number"] = (
                    row.get("normalized_invoice_number", "").strip()
                    or _normal(row["invoice_number"])
                )
            if feed == "invoice_lines":
                row["document_type"] = row.get("document_type", "").strip() or "invoice"
                row["original_invoice_line_ref"] = row.get("original_invoice_line_ref", "").strip()
            keys = BUSINESS_KEYS[feed]
            row["source_record_id"] = ":".join(row[key] for key in keys[1:])
            all_rows.append(row)

    columns = sorted({column for row in all_rows for column in row})
    return columns, all_rows
