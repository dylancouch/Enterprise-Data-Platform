"""Local browser interface for importing business CSVs and viewing reconciliations."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import uuid
from decimal import Decimal
from pathlib import Path

import streamlit as st

from data_pipelines.business_sample import SAMPLE_COMPANY_ID, SAMPLE_EXPORT_HASHES, write_sample
from data_pipelines.construction_generator import generate
from data_pipelines.construction_pipeline import run_pipeline
from data_pipelines.intake import (
    FEEDS,
    FIELD_LABELS,
    normalize_feed,
    read_csv_upload,
    suggest_column,
)

CORE_FEEDS = (
    "purchase_order_lines", "scale_tickets", "site_receipts", "invoices", "invoice_lines"
)

EXCEPTION_HELP = {
    "ALLOCATION_QUANTITY_MISMATCH": (
        "The quantity assigned to deliveries differs from the invoice quantity.",
        "Check the invoice quantity and the delivery links for this invoice line.",
    ),
    "CANCELLED_DELIVERY": (
        "This invoice line is linked to a cancelled delivery.",
        "Confirm whether the delivery was cancelled or the invoice points to the wrong delivery.",
    ),
    "CREDIT_MISSING_ORIGINAL_REFERENCE": (
        "The credit does not identify the invoice line it corrects.",
        "Add the original supplier invoice and line reference to the credit export.",
    ),
    "CREDIT_ORIGINAL_LINE_NOT_FOUND": (
        "The original invoice line named by this credit was not found in the selected files.",
        "Include the original invoice line or correct the reference, then import again.",
    ),
    "LINE_AMOUNT_MISMATCH": (
        "The invoice amount does not match its quantity multiplied by its unit price.",
        "Check the invoice quantity, rate, units, and line amount in the source export.",
    ),
    "MATERIAL_MISMATCH": (
        "The invoiced item differs from the item on the linked purchase order.",
        "Check the item code on the invoice and the linked purchase order line.",
    ),
    "MISSING_ALLOCATION": (
        "No delivery is linked to this invoice line.",
        "Add an invoice-to-delivery link if available. The report does not guess which delivery was billed.",
    ),
    "MISSING_INVOICE_HEADER": (
        "The invoice line has no matching invoice header in the selected files.",
        "Include the invoice header or correct the supplier and invoice IDs on the line.",
    ),
    "PENDING_SUPPORTING_DELIVERY": (
        "A linked delivery does not have an accepted receipt yet.",
        "Confirm the site receipt or update the invoice-to-delivery link, then import the latest data.",
    ),
    "PO_LINE_MISMATCH": (
        "The linked delivery and invoice point to different purchase order lines.",
        "Check the purchase order and line references in the delivery and allocation exports.",
    ),
    "QUANTITY_MISMATCH": (
        "The invoiced quantity differs from the accepted quantity on the linked deliveries.",
        "Compare the invoice quantity with the accepted delivery records and correct the source or links.",
    ),
    "SUPPLIER_MISMATCH": (
        "The invoice supplier differs from the supplier on the purchase order.",
        "Check the supplier IDs on the invoice and purchase order exports.",
    ),
    "UNIT_PRICE_MISMATCH": (
        "The invoice unit price differs from the purchase order price.",
        "Check the invoice rate, purchase order rate, and price units.",
    ),
    "UNSUPPORTED_CURRENCY": (
        "This report can compare USD invoices only.",
        "Use a USD export; this workflow does not convert currencies.",
    ),
    "UNSUPPORTED_DOCUMENT_TYPE": (
        "This invoice line is marked as an unsupported document type.",
        "Mark the row as an invoice or credit, or remove it from this export.",
    ),
    "UNSUPPORTED_INVOICE_UNIT": (
        "The invoice uses a quantity or price unit this report cannot convert.",
        "Use kg, metric tonnes, or US short tons, and confirm both quantity and price units.",
    ),
    "UNSUPPORTED_ALLOCATION_UNIT": (
        "The delivery link uses a unit this report cannot convert.",
        "Use kg, metric tonnes, or US short tons for the allocated quantity.",
    ),
}

QUARANTINE_HELP = {
    "GROSS_BELOW_TARE": "Gross weight is less than tare weight. Check the scale ticket values.",
    "INVALID_EVENT_TIMESTAMP": "The ticket date could not be read. Use an ISO date or timestamp.",
    "INVALID_WEIGHT": "Gross or tare weight is not a number. Correct the ticket values and import again.",
    "NEGATIVE_WEIGHT": "A scale ticket has a negative weight. Check the source record.",
    "UNKNOWN_WEIGHT_UNIT": "The weight unit is not supported. Use kg, metric tonnes, or US short tons.",
    "INVALID_ACCEPTANCE_STATUS": "The receipt status is not recognized. Use accepted, rejected, or pending.",
    "INVALID_ACCEPTED_FRACTION": "The accepted fraction must be a number between 0 and 1.",
    "INVALID_RECEIVED_TIMESTAMP": "The received date could not be read. Use an ISO date or timestamp.",
    "UNSUPPORTED_FRACTIONAL_ACCEPTANCE": "Partial receipt acceptance is not supported; mark the receipt accepted or rejected.",
    "INVALID_INVOICE_AMOUNT_OR_QUANTITY": "An invoice amount, quantity, or rate is not a valid number.",
    "NEGATIVE_INVOICE_AMOUNT_OR_QUANTITY": "An invoice amount, quantity, or rate is negative. Check the source row.",
    "INVALID_ALLOCATION_QUANTITY": "The allocated delivery quantity is not a valid number.",
    "NEGATIVE_ALLOCATION_QUANTITY": "The allocated delivery quantity is negative. Check the source row.",
    "UNKNOWN_ALLOCATION_UNIT": "The delivery link uses a unit this workflow cannot convert. Use kg, metric tonnes, or US short tons.",
    "UNKNOWN_INVOICE_PRICE_UNIT": "The invoice price unit is not supported. Use kg, metric tonnes, or US short tons.",
    "UNKNOWN_INVOICE_QUANTITY_UNIT": "The invoice quantity unit is not supported. Use kg, metric tonnes, or US short tons.",
    "INVALID_CREDIT_MAGNITUDE": "Credit quantities and amounts must be positive in the source file; the report applies the credit as a reduction.",
    "UNSUPPORTED_SCHEMA_VERSION": "This row uses a data format version this workflow cannot read.",
}


def _friendly_exception_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    readable = []
    for row in rows:
        explanation, action = EXCEPTION_HELP.get(
            str(row.get("exception_reason")),
            ("This invoice line needs review.", "Open the downloaded details and compare the source records."),
        )
        readable.append({
            "What needs review": explanation,
            "Suggested next step": action,
            "Supplier": row.get("supplier_id"),
            "Invoice": row.get("invoice_id"),
            "Invoice line": row.get("invoice_line_id"),
            "Purchase order": row.get("po_id"),
            "PO line": row.get("po_line_id"),
            "Item": row.get("material_id"),
            "Invoice quantity": row.get("billed_quantity"),
            "Accepted delivery quantity (metric tonnes)": row.get("allocated_receipt_quantity"),
            "Invoice amount": row.get("line_amount"),
            "Source file": row.get("imported_file"),
            "Delivery files": row.get("delivery_source_files"),
            "Allocation files": row.get("allocation_source_files"),
        })
    return readable


def _friendly_quarantine_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    readable = []
    for row in rows:
        readable.append({
            "Why this row was set aside": QUARANTINE_HELP.get(
                str(row.get("quarantine_reason")),
                "The row contains a value this workflow cannot use. Check the source row and correct it.",
            ),
            "Data type": FEEDS.get(str(row.get("feed")), FEEDS["scale_tickets"]).title,
            "Record": row.get("source_record_id"),
            "Ticket": row.get("ticket_id"),
            "Delivery": row.get("delivery_id"),
            "Value": row.get("weight_unit") or row.get("acceptance_status") or row.get("allocated_quantity"),
            "Source file": row.get("imported_file"),
        })
    return readable


def _show_processing_error(message: str, error: Exception) -> None:
    st.error(message)
    with st.expander("More detail"):
        st.code(str(error))


def _mapping_ui(feed: str, headers: list[str]) -> dict[str, str]:
    spec = FEEDS[feed]
    mappings: dict[str, str] = {}
    st.caption("Choose which column in this file contains each value. Suggested matches are preselected.")
    for field in (*spec.required, *spec.optional):
        suggested = suggest_column(field, headers)
        options = ["Not provided", *(header for header in headers if header not in mappings)]
        default_index = options.index(suggested) if suggested in headers else 0
        if suggested not in options:
            default_index = 0
        chosen = st.selectbox(
            f"{FIELD_LABELS.get(field, field)}{' *' if field in spec.required else ''}",
            options,
            index=default_index,
            key=f"mapping_{feed}_{field}",
        )
        if chosen != "Not provided":
            mappings[chosen] = field
    return mappings


def _write_normalized_feed(root: Path, feed: str, columns: list[str], rows: list[dict[str, str]]) -> None:
    path = root / "landing" / f"{feed}_user_import.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _run_from_sample(scenario: str) -> tuple[Path, dict[str, object]]:
    root = Path("data/user-runs") / f"sample-{uuid.uuid4().hex[:10]}"
    sample_source = root / "sample-source"
    if scenario == "representative-business-data":
        write_sample(sample_source)
        source_files = sample_source / "exports"
        company_id = SAMPLE_COMPANY_ID
    else:
        generate(sample_source, scenario)
        source_files = sample_source / "landing"
        company_id = "NORTHSTAR-CIVIL"
    for feed, spec in FEEDS.items():
        paths = sorted(source_files.glob(f"{feed}*.csv"))
        if not paths:
            continue
        headers, _ = read_csv_upload(paths[0].read_bytes())
        mappings = {
            header: field
            for field in (*spec.required, *spec.optional)
            if (header := suggest_column(field, headers)) is not None
        }
        columns, rows = normalize_feed(
            feed,
            (path.read_bytes() for path in paths),
            mappings,
            company_id,
            filenames=(path.name for path in paths),
        )
        _write_normalized_feed(root, feed, columns, rows)
    (root / "source_metadata.json").write_text(
        json.dumps({"synthetic": True, "source": "sample", "scenario": scenario}, indent=2) + "\n",
        encoding="utf-8",
    )
    return root, run_pipeline(root)


def _run_from_uploads(company_id: str, files: dict[str, list[object]], mappings: dict[str, dict[str, str]]) -> tuple[Path, dict[str, object]]:
    root = Path("data/user-runs") / f"business-{uuid.uuid4().hex[:10]}"
    root.mkdir(parents=True, exist_ok=False)
    imported: dict[str, int] = {}
    all_files_are_sample_data = True
    for feed, uploaded in files.items():
        if not uploaded:
            continue
        payloads = [item.getvalue() for item in uploaded]
        all_files_are_sample_data = all_files_are_sample_data and all(
            hashlib.sha256(payload).hexdigest() == SAMPLE_EXPORT_HASHES.get(feed)
            for payload in payloads
        )
        columns, rows = normalize_feed(
            feed,
            payloads,
            mappings[feed],
            company_id,
            filenames=(Path(item.name.replace("\\", "/")).name for item in uploaded),
        )
        _write_normalized_feed(root, feed, columns, rows)
        imported[feed] = len(rows)
    is_sample_upload = bool(imported) and all_files_are_sample_data
    (root / "source_metadata.json").write_text(
        json.dumps({
            "synthetic": is_sample_upload,
            "source": "representative_sample_csv_upload" if is_sample_upload else "user_csv_import",
            "rows_by_feed": imported,
        }, indent=2) + "\n",
        encoding="utf-8",
    )
    return root, run_pipeline(root)


def _csv_bytes(rows: list[dict[str, object]]) -> bytes:
    if not rows:
        return b""
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def _show_result(report: dict[str, object]) -> None:
    st.subheader("Reconciliation results")
    st.success("Your reconciliation is ready. Review the delivered quantities, invoice totals, and items that need attention below.")
    daily = report.get("daily_delivered", [])
    accepted = sum((Decimal(str(row.get("accepted_net_quantity", 0))) for row in daily), Decimal(0))
    exceptions = report.get("invoice_exceptions", [])
    invoice_total = sum(
        (Decimal(str(row.get("net_invoice_amount", 0))) for row in report.get("invoice_net_amounts", [])),
        Decimal(0),
    )
    first, second, third, fourth, fifth = st.columns(5)
    first.metric("Accepted delivered quantity", f"{accepted:,.3f} metric tonnes")
    second.metric("Net invoice amount", f"${invoice_total:,.2f}")
    third.metric("Invoice items to review", len(exceptions))
    fourth.metric("Duplicate rows ignored", int(report.get("duplicate_business_envelopes_suppressed", 0)))
    fifth.metric("Rows set aside", int(report.get("quarantine_rows", 0)))

    if exceptions:
        st.markdown("#### Items to review")
        st.dataframe(_friendly_exception_rows(exceptions[:200]), hide_index=True, use_container_width=True)
        if len(exceptions) > 200:
            st.caption("Showing the first 200 exceptions. Download the CSV for the full list.")
        st.download_button(
            "Download exception details (CSV)",
            _csv_bytes(exceptions),
            file_name="invoice-exceptions.csv",
            mime="text/csv",
        )
    else:
        st.info("No invoice exceptions were found in the selected data.")

    freshness = report.get("source_freshness", [])
    if freshness:
        with st.expander("Data availability and freshness"):
            freshness_labels = {
                "RECEIVED": "File received",
                "MISSING_EXPECTED_FILE": "No file selected",
                "MISSING_OR_INVALID_SOURCE_UPDATED_AT": "No valid source update time",
            }
            readable_freshness = []
            for row in freshness:
                status = str(row.get("freshness_status", ""))
                if status.startswith("STALE_SOURCE_UPDATE_OVER_"):
                    age_hours = status.removeprefix("STALE_SOURCE_UPDATE_OVER_").removesuffix("H")
                    status_text = f"Latest source update is over {age_hours} hours old"
                else:
                    status_text = freshness_labels.get(status, "Check source file")
                readable_freshness.append({
                    "Data type": FEEDS.get(str(row.get("source_feed")), FEEDS["purchase_order_lines"]).title,
                    "Files selected": row.get("files_received_this_run"),
                    "Rows read": row.get("rows_received_this_run"),
                    "Latest source update": row.get("latest_source_updated_at_utc"),
                    "Availability": status_text,
                })
            st.dataframe(readable_freshness, hide_index=True, use_container_width=True)
    quarantined = report.get("quarantined_records", [])
    if report.get("quarantine_rows", 0):
        st.warning(
            f"{report['quarantine_rows']} row(s) were set aside because their values could not be used. "
            "Review the reason and source record, correct the source export, then import it again."
        )
        if quarantined:
            with st.expander("Rows set aside and why"):
                st.dataframe(_friendly_quarantine_rows(quarantined), hide_index=True, use_container_width=True)
                if report["quarantine_rows"] > len(quarantined):
                    st.caption("Showing the first 200 set-aside rows. The report download includes the displayed detail.")
    st.download_button(
        "Download reconciliation report (JSON)",
        json.dumps(report, indent=2, default=str).encode("utf-8"),
        file_name="reconciliation-report.json",
        mime="application/json",
    )
    st.caption("This run and its source files are saved on this computer.")
    if not report.get("synthetic", True):
        st.caption("This report uses business data you imported. Input files and results remain on this computer.")
    else:
        st.caption("This report uses synthetic example data.")


def main() -> None:
    st.set_page_config(page_title="Business Data Reconciliation", page_icon="📊", layout="wide")
    st.title("Business data reconciliation")
    st.write(
        "Compare what your business ordered, received, and was invoiced for. "
        "Choose example data to explore the workflow or import your own CSV exports."
    )
    st.info("This local app processes files on this computer. It does not send your data to a service.")

    start_with = st.radio(
        "What data would you like to use?",
        ("Try example data", "Use my business data"),
        horizontal=True,
    )

    if start_with == "Try example data":
        scenario_names = {
            "Company purchasing reconciliation (recommended)": "representative-business-data",
            "Small reliability example": "golden",
            "A delivery receipt arrives late": "late-receipt",
            "A delivery ticket is corrected": "ticket-correction",
            "An unsupported measurement unit needs review": "invalid-unit",
        }
        scenario_label = st.selectbox("Choose an example", list(scenario_names))
        st.caption("Examples contain fictional business records and are labeled synthetic in the report.")
        st.caption("The recommended company example uses export-style column names. The app suggests mappings, then applies the same field checks and reconciliation used for imported files.")
        if st.button("Create reconciliation report", type="primary"):
            try:
                with st.spinner("Preparing example data and reconciling it…"):
                    _root, report = _run_from_sample(scenario_names[scenario_label])
                st.session_state["latest_result"] = report
            except Exception as exc:  # noqa: BLE001 - show local pipeline errors in the app.
                _show_processing_error(
                    "The example could not be processed. Try again, and review the details if the problem continues.",
                    exc,
                )

    else:
        company_id = st.text_input("Business name or ID", placeholder="For example, Northside Supply")
        st.write("Upload one or more CSV exports for each type of data. Files within a type must share the same columns.")
        st.caption("Purchase orders, delivery tickets, receipts, invoice headers, and invoice lines are required. "
                   "Invoice-to-delivery links are optional; without them, invoice quantities cannot be matched to deliveries.")

        selected_files: dict[str, list[object]] = {}
        selected_mappings: dict[str, dict[str, str]] = {}
        upload_errors: list[str] = []
        mapping_errors: list[str] = []
        for feed, spec in FEEDS.items():
            required_label = "Required" if feed in CORE_FEEDS else "Optional"
            st.markdown(f"#### {spec.title} · {required_label}")
            uploaded = st.file_uploader(
                f"Select {spec.title.lower()} CSV file(s)",
                type=["csv"],
                accept_multiple_files=True,
                key=f"upload_{feed}",
                max_upload_size=50,
            )
            selected_files[feed] = uploaded
            if not uploaded:
                continue
            try:
                headers, preview = read_csv_upload(uploaded[0].getvalue())
                if any(set(read_csv_upload(item.getvalue())[0]) != set(headers) for item in uploaded[1:]):
                    raise ValueError("Files for this data type need the same column names.")
                with st.expander(f"Preview {spec.title.lower()} and map columns", expanded=True):
                    st.dataframe({header: [row.get(header, "") for row in preview[:5]] for header in headers},
                                 hide_index=True, use_container_width=True)
                    selected_mappings[feed] = _mapping_ui(feed, headers)
                    missing_fields = [
                        field for field in spec.required if field not in selected_mappings[feed].values()
                    ]
                    if missing_fields:
                        labels = ", ".join(FIELD_LABELS.get(field, field) for field in missing_fields)
                        mapping_errors.append(f"{spec.title}: {labels}")
                        st.warning(f"Map these required values before continuing: {labels}.")
            except ValueError as exc:
                upload_errors.append(f"{spec.title}: {exc}")
                st.error(str(exc))

        missing_feeds = [FEEDS[feed].title for feed in CORE_FEEDS if not selected_files[feed]]
        if missing_feeds:
            st.warning("Still needed: " + ", ".join(missing_feeds) + ".")
        if upload_errors:
            st.error("Fix the file issues above before creating a report.")
        if mapping_errors:
            st.error("Some required fields still need a column mapping.")
        if st.button("Create reconciliation report from my files", type="primary",
                     disabled=bool(missing_feeds or upload_errors or mapping_errors or not company_id.strip())):
            try:
                with st.spinner("Checking your files and reconciling the data…"):
                    _root, report = _run_from_uploads(company_id, selected_files, selected_mappings)
                st.session_state["latest_result"] = report
            except Exception as exc:  # noqa: BLE001 - show local pipeline errors in the app.
                _show_processing_error(
                    "The files could not be reconciled. Correct the source data if needed, then import the files again.",
                    exc,
                )

    latest = st.session_state.get("latest_result")
    if latest:
        st.divider()
        _show_result(latest)


if __name__ == "__main__":
    main()
