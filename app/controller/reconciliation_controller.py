import logging
import csv
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Form, UploadFile, File, HTTPException
from fastapi.responses import Response
from urllib.parse import quote

from app.common import db_queries
from app.service.reconciliation_service import ReconciliationService
from app.sheets.workbook_writer import ReconciliationWorkbookWriter

router = APIRouter()
logger = logging.getLogger("controller.reconciliation")

MAX_DATE_RANGE_DAYS = 31


@router.post("/run")
async def run_reconciliation(
        start_date: str = Form(...),
        end_date: str = Form(...),
        current_batch_csv: UploadFile = File(None),
        settled_batch_csv: UploadFile = File(None)
):
    logger.info("=" * 80)
    logger.info("RECONCILIATION REQUEST | start=%s | end=%s", start_date, end_date)
    logger.info("=" * 80)

    # =====================================================
    # STEP 0: VALIDATIONS
    # =====================================================

    # Date format
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid start_date format. Expected YYYY-MM-DD")

    try:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid end_date format. Expected YYYY-MM-DD")

    if start_dt > date.today():
        raise HTTPException(status_code=400, detail="start_date cannot be a future date")

    if end_dt > date.today() + timedelta(days=1):
        raise HTTPException(status_code=400, detail="end_date cannot be a future date")

    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end_date cannot be earlier than start_date")

    if end_dt == start_dt:
        raise HTTPException(
            status_code=400,
            detail=(
                "end_date must be at least 1 day after start_date. "
                "For a single-day report use start_date='2026-02-11' and end_date='2026-02-12'."
            )
        )

    date_range_days = (end_dt - start_dt).days
    if date_range_days > MAX_DATE_RANGE_DAYS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Date range cannot exceed {MAX_DATE_RANGE_DAYS} days. "
                f"Requested: {date_range_days} days."
            )
        )

    logger.info("✓ Date validation passed | range=%s days | max_allowed=%s", date_range_days, MAX_DATE_RANGE_DAYS)

    # File presence
    if not current_batch_csv and not settled_batch_csv:
        raise HTTPException(status_code=400, detail="At least one CSV file (current or settled) must be provided")

    # ── File validation helper ──────────────────────────────────────
    async def validate_csv_file(file: UploadFile, label: str):
        if not file:
            return
        if not file.filename.lower().endswith(".csv"):
            raise HTTPException(status_code=400, detail=f"{label} must be a .csv file")
        contents = await file.read()
        if not contents:
            raise HTTPException(status_code=400, detail=f"{label} is empty")
        file.file.seek(0)

    await validate_csv_file(current_batch_csv, "current_batch_csv")
    await validate_csv_file(settled_batch_csv, "settled_batch_csv")

    logger.info("✓ CSV files validated")

    # =====================================================
    # STEP 1: DATABASE QUERIES
    # =====================================================
    logger.info("Starting database queries...")

    sales_orders = db_queries.fetch_sales_orders(start_date, end_date)
    logger.info("✓ Sales orders fetched | count=%s", len(sales_orders))

    if not sales_orders:
        raise HTTPException(
            status_code=404,
            detail=f"No sales orders found between {start_date} and {end_date}"
        )

    order_items = db_queries.fetch_order_items(start_date, end_date)
    logger.info("✓ Order items fetched | count=%s", len(order_items))

    # Order totals (for amount validation)
    sales_order_pnums = [o["process_number"] for o in sales_orders]
    logger.info("Extracted %s unique process numbers from sales orders", len(set(sales_order_pnums)))

    order_totals_raw = db_queries.fetch_order_totals(sales_order_pnums)
    logger.info("✓ Order totals fetched | count=%s", len(order_totals_raw))

    order_totals_map = {
        item["process_number"]: item["order_total"]
        for item in order_totals_raw
        if item.get("order_total") is not None
    }

    total_orders      = len(sales_orders)
    orders_with_totals = len(order_totals_map)
    coverage_pct       = (orders_with_totals / total_orders * 100) if total_orders else 0
    logger.info("Order total coverage: %.1f%% (%s/%s)", coverage_pct, orders_with_totals, total_orders)

    if coverage_pct < 95:
        logger.warning(
            "⚠️  %s orders missing order_total — amount validation skipped for these",
            total_orders - orders_with_totals
        )

    # =====================================================
    # STEP 2: MERGE ORDER ITEMS → SALES ORDERS
    # =====================================================
    logger.info("Merging order items with sales orders...")

    # Priority: SHIPPED > CLAIMED > ORDERED
    _priority = {"SHIPPED": 3, "CLAIMED": 2, "ORDERED": 1}
    order_status_lookup = {}
    for item in order_items:
        oid        = item["order_process_number"]
        new_status = item["order_status"]
        current    = order_status_lookup.get(oid)
        if current is None or _priority.get(new_status, 0) > _priority.get(current, 0):
            order_status_lookup[oid] = new_status

    orders_with_status_count = 0
    for order in sales_orders:
        oid = order["process_number"]
        fs  = order_status_lookup.get(oid)
        order["fulfillment_status"] = fs
        if fs:
            orders_with_status_count += 1

    logger.info(
        "✓ Order merge completed | orders_with_items=%s/%s (%.1f%%)",
        orders_with_status_count, len(sales_orders),
        (orders_with_status_count / len(sales_orders) * 100) if sales_orders else 0
    )

    # =====================================================
    # STEP 3: READ CSV FILES
    # =====================================================
    logger.info("Reading CSV files...")

    def read_csv(upload: UploadFile, label: str) -> list:
        if not upload:
            logger.info("No %s file provided", label)
            return []
        try:
            upload.file.seek(0)
            content = upload.file.read().decode("utf-8-sig").splitlines()
            rows = list(csv.DictReader(content))
            logger.info("✓ %s parsed | rows=%s", label, len(rows))
            return rows
        except UnicodeDecodeError as e:
            logger.error("Failed to decode %s: %s", label, e)
            raise HTTPException(status_code=400, detail=f"{label} encoding error. Ensure file is UTF-8.")
        except Exception as e:
            logger.error("Unexpected error reading %s: %s", label, e)
            raise HTTPException(status_code=500, detail=f"Failed to read {label}: {e}")

    converge_current_rows = read_csv(current_batch_csv, "Converge CURRENT batch")
    converge_settled_rows = read_csv(settled_batch_csv, "Converge SETTLED batch")

    # Query 3: ASN process numbers — orders the warehouse physically shipped
    asn_rows_raw        = db_queries.fetch_asn_process_numbers(start_date, end_date)
    asn_process_numbers = [r["process_number"] for r in asn_rows_raw]
    logger.info("✓ ASN process numbers fetched from DB | count=%s", len(asn_process_numbers))

    # Query 4: Order totals specifically for ASN orders (ASN numbers are the input).
    # We need the CXP order_total for each shipped order to compare against
    # what Converge settled. This is a separate fetch from order_totals_raw
    # (which serves the classifier) — scope is ASN orders only.
    asn_order_totals = db_queries.fetch_order_totals(asn_process_numbers) if asn_process_numbers else []
    logger.info("✓ ASN order totals fetched | count=%s/%s",
                len(asn_order_totals), len(asn_process_numbers))

    # =====================================================
    # STEP 4: RUN RECONCILIATION SERVICE
    # =====================================================
    logger.info("Starting reconciliation service...")

    try:
        reconciliation_result = ReconciliationService.run_reconciliation(
            cxp_orders=sales_orders,
            converge_current_rows=converge_current_rows,
            converge_settled_rows=converge_settled_rows,
            order_totals=order_totals_map,
            asn_process_numbers=asn_process_numbers
        )
    except Exception as e:
        logger.error("Reconciliation service failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Reconciliation failed: {e}")

    classification          = reconciliation_result["classification"]
    converge_current_result = reconciliation_result["converge_current_result"]
    converge_settled_result = reconciliation_result["converge_settled_result"]

    # =====================================================
    # STEP 5: LOG SUMMARY
    # =====================================================
    logger.info("=" * 80)
    logger.info("RECONCILIATION SUMMARY")
    logger.info("=" * 80)
    logger.info("Total Orders:          %s", len(sales_orders))
    logger.info("Success:               %s", len(classification.get("successful_orders", [])))
    logger.info("Failed:                %s", len(classification.get("failed_orders", [])))
    logger.info("Action Required:       %s", len(classification.get("action_required_orders", [])))
    logger.info("Retry Successes:       %s", len(classification.get("retry_success_orders", [])))
    logger.info("Data Inconsistencies:  %s", len(classification.get("converge_data_inconsistencies", [])))
    logger.info("Amount Mismatches:     %s", len(classification.get("settlement_amount_mismatches", [])))

    action_required = classification.get("action_required_orders", [])
    if action_required:
        logger.warning("⚠️  %s orders require manual attention!", len(action_required))

    settlement_mismatches = classification.get("settlement_amount_mismatches", [])
    if settlement_mismatches:
        logger.warning("⚠️  %s settlement amount mismatches!", len(settlement_mismatches))
        for m in settlement_mismatches[:3]:
            logger.warning(
                "  - %s: Order=%.2f vs Settled=%.2f",
                m["order_id"], m["order_total"], m["settled_amount"]
            )

    logger.info("=" * 80)

    # =====================================================
    # STEP 6: EXCEL GENERATION
    # =====================================================
    logger.info("Generating Excel report...")

    try:
        writer = ReconciliationWorkbookWriter(start_date)

        # Sheet 1: Reconciliation (first tab)
        writer.create_reconciliation_sheet(
            cxp_orders=sales_orders,
            classification=classification,
            converge_current=converge_current_result,
            converge_settled=converge_settled_result,
            asn_process_numbers=asn_process_numbers,
            order_totals=order_totals_raw
        )

        # Sheet 2: CXP
        writer.create_cxp_sheet(
            sales_orders=sales_orders,
            order_items=order_items,
            classification=classification,
            converge_current=converge_current_result,
            converge_settled=converge_settled_result,
            asn_process_numbers=asn_process_numbers
        )

        # Sheet 3: Converge CURRENT
        writer.create_converge_sheet(
            converge_rows=converge_current_rows,
            converge_current=converge_current_result,
            sales_orders=sales_orders
        )

        # Sheet 4: Converge SETTLED
        writer.create_converge_settled_sheet(converge_settled_rows)

        # Sheet 5: Orders Shipped
        # asn_order_totals = fetch_order_totals(asn_process_numbers) — Query 4
        # Scope is ASN orders only, not all sales orders.
        writer.create_orders_shipped_sheet(
            asn_process_numbers=asn_process_numbers,
            asn_order_totals=asn_order_totals,
            converge_settled=converge_settled_result,
            classification=classification
        )

        # Sheet 6: Logs (everything that would have gone to the log file)
        writer.create_logs_sheet(
            start_date=start_date,
            end_date=end_date,
            sales_orders=sales_orders,
            order_items=order_items,
            asn_process_numbers=asn_process_numbers,
            asn_order_totals=asn_order_totals,
            order_totals=order_totals_raw,
            classification=classification,
            converge_current_result=converge_current_result,
            converge_settled_result=converge_settled_result
        )

        excel_io = writer.to_bytes()
        logger.info("✓ Excel report generated | size=%s KB", len(excel_io.getvalue()) // 1024)

    except Exception as e:
        logger.error("Excel generation failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Excel generation failed: {e}")

    # =====================================================
    # STEP 7: RESPONSE
    # =====================================================
    filename         = f"Reconciliation_{start_date}_to_{end_date}.xlsx"
    encoded_filename = quote(filename)

    logger.info("=" * 80)
    logger.info("RECONCILIATION COMPLETED SUCCESSFULLY")
    logger.info("Report: %s", filename)
    logger.info("=" * 80)

    return Response(
        content=excel_io.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{encoded_filename}"',
            "Content-Length": str(len(excel_io.getvalue()))
        }
    )