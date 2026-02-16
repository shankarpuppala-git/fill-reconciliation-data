import logging
from datetime import date, datetime, timedelta
import csv

from fastapi import APIRouter, Form, UploadFile, File, HTTPException
from fastapi.responses import Response
from urllib.parse import quote

from app.common import db_queries
from app.service.reconciliation_service import ReconciliationService
from app.sheets.workbook_writer import ReconciliationWorkbookWriter

router = APIRouter()
logger = logging.getLogger("controller.reconciliation")

# =====================================================
# CONFIGURATION: Max date range allowed (in days)
# =====================================================
MAX_DATE_RANGE_DAYS = 31  # Change this value to increase/decrease allowed range


@router.post("/run")
async def run_reconciliation(
        start_date: str = Form(...),
        end_date: str = Form(...),
        current_batch_csv: UploadFile = File(None),
        settled_batch_csv: UploadFile = File(None),
        notification_emails: str = Form(None)  # Optional: comma-separated emails
):
    logger.info("=" * 80)
    logger.info("RECONCILIATION REQUEST | start=%s | end=%s", start_date, end_date)
    logger.info("=" * 80)

    # =====================================================
    # STEP 0: VALIDATIONS
    # =====================================================

    # ---------- Date Format Validation ----------
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid start_date format. Expected YYYY-MM-DD"
        )

    try:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid end_date format. Expected YYYY-MM-DD"
        )

    # ---------- Date Range Validation ----------
    if start_dt > date.today():
        raise HTTPException(
            status_code=400,
            detail="start_date cannot be a future date"
        )

    if end_dt > date.today() + timedelta(days=1):
        raise HTTPException(
            status_code=400,
            detail="end_date cannot be a future date (can be tomorrow at most)"
        )

    if end_dt < start_dt:
        raise HTTPException(
            status_code=400,
            detail="end_date cannot be earlier than start_date"
        )

    if end_dt == start_dt:
        raise HTTPException(
            status_code=400,
            detail="end_date must be at least 1 day after start_date (for single day report, use start_date='2026-02-11' and end_date='2026-02-12')"
        )

    # Calculate date range in days
    date_range_days = (end_dt - start_dt).days

    if date_range_days > MAX_DATE_RANGE_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Date range cannot exceed {MAX_DATE_RANGE_DAYS} days. Requested range: {date_range_days} days. "
                   f"Please reduce the date range or contact administrator to increase the limit."
        )

    logger.info("✓ Date validation passed | range=%s days | max_allowed=%s", date_range_days, MAX_DATE_RANGE_DAYS)

    # ---------- File Presence Validation ----------
    if not current_batch_csv and not settled_batch_csv:
        raise HTTPException(
            status_code=400,
            detail="At least one CSV file must be provided"
        )

    # ---------- File Validation Helper ----------
    async def validate_csv_file(file: UploadFile, file_name: str):
        if not file:
            return

        if not file.filename.lower().endswith(".csv"):
            raise HTTPException(
                status_code=400,
                detail=f"{file_name} must be a CSV file"
            )

        contents = await file.read()
        if not contents:
            raise HTTPException(
                status_code=400,
                detail=f"{file_name} is empty"
            )

        # Reset file pointer for later reading
        file.file.seek(0)

    # ---------- Validate Files ----------
    await validate_csv_file(current_batch_csv, "current_batch_csv")
    await validate_csv_file(settled_batch_csv, "settled_batch_csv")

    logger.info("✓ CSV files validated")

    # =====================================================
    # STEP 1: DATABASE QUERIES (SOURCE OF TRUTH)
    # =====================================================
    logger.info("Starting database queries...")

    # 1. Fetch sales orders
    sales_orders = db_queries.fetch_sales_orders(start_date, end_date)
    logger.info("✓ Sales orders fetched | count=%s", len(sales_orders))

    if not sales_orders:
        logger.warning("No sales orders found for date range")
        raise HTTPException(
            status_code=404,
            detail=f"No sales orders found between {start_date} and {end_date}"
        )

    # 2. Fetch order items
    order_items = db_queries.fetch_order_items(start_date, end_date)
    logger.info("✓ Order items fetched | count=%s", len(order_items))

    # 3. ✅ FIXED: Fetch order totals for SALES ORDERS (not ASN!)
    sales_order_process_numbers = [order['process_number'] for order in sales_orders]
    logger.info("Extracted %s unique process numbers from sales orders", len(set(sales_order_process_numbers)))

    order_totals = db_queries.fetch_order_totals(sales_order_process_numbers)
    logger.info("✓ Order totals fetched | count=%s", len(order_totals))

    # Create order totals map with null handling
    order_totals_map = {
        item['process_number']: item['order_total']
        for item in order_totals
        if item.get('order_total') is not None
    }

    # Calculate and log coverage
    total_orders = len(sales_orders)
    orders_with_totals = len(order_totals_map)
    coverage_pct = (orders_with_totals / total_orders * 100) if total_orders > 0 else 0

    logger.info(
        f"Order total coverage: {coverage_pct:.1f}% "
        f"({orders_with_totals}/{total_orders} orders)"
    )

    if coverage_pct < 95:
        missing_count = total_orders - orders_with_totals
        logger.warning(
            f"⚠️ {missing_count} orders missing order_total - "
            f"settlement amount validation will be skipped for these orders"
        )

    # 4. Fetch ASN process numbers (for shipping validation in sheets)
    asn_rows = db_queries.fetch_asn_process_numbers(start_date, end_date)
    asn_process_numbers = [r["process_number"] for r in asn_rows]
    logger.info("✓ ASN process numbers fetched | count=%s", len(asn_process_numbers))

    # =====================================================
    # STEP 2: MERGE ORDER DATA
    # =====================================================
    logger.info("Merging order items with sales orders...")

    # Build order status lookup
    order_status_lookup = {}
    for item in order_items:
        order_id = item["order_process_number"]
        # Keep the "highest" status if multiple items exist
        current_status = order_status_lookup.get(order_id)
        new_status = item["order_status"]

        # Priority: SHIPPED > CLAIMED > ORDERED
        if current_status is None:
            order_status_lookup[order_id] = new_status
        elif current_status != "SHIPPED" and new_status == "SHIPPED":
            order_status_lookup[order_id] = new_status
        elif current_status == "ORDERED" and new_status in ("CLAIMED", "SHIPPED"):
            order_status_lookup[order_id] = new_status

    # Add fulfillment_status to each order
    orders_with_status_count = 0
    for order in sales_orders:
        order_id = order["process_number"]
        fulfillment_status = order_status_lookup.get(order_id)
        order["fulfillment_status"] = fulfillment_status
        if fulfillment_status:
            orders_with_status_count += 1

    logger.info(
        "✓ Order merge completed | orders_with_items=%s/%s (%.1f%%)",
        orders_with_status_count,
        len(sales_orders),
        (orders_with_status_count / len(sales_orders) * 100) if sales_orders else 0
    )

    # =====================================================
    # STEP 3: READ CSV FILES
    # =====================================================
    logger.info("Reading CSV files...")

    def read_csv_file(upload: UploadFile, file_label: str):
        """Read and parse CSV file with error handling."""
        if not upload:
            logger.info(f"No {file_label} file provided")
            return []

        try:
            upload.file.seek(0)
            content = upload.file.read().decode("utf-8-sig").splitlines()  # utf-8-sig handles BOM
            rows = list(csv.DictReader(content))
            logger.info(f"✓ {file_label} parsed | rows=%s", len(rows))
            return rows
        except UnicodeDecodeError as e:
            logger.error(f"Failed to decode {file_label}: {e}")
            raise HTTPException(
                status_code=400,
                detail=f"{file_label} encoding error. Please ensure file is UTF-8 encoded."
            )
        except csv.Error as e:
            logger.error(f"Failed to parse {file_label}: {e}")
            raise HTTPException(
                status_code=400,
                detail=f"{file_label} is not a valid CSV file: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Unexpected error reading {file_label}: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to read {file_label}: {str(e)}"
            )

    converge_current_rows = read_csv_file(current_batch_csv, "Converge CURRENT batch")
    converge_settled_rows = read_csv_file(settled_batch_csv, "Converge SETTLED batch")

    # =====================================================
    # STEP 4: RUN RECONCILIATION SERVICE
    # =====================================================
    logger.info("Starting reconciliation service...")

    try:
        reconciliation_result = ReconciliationService.run_reconciliation(
            cxp_orders=sales_orders,
            converge_current_rows=converge_current_rows,
            converge_settled_rows=converge_settled_rows,
            order_totals=order_totals_map  # ✅ Now has correct data!
        )
    except Exception as e:
        logger.error(f"Reconciliation service failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Reconciliation failed: {str(e)}"
        )

    classification = reconciliation_result["classification"]
    converge_current_result = reconciliation_result.get("converge_current_result")
    converge_settled_result = reconciliation_result["converge_settled_result"]

    # =====================================================
    # STEP 5: LOG SUMMARY STATISTICS
    # =====================================================
    logger.info("=" * 80)
    logger.info("RECONCILIATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total Orders: {len(sales_orders)}")
    logger.info(f"Success: {len(classification.get('successful_orders', []))}")
    logger.info(f"Failed: {len(classification.get('failed_orders', []))}")
    logger.info(f"Risky (Action Required): {len(classification.get('action_required_orders', []))}")
    logger.info(f"Retry Successes: {len(classification.get('retry_success_orders', []))}")
    logger.info(f"Data Inconsistencies: {len(classification.get('converge_data_inconsistencies', []))}")
    logger.info(f"Settlement Amount Mismatches: {len(classification.get('settlement_amount_mismatches', []))}")

    # Highlight critical issues
    action_required = classification.get('action_required_orders', [])
    if action_required:
        logger.warning(f"⚠️ {len(action_required)} orders require manual attention!")

    settlement_mismatches = classification.get('settlement_amount_mismatches', [])
    if settlement_mismatches:
        logger.warning(f"⚠️ {len(settlement_mismatches)} orders have settlement amount mismatches!")
        # Log first few examples
        for mismatch in settlement_mismatches[:3]:
            logger.warning(
                f"  - {mismatch['order_id']}: "
                f"Order=${mismatch['order_total']:.2f} vs "
                f"Settled=${mismatch['settled_amount']:.2f}"
            )

    logger.info("=" * 80)

    # =====================================================
    # STEP 6: EXCEL GENERATION
    # =====================================================
    logger.info("Generating Excel report...")

    try:
        writer = ReconciliationWorkbookWriter(start_date)

        # Sheet 1: CXP with highlighting
        writer.create_cxp_sheet(
            sales_orders=sales_orders,
            order_items=order_items,
            classification=classification,
            converge_current=converge_current_result,
            converge_settled=converge_settled_result,
            asn_process_numbers=asn_process_numbers
        )

        # Sheet 2: Converge CURRENT with highlighting
        writer.create_converge_sheet(
            converge_rows=converge_current_rows,
            converge_current=converge_current_result,
            sales_orders=sales_orders
        )

        # Sheet 3: Converge SETTLED with highlighting
        writer.create_converge_settled_sheet(converge_settled_rows)

        # Sheet 4: Orders Shipped with ASN vs Settled comparison
        writer.create_orders_shipped_sheet(
            asn_process_numbers=asn_process_numbers,
            order_totals=order_totals,
            converge_settled=converge_settled_result
        )

        # Sheet 5: RECONCILIATION REPORT (Main summary)
        writer.create_reconciliation_sheet(
            cxp_orders=sales_orders,
            classification=classification,
            converge_current=converge_current_result,
            converge_settled=converge_settled_result,
            asn_process_numbers=asn_process_numbers,
            order_totals=order_totals
        )

        excel_io = writer.to_bytes()

        logger.info("✓ Excel report generated | size=%s KB", len(excel_io.getvalue()) // 1024)

    except Exception as e:
        logger.error(f"Excel generation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Excel generation failed: {str(e)}"
        )

    # =====================================================
    # STEP 7: PREPARE RESPONSE
    # =====================================================

    # Generate filename
    if start_date == end_date:
        filename = f"Reconciliation_{start_date}.xlsx"
    else:
        filename = f"Reconciliation_{start_date}_to_{end_date}.xlsx"

    encoded_filename = quote(filename)

    logger.info("=" * 80)
    logger.info("RECONCILIATION COMPLETED SUCCESSFULLY")
    logger.info(f"Report: {filename}")
    logger.info("=" * 80)

    return Response(
        content=excel_io.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{encoded_filename}"',
            "Content-Length": str(len(excel_io.getvalue()))
        }
    )