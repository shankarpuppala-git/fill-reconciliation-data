import logging
from datetime import date, datetime, timedelta

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
    logger.info("Reconciliation request received | start_date=%s | end_date=%s", start_date, end_date)

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

    logger.info("Date range validation passed | days=%s | max_allowed=%s", date_range_days, MAX_DATE_RANGE_DAYS)

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

    # =====================================================
    # STEP 1: DB QUERIES (SOURCE OF TRUTH)
    # =====================================================
    logger.info("Running DB queries for date range")

    sales_orders = db_queries.fetch_sales_orders(start_date, end_date)
    logger.info("Sales orders fetched | count=%s", len(sales_orders))

    order_items = db_queries.fetch_order_items(start_date, end_date)
    logger.info("Order items fetched | count=%s", len(order_items))

    asn_rows = db_queries.fetch_asn_process_numbers(start_date, end_date)
    asn_process_numbers = [r["process_number"] for r in asn_rows]
    logger.info("ASN process numbers fetched | count=%s", len(asn_process_numbers))

    order_totals = db_queries.fetch_order_totals(asn_process_numbers)
    logger.info("Order totals fetched | count=%s", len(order_totals))

    # =====================================================
    # MERGE: Add fulfillment_status to sales_orders
    # =====================================================
    order_status_lookup = {}
    for item in order_items:
        order_id = item["order_process_number"]
        if order_id not in order_status_lookup:
            order_status_lookup[order_id] = item["order_status"]

    for order in sales_orders:
        order_id = order["process_number"]
        order["fulfillment_status"] = order_status_lookup.get(order_id, None)

    logger.info("Merged order items into sales orders | orders_with_items=%s",
                sum(1 for o in sales_orders if o.get("fulfillment_status")))

    # =====================================================
    # STEP 2: READ CSV FILES (RAW ROWS)
    # =====================================================
    def read_csv(upload: UploadFile):
        if not upload:
            return []

        upload.file.seek(0)
        content = upload.file.read().decode("utf-8").splitlines()
        import csv
        return list(csv.DictReader(content))

    converge_current_rows = read_csv(current_batch_csv)
    converge_settled_rows = read_csv(settled_batch_csv)

    logger.info(
        "CSV files read | current_rows=%s | settled_rows=%s",
        len(converge_current_rows),
        len(converge_settled_rows)
    )

    # =====================================================
    # STEP 3: CALL SERVICE LAYER (PURE LOGIC)
    # =====================================================
    reconciliation_result = ReconciliationService.run_reconciliation(
        cxp_orders=sales_orders,
        converge_current_rows=converge_current_rows,
        converge_settled_rows=converge_settled_rows
    )

    classification = reconciliation_result["classification"]
    converge_current_result = reconciliation_result.get("converge_current_result")
    converge_settled_result = reconciliation_result["converge_settled_result"]


    # =====================================================
    # STEP 4: EXCEL GENERATION (PRESENTATION)
    # =====================================================
    # Use start_date for filename
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

    # Sheet 2: Converge with highlighting
    writer.create_converge_sheet(
        converge_rows=converge_current_rows,
        converge_current=converge_current_result,
        sales_orders=sales_orders
    )

    # Sheet 3: Converge Settled with highlighting
    writer.create_converge_settled_sheet(converge_settled_rows)

    # Sheet 4: Orders Shipped with ASN vs Settled comparison
    writer.create_orders_shipped_sheet(
        asn_process_numbers=asn_process_numbers,
        order_totals=order_totals,
        converge_settled=converge_settled_result
    )

    # Sheet 5: RECONCILIATION REPORT
    writer.create_reconciliation_sheet(
        cxp_orders=sales_orders,
        classification=classification,
        converge_current=converge_current_result,
        converge_settled=converge_settled_result,
        asn_process_numbers=asn_process_numbers,
        order_totals=order_totals
    )

    excel_io = writer.to_bytes()

    if start_date == end_date:
        filename = f"Reconciliation_{start_date}.xlsx"
    else:
        filename = f"Reconciliation_{start_date}_to_{end_date}.xlsx"

    encoded_filename = quote(filename)




    logger.info("Reconciliation completed successfully | start_date=%s | end_date=%s", start_date, end_date)

    return Response(
        content=excel_io.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{encoded_filename}"',
            "Content-Length": str(len(excel_io.getvalue()))
        }
    )
