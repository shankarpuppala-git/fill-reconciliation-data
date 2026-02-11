import logging
from fastapi import APIRouter, Form, UploadFile, File
from fastapi.responses import Response
from urllib.parse import quote

from app.common import db_queries
from app.service.reconciliation_service import ReconciliationService
from app.sheets.workbook_writer import ReconciliationWorkbookWriter

router = APIRouter()
logger = logging.getLogger("controller.reconciliation")


@router.post("/reconciliation/run")
async def run_reconciliation(
        business_date: str = Form(...),
        current_batch_csv: UploadFile = File(None),
        settled_batch_csv: UploadFile = File(None)
):
    logger.info("Reconciliation request received | business_date=%s", business_date)

    # =====================================================
    # STEP 1: DB QUERIES (SOURCE OF TRUTH)
    # =====================================================
    logger.info("Running DB queries")

    sales_orders = db_queries.fetch_sales_orders(business_date)
    logger.info("Sales orders fetched | count=%s", len(sales_orders))

    order_items = db_queries.fetch_order_items(business_date)
    logger.info("Order items fetched | count=%s", len(order_items))

    asn_rows = db_queries.fetch_asn_process_numbers(business_date)
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

    logger.info(
        "Classification summary | success=%s | failed=%s | risky=%s | retry_success=%s",
        len(classification["successful_orders"]),
        len(classification["failed_orders"]),
        len(classification["risky_orders"]),
        len(classification["retry_success_orders"])
    )

    # =====================================================
    # STEP 4: EXCEL GENERATION (PRESENTATION)
    # =====================================================
    writer = ReconciliationWorkbookWriter(business_date)

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
        sales_orders=sales_orders  # ADD THIS LINE
    )

    # Sheet 3: Converge Settled with highlighting
    writer.create_converge_settled_sheet(converge_settled_rows)

    # Sheet 4: Orders Shipped with ASN vs Settled comparison
    writer.create_orders_shipped_sheet(
        asn_process_numbers=asn_process_numbers,
        order_totals=order_totals,
        converge_settled=converge_settled_result
    )

    # Sheet 5: RECONCILIATION REPORT (The main one!)
    writer.create_reconciliation_sheet(
        cxp_orders=sales_orders,
        classification=classification,
        converge_current=converge_current_result,
        converge_settled=converge_settled_result,
        asn_process_numbers=asn_process_numbers,
        order_totals=order_totals
    )

    excel_io = writer.to_bytes()

    filename = f"Reconciliation_{business_date}.xlsx"
    encoded_filename = quote(filename)

    logger.info("Reconciliation completed successfully | business_date=%s", business_date)

    return Response(
        content=excel_io.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{encoded_filename}"',
            "Content-Length": str(len(excel_io.getvalue()))
        }
    )
