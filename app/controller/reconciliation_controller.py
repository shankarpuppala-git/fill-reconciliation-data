from cmath import exp
from unittest import expectedFailure

from exceptiongroup import catch
from fastapi import APIRouter, Form, UploadFile, File

from app.service.reconciliation_service import ReconciliationService
from fastapi.responses import StreamingResponse
from datetime import datetime
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/reconciliation/run")
async def run_reconciliation(
    business_date: str = Form(...),
    current_batch_csv: UploadFile = File(None),
    settled_batch_csv: UploadFile = File(None)
):

    logger.info("Reconciliation started | business_date=%s", business_date)

    try:
        reconciliation_data = ReconciliationService.run_db_queries(business_date=business_date,logger=logger)

        shipped_count = len(reconciliation_data.get("asn_process_numbers", []))
        logger.info(
            "DB reconciliation completed | business_date=%s | shipped_orders=%s",
            business_date,
            shipped_count
        )
    except Exception as e:
        logger.exception("Reconciliation failed | business_date=%s", business_date)
        logger.exception(e)

    try:
        csv_summary = ReconciliationService.process_converge_files(current_csv=current_batch_csv,settled_csv=settled_batch_csv,logger=logger)
        logger.info(
            "CSV processed | business_date=%s | settled_batches=%s",
            business_date,
            csv_summary.get("settled_batches", {}).get("total_rows", 0)
        )
    except Exception as e:
        logger.exception("CSV file  failed | business_date=%s", business_date)
        logger.exception(e)
        raise

    try:
        logger.info("Reconciliation shipped vs settled | business_date=%s", business_date)
        reconciliation_result = ReconciliationService.reconcile_shipped_vs_settled(reconciliation_data["asn_process_numbers"],csv_summary["settled_batches"]["transaction_type_breakdown"])
        logger.info("result of the shipped and settled orders %s",reconciliation_result)

    except Exception as e:
        logger.exception("Reocniliation logic failed | business_date=%s", business_date)
        raise

    # Excel generation
    excel_bytes = ReconciliationService.generate_reconciliation_workbook(
        business_date=business_date,
        reconciliation_data=reconciliation_data,
        csv_summary=csv_summary
    )
    filename = f"Reconciliation_{business_date}.xlsx"

    logger.info("Reconciliation completed  Successfully | business_date=%s", business_date)

    return StreamingResponse(
        excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )


