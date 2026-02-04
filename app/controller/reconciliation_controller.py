from fastapi import APIRouter, Form, UploadFile, File

from app.service.reconciliation_service import ReconciliationService
from fastapi.responses import StreamingResponse
from datetime import datetime

router = APIRouter()



@router.post("/reconciliation/run")
async def run_reconciliation(
    business_date: str = Form(...),
    current_batch_csv: UploadFile = File(None),
    settled_batch_csv: UploadFile = File(None)
):
    logger = None

    reconciliation_data = ReconciliationService.run_db_queries(business_date=business_date,logger=logger)

    csv_summary = ReconciliationService.process_converge_files(current_csv=current_batch_csv,settled_csv=settled_batch_csv,logger=logger)

    reconciliation_result = ReconciliationService.reconcile_shipped_vs_settled(reconciliation_data["asn_process_numbers"],csv_summary["settled_batches"]["transaction_type_breakdown"])

    # Excel generation
    excel_bytes = ReconciliationService.generate_reconciliation_workbook(
        business_date=business_date,
        reconciliation_data=reconciliation_data,
        csv_summary=csv_summary
    )
    filename = f"Reconciliation_{business_date}.xlsx"
    response = {
        "status": "SUCCESS",
        "business_date": business_date,

        "db_summary": {
            "orders_shipped": len(reconciliation_data["asn_process_numbers"])
        },

        "Converge_summary": csv_summary,

        "reconciliation": reconciliation_result,

    }

    if logger:
        logger.log("INFO", f"Reconciliation completed: {response['reconciliation']}")

    return StreamingResponse(
        excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )


