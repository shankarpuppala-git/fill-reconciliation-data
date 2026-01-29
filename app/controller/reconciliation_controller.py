from fastapi import APIRouter, Form, UploadFile, File
from datetime import datetime

from app.service.reconciliation_service import ReconciliationService
from app.common.logger import GoogleDocsLogger

router = APIRouter()

LOG_FOLDER_ID = "1GF7Y1YJFEXbAHAp4U0FWhYWeJeCQU-Dk"
SERVICE_ACCOUNT_FILE = "service_account.json"
SPREADSHEET_ID = "1unJYpVGmscjItt02nkgpo--9PYatPLkcCnBy76AQu7U"


@router.post("/reconciliation/run")
async def run_reconciliation(
    business_date: str = Form(...),
    current_batch_csv: UploadFile = File(None),
    settled_batch_csv: UploadFile = File(None)
):
    logger = None

    # -------- LOGGER (FEATURE FLAGGED FOR UAT) --------
    try:
        logger = GoogleDocsLogger(
            folder_id=LOG_FOLDER_ID,
            service_account_file=SERVICE_ACCOUNT_FILE
        )
    except Exception as e:
        print("⚠️ GoogleDocsLogger disabled for UAT:", e)

    try:
        if logger:
            logger.log("INFO", "Reconciliation API triggered")

        # ================= DB RECONCILIATION =================
        reconciliation_data = ReconciliationService.run_db_reconciliation(
            business_date=business_date,
            logger=logger
        )

        # ================= CSV PROCESSING =================
        csv_summary = ReconciliationService.process_converge_csvs(
            current_csv=current_batch_csv,
            settled_csv=settled_batch_csv,
            logger=logger
        )

        # ================= RECONCILIATION LOGIC =================
        reconciliation_result = ReconciliationService.reconcile_shipped_vs_settled(
            reconciliation_data["asn_process_numbers"],
            csv_summary["settled_batches"]["transaction_type_breakdown"]
        )

        # ================= GOOGLE SHEETS (DISABLED IN UAT) =================
        if logger:
            ReconciliationService.write_db_results_to_sheets(
                reconciliation_data=reconciliation_data,
                spreadsheet_id=SPREADSHEET_ID,
                business_date=business_date,
                logger=logger
            )
        else:
            print("[INFO] Skipping Google Sheets write (UAT mode)")

        # ================= FINAL RESPONSE =================
        response = {
            "status": "SUCCESS",
            "business_date": business_date,

            "db_summary": {
                "orders_shipped": len(reconciliation_data["asn_process_numbers"])
            },

            "Converge_summary": csv_summary,

            "reconciliation": reconciliation_result,

            "google_sheets_write": "SKIPPED (UAT mode)"
        }

        if logger:
            logger.log("INFO", f"Reconciliation completed: {response['reconciliation']}")

        return response

    except Exception as e:
        if logger:
            logger.log("ERROR", str(e))
        else:
            print("[ERROR]", str(e))
        raise
