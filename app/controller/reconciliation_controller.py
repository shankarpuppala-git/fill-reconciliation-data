from fastapi import APIRouter, Form, UploadFile, File
from app.service.reconciliation_service import ReconciliationService
from app.common.logger import GoogleDocsLogger
from datetime import datetime

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

        # -------- DB QUERIES --------
        reconciliation_data = ReconciliationService.run_db_reconciliation(
            business_date=business_date,
            logger=logger
        )

        # -------- WRITE DB DATA TO SHEETS --------
        ReconciliationService.write_db_results_to_sheets(
            reconciliation_data=reconciliation_data,
            spreadsheet_id=SPREADSHEET_ID,
            business_date=business_date,
            logger=logger
        )

        # -------- CSV PROCESSING --------
        date_suffix = datetime.strptime(
            business_date, "%Y-%m-%d"
        ).strftime("%m/%d")

        ReconciliationService.process_converge_csvs(
            current_csv=current_batch_csv,
            settled_csv=settled_batch_csv,
            spreadsheet_id=SPREADSHEET_ID,
            date_suffix=date_suffix,
            logger=logger
        )
        if logger:
         logger.log("INFO", "Reconciliation completed successfully")

        return {
            "status": "SUCCESS",
            "business_date": business_date
        }

    except Exception as e:
        logger.log("ERROR", str(e))
        raise
