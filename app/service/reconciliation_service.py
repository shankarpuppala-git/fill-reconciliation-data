from datetime import datetime
import csv
from io import TextIOWrapper

from app.common import db_queries
from app.sheets.sheets_writer import GoogleSheetsWriter

def safe_log(logger, level, message):
    if logger:
        logger.log(level, message)
    else:
        print(f"[{level}] {message}")

class ReconciliationService:

    # ================= DB QUERIES =================
    @staticmethod
    def run_db_reconciliation(business_date: str, logger):
        safe_log(logger,"INFO", f"Starting DB reconciliation for date {business_date}")

        safe_log(logger,"INFO", "Running Query-1: Sales Orders")
        sales_orders = db_queries.fetch_sales_orders(business_date)
        safe_log(logger,"INFO", f"Query-1 completed | rows={len(sales_orders)}")

        safe_log(logger,"INFO", "Running Query-2: Order Items")
        order_items = db_queries.fetch_order_items(business_date)
        safe_log(logger,"INFO", f"Query-2 completed | rows={len(order_items)}")

        safe_log(logger,"INFO", "Running Query-3: ASN Process Numbers")
        asn_rows = db_queries.fetch_asn_process_numbers(business_date)
        process_numbers = [row["process_number"] for row in asn_rows]
        safe_log(logger,"INFO", f"Query-3 completed | rows={len(process_numbers)}")

        order_totals = []
        if process_numbers:
            safe_log(logger,"INFO", "Running Query-4: Order Totals")
            order_totals = db_queries.fetch_order_totals(process_numbers)
            safe_log(logger,"INFO", f"Query-4 completed | rows={len(order_totals)}")
        else:
            safe_log(logger,"WARN", "Query-4 skipped (no ASN process numbers)")

        return {
            "sales_orders": sales_orders,
            "order_items": order_items,
            "asn_process_numbers": process_numbers,
            "order_totals": order_totals
        }

    # ================= CSV PROCESSING =================
    @staticmethod
    def process_converge_csvs(
        current_csv,
        settled_csv,
        spreadsheet_id: str,
        date_suffix: str,
        logger
    ):
        writer = GoogleSheetsWriter(
            spreadsheet_id=spreadsheet_id,
            service_account_file="service_account.json"
        )

        # ---------- CURRENTBATCHES → Converge ----------
        if current_csv:
            safe_log(logger,"INFO", "Processing CURRENTBATCHES CSV")

            converge_rows = []
            csv_reader = csv.DictReader(
                TextIOWrapper(current_csv.file, encoding="utf-8")
            )

            for row_num, row in enumerate(csv_reader, start=2):
                invoice = row.get("Invoice Number", "").strip()
                auth_msg = row.get("Auth Message", "").strip()
                customer = row.get("Customer Full Name", "").strip()
                txn_date = row.get("Transaction Date", "").strip()

                if not invoice:
                    safe_log(logger,
                        "WARN",
                        f"CURRENTBATCHES: Skipping row {row_num} (missing Invoice Number)"
                    )
                    continue

                if txn_date and not (auth_msg or customer):
                    safe_log(logger,
                        "WARN",
                        f"CURRENTBATCHES: Skipping row {row_num} (only Transaction Date present)"
                    )
                    continue

                converge_rows.append([
                    invoice,
                    auth_msg,
                    customer,
                    txn_date
                ])

            if converge_rows:
                converge_sheet = f"Converge {date_suffix}"
                writer.write_block(converge_sheet, 2, 1, converge_rows)
                safe_log(logger,
                    "INFO",
                    f"Wrote {len(converge_rows)} rows to {converge_sheet}"
                )
            else:
                safe_log(logger,"WARN", "No valid rows found in CURRENTBATCHES CSV")

        # ---------- SETTLEDBATCHES → Converge Settled ----------
        if settled_csv:
            safe_log(logger,"INFO", "Processing SETTLEDBATCHES CSV")

            settled_rows = []
            csv_reader = csv.DictReader(
                TextIOWrapper(settled_csv.file, encoding="utf-8")
            )

            for row_num, row in enumerate(csv_reader, start=2):
                invoice = row.get("Invoice Number", "").strip()
                amount = row.get("Original Amount", "").strip()

                if not invoice:
                    safe_log(logger,
                        "WARN",
                        f"SETTLEDBATCHES: Skipping row {row_num} (missing Invoice Number)"
                    )
                    continue

                settled_rows.append([invoice, amount])

            if settled_rows:
                settled_sheet = f"Converge Settled {date_suffix}"
                writer.write_block(settled_sheet, 2, 1, settled_rows)
                safe_log(logger,
                    "INFO",
                    f"Wrote {len(settled_rows)} rows to {settled_sheet}"
                )
            else:
                safe_log(logger,"WARN", "No valid rows found in SETTLEDBATCHES CSV")

    # ================= WRITE DB DATA TO SHEETS =================
    @staticmethod
    def write_db_results_to_sheets(
        reconciliation_data: dict,
        spreadsheet_id: str,
        business_date: str,
        logger
    ):
        safe_log(logger,"INFO", "Writing DB results to Google Sheets")

        writer = GoogleSheetsWriter(
            spreadsheet_id=spreadsheet_id,
            service_account_file="service_account.json"
        )

        date_suffix = datetime.strptime(
            business_date, "%Y-%m-%d"
        ).strftime("%m/%d")

        cxp_sheet = f"CXP {date_suffix}"
        shipped_sheet = f"Orders Shipped {date_suffix}"

        # Query-1 → CXP (A–F)
        q1_data = [
            [
                row["process_number"],
                row["notif_email"],
                row["order_date"],
                row["order_state"],
                row["notify_mobile_no"],
                row["payment_reference_no"]
            ]
            for row in reconciliation_data["sales_orders"]
        ]
        writer.write_block(cxp_sheet, 2, 1, q1_data)

        # Query-2 → CXP (L–M)
        q2_data = [
            [
                row["order_process_number"],
                row["order_status"]
            ]
            for row in reconciliation_data["order_items"]
        ]
        writer.write_block(cxp_sheet, 2, 12, q2_data)

        # Query-3 → Orders Shipped (A)
        writer.write_single_column(
            shipped_sheet,
            2,
            1,
            reconciliation_data["asn_process_numbers"]
        )

        # Query-4 → Orders Shipped (E–F)
        q4_data = [
            [
                row["process_number"],
                row["order_total"]
            ]
            for row in reconciliation_data["order_totals"]
        ]
        writer.write_block(shipped_sheet, 2, 5, q4_data)

        safe_log(logger,"INFO", "DB results written to Google Sheets successfully")
