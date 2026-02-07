from datetime import datetime
import csv
import io
from io import TextIOWrapper
from collections import defaultdict

from app.common import db_queries
from app.db.db_client import get_db_connection
from app.sheets.workbook_writer import ReconciliationWorkbookWriter





def safe_log(logger, level, message):
    if logger:
        logger.log(level, message)
    else:
        print(f"[{level}] {message}")


class ReconciliationService:

    # ================= DB QUERIES =================
    @staticmethod
    def run_db_queries(business_date: str, logger):
        logger.info(f"Starting DB reconciliation for date {business_date}")

        logger.info("Running Query-1: Sales Orders")
        sales_orders = db_queries.fetch_sales_orders(business_date)
        logger.info( f"Query-1 completed | rows={len(sales_orders)}")

        logger.info("Running Query-2: Order Items")
        order_items = db_queries.fetch_order_items(business_date)
        logger.info( f"Query-2 completed | rows={len(order_items)}")

        logger.info( "Running Query-3: ASN Process Numbers")
        asn_rows = db_queries.fetch_asn_process_numbers(business_date)
        process_numbers = [row["process_number"] for row in asn_rows]
        logger.info( f"Query-3 completed | rows={len(process_numbers)}")

        order_totals = []
        if process_numbers:
            logger.info( "Running Query-4: Order Totals")
            order_totals = db_queries.fetch_order_totals(process_numbers)
            logger.info( f"Query-4 completed | rows={len(order_totals)}")
        else:
            logger.warning( "Query-4 skipped (no ASN process numbers)")


        return {
            "sales_orders": sales_orders,
            "order_items": order_items,
            "asn_process_numbers": process_numbers,
            "order_totals": order_totals
        }

    # ================= CSV PROCESSING =================
    @staticmethod
    def process_converge_files(current_csv, settled_csv, logger):
        csv_summary = {
            "current_batches": {
                "total_rows": 0,
                "valid_rows": 0,
                "skipped_rows": 0,
                "rows":[]
            },
            "settled_batches": {
                "total_rows": 0,
                "transaction_type_breakdown": {},
                "rows":[]
            }
        }

        # ---------- CURRENTBATCHES ----------
        if current_csv:
            logger.info( "Processing CURRENTBATCHES CSV")

            current_csv.file.seek(0)
            content = current_csv.file.read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(content))

            for row_num, row in enumerate(reader, start=2):
                csv_summary["current_batches"]["total_rows"] += 1

                invoice = row.get("Invoice Number", "").strip()
                auth_msg = row.get("Auth Message", "").strip()
                customer = row.get("Customer Full Name", "").strip()
                txn_date = row.get("Transaction Date", "").strip()

                if not invoice or (txn_date and not (auth_msg or customer)):
                    csv_summary["current_batches"]["skipped_rows"] += 1
                    logger.warn(f"CURRENTBATCHES: Skipping row {row_num}")
                    continue

                csv_summary["current_batches"]["valid_rows"] += 1
                csv_summary["current_batches"]["rows"].append({
                    "invoice": invoice,
                    "auth_message": auth_msg,
                    "customer": customer,
                    "transaction_date": txn_date
                })

            logger.info(f"CURRENTBATCHES summary: {csv_summary['current_batches']}")

        # ---------- SETTLEDBATCHES ----------
        if settled_csv:
            logger.info("Processing SETTLEDBATCHES CSV")

            txn_type_map = defaultdict(int)
            settled_csv.file.seek(0)
            content = settled_csv.file.read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(content))

            for row_num, row in enumerate(reader, start=2):
                csv_summary["settled_batches"]["total_rows"] += 1

                amount = row.get("Original Amount", "").strip()
                invoice = row.get("Invoice Number", "").strip()
                status= row.get("Transaction Status").strip()
                txn_type = row.get("Original Transaction Type", "").strip().upper()
                if not txn_type:
                    txn_type = "UNKNOWN"

                txn_type_map[txn_type] += 1

                csv_summary["settled_batches"]["rows"].append({
                    "invoice": invoice,
                    "transaction_type": txn_type,
                    "amount": amount,
                    "status":status
                })
            csv_summary["settled_batches"]["transaction_type_breakdown"] = dict(txn_type_map)

            logger.info(f"SETTLEDBATCHES summary: {csv_summary['settled_batches']}");


        return csv_summary


    # ================= RECONCILIATION LOGIC =================
    @staticmethod
    def reconcile_shipped_vs_settled(asn_process_numbers, settled_txn_breakdown):
        shipped_count = len(asn_process_numbers)
        settled_sale_count = settled_txn_breakdown.get("SALE", 0)

        if shipped_count == settled_sale_count:
            return {
                "status": "MATCHED",
                "message": (
                    f"{shipped_count} orders shipped and "
                    f"{settled_sale_count} orders settled – MATCHED"
                )
            }

        if shipped_count > settled_sale_count:
            diff = shipped_count - settled_sale_count
            return {
                "status": "MISMATCH",
                "message": (
                    f"{shipped_count} orders shipped, "
                    f"{settled_sale_count} orders settled – "
                    f"{diff} orders pending settlement"
                )
            }

        diff = settled_sale_count - shipped_count
        return {
            "status": "MISMATCH",
            "message": (
                f"{shipped_count} orders shipped, "
                f"{settled_sale_count} orders settled – "
                f"{diff} extra settlements found"
            )
        }

    @staticmethod
    def generate_reconciliation_workbook(
            business_date: str,
            reconciliation_data: dict,
            csv_summary: dict
    ):
        writer = ReconciliationWorkbookWriter(business_date)

        # Sheet 1: CXP
        writer.create_cxp_sheet(
            sales_orders=reconciliation_data["sales_orders"],
            order_items=reconciliation_data["order_items"]
        )

        # Sheet 2: Converge
        writer.create_converge_sheet(
            converge_rows=csv_summary["current_batches"]["rows"]
        )

        # Sheet 3: Converge Settled
        writer.create_converge_settled_sheet(
            settled_rows=csv_summary["settled_batches"]["rows"]
        )

        # Sheet 4: Orders Shipped
        writer.create_orders_shipped_sheet(
            shipped_numbers=reconciliation_data["asn_process_numbers"]
        )

        return writer.to_bytes()
