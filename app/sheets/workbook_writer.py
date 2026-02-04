from datetime import datetime
from openpyxl import Workbook
from io import BytesIO
from openpyxl.utils import get_column_letter


class ReconciliationWorkbookWriter:

    def __init__(self, business_date: str):
        """
        business_date format: YYYY-MM-DD
        """
        self.business_date = business_date
        self.workbook = Workbook()
        default_sheet = self.workbook.active
        self.workbook.remove(default_sheet)

    def get_filename(self) -> str:
        date_str = datetime.strptime(self.business_date, "%Y-%m-%d").strftime("%m-%d-%Y")
        return f"Reconciliation_{date_str}.xlsx"

    # ================= SHEET 1: CXP =================
    def create_cxp_sheet(self, sales_orders: list, order_items: list):
        sheet = self.workbook.create_sheet("CXP")

        headers = [
            "Process Number", "Email", "Order Date",
            "Order State", "Mobile", "Payment Ref"
        ]
        sheet.append(headers)

        for row in sales_orders:
            sheet.append([
                row["process_number"],
                row["notif_email"],
                row["order_date"],
                row["order_state"],
                row["notify_mobile_no"],
                row["payment_reference_no"]
            ])

        # Query-2 data (L–M)
        start_col = 12
        sheet.cell(row=1, column=start_col, value="Order Process Number")
        sheet.cell(row=1, column=start_col + 1, value="Order Status")

        for idx, row in enumerate(order_items, start=2):
            sheet.cell(row=idx, column=start_col, value=row["order_process_number"])
            sheet.cell(row=idx, column=start_col + 1, value=row["order_status"])

    # ================= SHEET 2: CONVERGE =================
    def create_converge_sheet(self, converge_rows: list):
        sheet = self.workbook.create_sheet("Converge")

        headers = [
            "Invoice Number", "Auth Message",
            "Customer Name", "Transaction Date",""
        ]
        sheet.append(headers)

        for row in converge_rows:
            sheet.append([row.get("invoice"), row.get("auth_message"), row.get("customer"),row.get("transaction_date")])

    # ================= SHEET 3: CONVERGE SETTLED =================
    def create_converge_settled_sheet(self, settled_rows: list):
        sheet = self.workbook.create_sheet("Converge Settled")

        headers = [
            "Invoice Number", "Original Amount", "Original Transaction Type"
        ]
        sheet.append(headers)

        for row in settled_rows:
            sheet.append([row.get("invoice"), row.get("amount"), row.get("txn_type")])

    # ================= SHEET 4: ORDERS SHIPPED =================
    def create_orders_shipped_sheet(self, shipped_numbers: list):
        sheet = self.workbook.create_sheet("Orders Shipped")

        sheet.append(["Order Number", "Order Total", "Matching with converge","Difference"])


        for idx, process_number in enumerate(shipped_numbers, start=2):
            sheet.cell(row=idx, column=1, value=process_number)

            # Excel VLOOKUP (bounded, efficient)
            sheet.cell(
                row=idx,
                column=3,
                value=f'=IF(A{idx}="","",VLOOKUP(A{idx},Converge!$A:$B,2,FALSE))'
            )

    # ================= SAVE =================
    def save(self, directory: str = "."):
        path = f"{directory}/{self.get_filename()}"
        self.workbook.save(path)
        return path



    def to_bytes(self) -> BytesIO:
        """
        Returns Excel workbook as in-memory bytes
        """
        output = BytesIO()
        self.workbook.save(output)
        output.seek(0)
        return output
