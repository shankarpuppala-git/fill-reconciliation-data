from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from io import BytesIO
from collections import defaultdict
from openpyxl.cell.cell import MergedCell
import logging
logger = logging.getLogger("workbook_writer")

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

    # ================= SHEET 1: CXP WITH HIGHLIGHTING =================
    def create_cxp_sheet(
            self,
            sales_orders: list,
            order_items: list,
            classification: dict,
            converge_current: dict,
            converge_settled: dict,
            asn_process_numbers: list
    ):
        """
        Creates CXP sheet with multiple retry highlighting
        """
        sheet = self.workbook.create_sheet("CXP")

        # Dark blue header
        headers = [
            "Order number", "Email", "Date", "CXP DB Status",
            "Phone number", "Payment Reference", "CXP Status",
            "Converge Status", "Converge Settled", "ASN"
        ]

        sheet.append(headers)
        self._apply_header_style(sheet, 1, len(headers))

        # Build customer history for retry detection
        customer_history = defaultdict(list)
        for order in sales_orders:
            email = order.get("notif_email")
            phone = order.get("notify_mobile_no")
            customer_key = email or phone
            if customer_key:
                customer_history[customer_key].append(order["process_number"])

        # Identify customers with multiple attempts
        multi_retry_orders = set()
        for customer, orders in customer_history.items():
            if len(orders) > 1:
                multi_retry_orders.update(orders)

        # Converge data lookup
        converge_invoices = converge_current.get("invoices", {})
        settled_invoices = converge_settled.get("invoice_level", {})

        row_num = 2
        for order in sales_orders:
            order_id = order["process_number"]

            # Get classification data
            order_classification = classification["orders"].get(order_id, {})

            # Get converge data
            converge_info = converge_invoices.get(order_id, {})
            settled_info = settled_invoices.get(order_id, {})

            converge_status = converge_info.get("final_status", "NA")
            is_settled = "Yes" if settled_info.get("settled", False) else "No"
            asn_status = "1" if order_id in asn_process_numbers else ""

            row_data = [
                order_id,
                order.get("notif_email"),
                order.get("order_date"),
                order.get("order_state"),
                order.get("notify_mobile_no"),
                order.get("payment_reference_no"),
                order.get("fulfillment_status"),
                converge_status,
                is_settled,
                asn_status
            ]

            sheet.append(row_data)

            # Apply highlighting for multiple retries
            if order_id in multi_retry_orders:
                state = order_classification.get("state")
                if state == "SUCCESS":
                    fill_color = "C6EFCE"  # Light green
                elif state == "FAILED":
                    fill_color = "FFEB9C"  # Light yellow
                else:
                    fill_color = "FFF2CC"  # Light yellow-orange

                self._apply_row_fill(sheet, row_num, len(headers), fill_color)

            row_num += 1

        self._auto_fit_columns(sheet)

    # ================= SHEET 2: CONVERGE WITH HIGHLIGHTING =================
    def create_converge_sheet(self, converge_rows: list, converge_current: dict, sales_orders: list):
        """
        Creates Converge sheet with card issue highlighting
        Includes CXP DB Status and Email from sales_orders lookup
        """
        sheet = self.workbook.create_sheet("Converge")

        headers = [
            "Invoice Number", "Auth Message", "customer Full Name",
            "Transaction Date", "CXP DB Status", "Email",
            "Card Related Issues", "For Multiple Tries"
        ]

        sheet.append(headers)
        self._apply_header_style(sheet, 1, len(headers))

        # Build lookup from sales_orders
        cxp_lookup = {}
        for order in sales_orders:
            order_id = order["process_number"]
            cxp_lookup[order_id] = {
                "cxp_db_status": order.get("order_state", ""),
                "email": order.get("notif_email", "")
            }

        converge_invoices = converge_current.get("invoices", {})

        row_num = 2
        for row in converge_rows:
            invoice = row.get("Invoice Number", "").strip()
            auth_message = row.get("Auth Message", "").strip()

            # Check if this is a card-related issue
            is_card_issue = any(keyword in auth_message.upper() for keyword in
                                ["DECLINED", "SUSPECTED FRAUD", "NSF", "CLOSED", "WITHDRAWAL"])

            # Check if data inconsistency
            invoice_data = converge_invoices.get(invoice, {})
            is_data_issue = invoice_data.get("is_data_issue", False)
            multiple_tries = "Yes" if is_data_issue else ""

            # Get CXP data from lookup
            cxp_data = cxp_lookup.get(invoice, {})
            cxp_db_status = cxp_data.get("cxp_db_status", "")
            email = cxp_data.get("email", "")

            row_data = [
                invoice,
                auth_message,
                row.get("Customer Full Name"),
                row.get("Transaction Date"),
                cxp_db_status,  # From CXP lookup
                email,  # From CXP lookup
                "",  # Card Related Issues - NO TEXT, just highlighting
                multiple_tries
            ]

            sheet.append(row_data)

            # Apply blue highlighting for card issues (NO "Yes" text)
            if is_card_issue:
                self._apply_row_fill(sheet, row_num, len(headers), "DAEEF3")  # Light blue

            row_num += 1

        self._auto_fit_columns(sheet)
    # ================= SHEET 3: CONVERGE SETTLED WITH HIGHLIGHTING =================
    def create_converge_settled_sheet(self, settled_rows: list):
        """
        Creates Converge Settled sheet with discrepancy highlighting
        """
        sheet = self.workbook.create_sheet("Converge Settled")

        headers = [
            "INVOICE NUMBER", "AMOUNT", "Transaction Status", "Original Transaction Type"
        ]

        sheet.append(headers)
        self._apply_header_style(sheet, 1, len(headers))

        row_num = 2
        for row in settled_rows:
            invoice = row.get("Invoice Number", "").strip()

            # Skip summary rows
            if not invoice:
                continue

            transaction_status = row.get("Transaction Status", "").strip()

            # Check for discrepancies (non-SETTLED status)
            is_discrepancy = transaction_status.upper() != "SETTLED"

            row_data = [
                invoice,
                row.get("Original Amount"),
                transaction_status,
                row.get("Original Transaction Type")
            ]

            sheet.append(row_data)

            # Apply red highlighting for discrepancies
            if is_discrepancy:
                self._apply_row_fill(sheet, row_num, len(headers), "FFC7CE")  # Light red

            row_num += 1

        self._auto_fit_columns(sheet)

    # ================= SHEET 4: ORDERS SHIPPED WITH COMPARISON =================
    def create_orders_shipped_sheet(
            self,
            asn_process_numbers: list,
            order_totals: list,
            converge_settled: dict
    ):
        """
        Creates Orders Shipped sheet with ASN vs Settled comparison
        """
        sheet = self.workbook.create_sheet("Orders Shipped")

        headers = [
            "Order no.", "Sum of Total CXP", "MATCHING WITH CONVERGE",
            "Differences", "Order no", "Amount"
        ]

        sheet.append(headers)
        self._apply_header_style(sheet, 1, len(headers))

        # Build lookup dictionaries
        order_total_map = {item["process_number"]: item["order_total"] for item in order_totals}
        settled_invoices = converge_settled.get("invoice_level", {})

        # Get settled order amounts
        settled_amount_map = {}
        for invoice, data in settled_invoices.items():
            for raw_row in data.get("raw_rows", []):
                if raw_row.get("transaction_type") == "SALE":
                    settled_amount_map[invoice] = raw_row.get("amount")
                    break

        row_num = 2
        for order_id in asn_process_numbers:
            cxp_amount = order_total_map.get(order_id)
            converge_amount = settled_amount_map.get(order_id)

            # Check if order is in settled batch
            is_in_converge = order_id in settled_invoices
            matching_status = "YES" if is_in_converge else "NO"

            # Calculate difference
            difference = ""
            if cxp_amount and converge_amount:
                diff = abs(float(cxp_amount) - float(converge_amount))
                difference = f"{diff:.2f}" if diff > 0.01 else "NO"
            elif not is_in_converge:
                difference = "NOT IN CONVERGE"

            row_data = [
                order_id,
                cxp_amount,
                matching_status,
                difference,
                order_id,  # Repeated for second section
                converge_amount if converge_amount else "NA"
            ]

            sheet.append(row_data)

            # Highlight discrepancies
            if matching_status == "NO" or (difference and difference not in ["NO", ""]):
                self._apply_row_fill(sheet, row_num, len(headers), "FFC7CE")  # Light red
                # Add comment for missing orders
                if not is_in_converge:
                    sheet.cell(row=row_num, column=3).comment = "Order in ASN but not in Settled batch"

            row_num += 1

        self._auto_fit_columns(sheet)

    # ================= SHEET 5: RECONCILIATION (THE BIG ONE) =================
    def create_reconciliation_sheet(
            self,
            cxp_orders: list,
            classification: dict,
            converge_current: dict,
            converge_settled: dict,
            asn_process_numbers: list,
            order_totals: list
    ):
        """
        Creates the main Reconciliation sheet with 6 sections:
        1-4: Headers only (no data rows)
        5: Summary Statistics
        6: FAILED orders only (not all orders)
        """
        sheet = self.workbook.create_sheet("Reconciliation", 0)  # First sheet

        current_row = 1

        # Build helper lookups
        converge_invoices = converge_current.get("invoices", {})
        settled_invoices = converge_settled.get("invoice_level", {})
        order_total_map = {item["process_number"]: item["order_total"] for item in order_totals}
        orders_dict = classification["orders"]

        # ===== SECTION 1: Headers only =====
        current_row = self._add_section_header(
            sheet, current_row,
            "Orders present in CXP and not present in Converge"
        )

        headers_1 = ["Order #", "Converge Status", "CXP Status", "Remarks"]
        sheet.append(headers_1)
        self._apply_sub_header_style(sheet, current_row, len(headers_1))
        current_row += 1
        # NO DATA ROWS - just headers

        current_row += 2  # Blank rows

        # ===== SECTION 2: Headers only =====
        current_row = self._add_section_header(
            sheet, current_row,
            "Orders Shipped in CXP but amount different in Converge"
        )

        headers_2 = ["Order #", "Converge Amount", "CXP Amount", "Remarks"]
        sheet.append(headers_2)
        self._apply_sub_header_style(sheet, current_row, len(headers_2))
        current_row += 1
        # NO DATA ROWS - just headers

        current_row += 2

        # ===== SECTION 3: Headers only =====
        current_row = self._add_section_header(
            sheet, current_row,
            "Orders Claimed / Shipped in CXP but not showing in ASN"
        )

        headers_3 = ["Order #", "ASN Status", "CXP Status", "Remarks"]
        sheet.append(headers_3)
        self._apply_sub_header_style(sheet, current_row, len(headers_3))
        current_row += 1
        # NO DATA ROWS - just headers

        current_row += 2

        # ===== SECTION 4: Headers only =====
        current_row = self._add_section_header(
            sheet, current_row,
            "Orders Shipped in CXP but converge is not settled"
        )

        headers_4 = ["Order #", "CXP status", "Converge Status", "Remarks"]
        sheet.append(headers_4)
        self._apply_sub_header_style(sheet, current_row, len(headers_4))
        current_row += 1
        # NO DATA ROWS - just headers

        current_row += 3

        # ===== SECTION 5: SUMMARY STATISTICS =====
        summary_start_row = current_row

        total_orders = len(cxp_orders)
        successful_orders = len(classification["successful_orders"])

        # Declined due to credit card
        declined_count = sum(
            1 for order_id, data in orders_dict.items()
            if data.get("state") == "FAILED" and
            "DECLINED" in data.get("converge_status", "")
        )

        # User cancelled
        cancelled_count = sum(
            1 for order in cxp_orders
            if order.get("order_state") == "PAYMENT_CANCELLED"
        )

        # Retry success
        retry_success_count = len(classification["retry_success_orders"])

        summary_data = [
            ["Total Number of orders submitted", total_orders],
            ["Number of orders placed successfully", successful_orders],
            ["Number of orders declined due to credit card related issues", declined_count],
            ["Number of orders cancelled by users (user-initiated)", cancelled_count],
            ["Number of orders placed successfully after multiple tries", retry_success_count]
        ]

        for label, value in summary_data:
            sheet.append([label, "", "", value])
            current_row += 1

        current_row += 2

        # ===== SECTION 6: FAILED ORDERS ONLY =====
        current_row = self._add_section_header(
            sheet, current_row,
            "Failed Orders Details"
        )

        headers_6 = ["Order Number", "Email", "Phone Number", "CXP Status", "Converge Status",
                     "Reason for order failure"]
        sheet.append(headers_6)
        self._apply_sub_header_style(sheet, current_row, len(headers_6))
        current_row += 1

        # Get all FAILED order IDs
        failed_order_ids = set(classification["failed_orders"])
        internal_users={"amit.kumar@phasezeroventures.com"}

        # Add ONLY failed orders
        for order in cxp_orders:
            order_id = order["process_number"]

            email = order.get("notif_email", "").strip().lower()

            if email in internal_users:
                logger.info("Internal user email %s",email)
                continue

            # Only add if this order is in failed list
            if order_id in failed_order_ids:
                order_class = orders_dict.get(order_id, {})
                sheet.append([
                    order_id,
                    order.get("notif_email"),
                    order.get("notify_mobile_no"),
                    order_class.get("cxp_db_status", "NA"),
                    order_class.get("converge_status", "NA"),
                    ""  # Empty for now as requested
                ])
                current_row += 1

        self._auto_fit_columns(sheet)
    # ================= HELPER METHODS FOR STYLING =================
    def _apply_header_style(self, sheet, row, col_count):
        """Apply dark blue header style"""
        dark_blue_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
        white_font = Font(color="FFFFFF", bold=True)

        for col in range(1, col_count + 1):
            cell = sheet.cell(row=row, column=col)
            cell.fill = dark_blue_fill
            cell.font = white_font
            cell.alignment = Alignment(horizontal="center", vertical="center")

    def _apply_sub_header_style(self, sheet, row, col_count):
        """Apply light blue sub-header style"""
        light_blue_fill = PatternFill(start_color="B4C7E7", end_color="B4C7E7", fill_type="solid")
        bold_font = Font(bold=True)

        for col in range(1, col_count + 1):
            cell = sheet.cell(row=row, column=col)
            cell.fill = light_blue_fill
            cell.font = bold_font
            cell.alignment = Alignment(horizontal="center", vertical="center")

    def _apply_row_fill(self, sheet, row, col_count, color):
        """Apply fill color to entire row"""
        fill = PatternFill(start_color=color, end_color=color, fill_type="solid")

        for col in range(1, col_count + 1):
            sheet.cell(row=row, column=col).fill = fill

    def _add_section_header(self, sheet, row, title):
        """Add section header with dark blue background"""
        sheet.append([title])
        self._apply_header_style(sheet, row, 6)  # Span 6 columns

        # Merge cells for section header
        sheet.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)

        return row + 1

    def _auto_fit_columns(self, sheet):
        """Auto-fit column widths based on content"""

        for column in sheet.columns:
            max_length = 0
            column_letter = None

            for cell in column:
                # Skip merged cells
                if isinstance(cell, MergedCell):
                    continue

                # Get column letter from first non-merged cell
                if column_letter is None:
                    column_letter = cell.column_letter

                try:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                except:
                    pass

            # Only adjust if we found a valid column
            if column_letter:
                adjusted_width = min(max_length + 2, 50)
                sheet.column_dimensions[column_letter].width = adjusted_width
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
