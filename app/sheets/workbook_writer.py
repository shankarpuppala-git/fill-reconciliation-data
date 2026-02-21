from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.cell.cell import MergedCell
from openpyxl.comments import Comment
from io import BytesIO
from collections import defaultdict
import logging

logger = logging.getLogger("workbook_writer")


class ReconciliationWorkbookWriter:

    def __init__(self, business_date: str):
        """business_date format: YYYY-MM-DD"""
        self.business_date = business_date
        self.workbook = Workbook()
        self.workbook.remove(self.workbook.active)

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
        sheet = self.workbook.create_sheet("CXP")

        headers = [
            "Order number", "Email", "Date", "CXP DB Status",
            "Phone number", "Payment Reference", "CXP Status",
            "Converge Status", "Converge Settled", "ASN"
        ]
        sheet.append(headers)
        self._apply_header_style(sheet, 1, len(headers))

        customer_history = defaultdict(list)
        for order in sales_orders:
            key = order.get("notif_email") or order.get("notify_mobile_no")
            if key:
                customer_history[key].append(order["process_number"])

        multi_retry_orders = {
            oid
            for orders in customer_history.values()
            if len(orders) > 1
            for oid in orders
        }

        converge_invoices = converge_current.get("invoices", {})
        settled_invoices  = converge_settled.get("invoice_level", {})
        asn_set           = set(asn_process_numbers)

        row_num = 2
        for order in sales_orders:
            order_id      = order["process_number"]
            order_class   = classification["orders"].get(order_id, {})
            converge_info = converge_invoices.get(order_id, {})
            settled_info  = settled_invoices.get(order_id, {})

            converge_status = converge_info.get("final_status", "NA")
            is_settled      = "Yes" if settled_info.get("settled", False) else "No"
            asn_status      = "1" if order_id in asn_set else ""

            sheet.append([
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
            ])

            if order_id in multi_retry_orders:
                state = order_class.get("state")
                colour = "C6EFCE" if state == "SUCCESS" else ("FFEB9C" if state == "FAILED" else "FFF2CC")
                self._apply_row_fill(sheet, row_num, len(headers), colour)

            row_num += 1

        self._auto_fit_columns(sheet)

    # ================= SHEET 2: CONVERGE WITH HIGHLIGHTING =================
    def create_converge_sheet(self, converge_rows: list, converge_current: dict, sales_orders: list):
        sheet = self.workbook.create_sheet("Converge")

        headers = [
            "Invoice Number", "Auth Message", "customer Full Name",
            "Transaction Date", "CXP DB Status", "Email",
            "Card Related Issues", "For Multiple Tries"
        ]
        sheet.append(headers)
        self._apply_header_style(sheet, 1, len(headers))

        cxp_lookup = {
            o["process_number"]: {
                "cxp_db_status": o.get("order_state", ""),
                "email":         o.get("notif_email", "")
            }
            for o in sales_orders
        }

        converge_invoices = converge_current.get("invoices", {})

        row_num = 2
        for row in converge_rows:
            invoice      = (row.get("Invoice Number") or "").strip()
            auth_message = (row.get("Auth Message") or "").strip()

            is_card_issue = any(
                kw in auth_message.upper()
                for kw in ("DECLINED", "SUSPECTED FRAUD", "NSF", "CLOSED", "WITHDRAWAL")
            )

            invoice_data   = converge_invoices.get(invoice, {})
            is_data_issue  = invoice_data.get("is_data_issue", False)
            multiple_tries = "Yes" if is_data_issue else ""

            cxp_data = cxp_lookup.get(invoice, {})

            sheet.append([
                invoice,
                auth_message,
                row.get("Customer Full Name"),
                row.get("Transaction Date"),
                cxp_data.get("cxp_db_status", ""),
                cxp_data.get("email", ""),
                "Yes" if is_card_issue else "",
                multiple_tries
            ])

            if is_card_issue:
                self._apply_row_fill(sheet, row_num, len(headers), "DAEEF3")

            row_num += 1

        self._auto_fit_columns(sheet)

    # ================= SHEET 3: CONVERGE SETTLED WITH HIGHLIGHTING =================
    def create_converge_settled_sheet(self, settled_rows: list):
        sheet = self.workbook.create_sheet("Converge Settled")

        headers = [
            "INVOICE NUMBER", "AMOUNT", "Transaction Status", "Original Transaction Type"
        ]
        sheet.append(headers)
        self._apply_header_style(sheet, 1, len(headers))

        row_num = 2
        for row in settled_rows:
            invoice = (row.get("Invoice Number") or "").strip()
            if not invoice:
                continue

            transaction_status = (row.get("Transaction Status") or "").strip()
            is_discrepancy     = transaction_status.upper() != "SETTLED"

            sheet.append([
                invoice,
                row.get("Original Amount"),
                transaction_status,
                row.get("Original Transaction Type")
            ])

            if is_discrepancy:
                self._apply_row_fill(sheet, row_num, len(headers), "FFC7CE")

            row_num += 1

        self._auto_fit_columns(sheet)

    # ================= SHEET 4: ORDERS SHIPPED =================
    def create_orders_shipped_sheet(
        self,
        asn_process_numbers: list,
        order_totals: list,
        converge_settled: dict
    ):
        """
        ASN process numbers come from fetch_asn_process_numbers DB query.
        Order totals come from fetch_order_totals DB query.
        Compares every ASN order against what settled in Converge.
        """
        sheet = self.workbook.create_sheet("Orders Shipped")

        headers = [
            "Order no.", "Sum of Total CXP", "MATCHING WITH CONVERGE",
            "Differences", "Order no", "Amount"
        ]
        sheet.append(headers)
        self._apply_header_style(sheet, 1, len(headers))

        if not asn_process_numbers:
            sheet.append(["No ASN orders found for this period."])
            self._auto_fit_columns(sheet)
            return

        # Build CXP order total map — order_totals is a list of
        # {process_number, order_total} dicts from fetch_order_totals
        order_total_map = {}
        for item in (order_totals or []):
            pnum  = item.get("process_number")
            total = item.get("order_total")
            if pnum and total is not None:
                order_total_map[str(pnum).strip()] = total

        settled_invoices = converge_settled.get("invoice_level", {})

        # Settled amount: use first SALE row (confirmed correct by user)
        settled_amount_map = {}
        for invoice, data in settled_invoices.items():
            for raw_row in data.get("raw_rows", []):
                if raw_row.get("transaction_type") == "SALE":
                    settled_amount_map[invoice] = raw_row.get("amount")
                    break

        row_num = 2
        for order_id in asn_process_numbers:
            oid_str         = str(order_id).strip()
            cxp_amount      = order_total_map.get(oid_str)
            converge_amount = settled_amount_map.get(oid_str)
            is_in_converge  = oid_str in settled_invoices
            matching_status = "YES" if is_in_converge else "NO"

            difference = ""
            if cxp_amount is not None and converge_amount is not None:
                try:
                    diff = abs(float(cxp_amount) - float(converge_amount))
                    difference = f"{diff:.2f}" if diff > 0.01 else "NO"
                except (TypeError, ValueError):
                    difference = "ERROR"
            elif not is_in_converge:
                difference = "NOT IN CONVERGE"

            sheet.append([
                oid_str,
                cxp_amount,
                matching_status,
                difference,
                oid_str,
                converge_amount if converge_amount is not None else "NA"
            ])

            if matching_status == "NO" or (difference and difference not in ("NO", "")):
                self._apply_row_fill(sheet, row_num, len(headers), "FFC7CE")
                if not is_in_converge:
                    try:
                        sheet.cell(row=row_num, column=3).comment = Comment(
                            "Order in ASN but not in Settled batch", "Reconciliation"
                        )
                    except Exception:
                        pass

            row_num += 1

        self._auto_fit_columns(sheet)

    # ================= SHEET 5: RECONCILIATION =================
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
        6-section reconciliation report. Uses explicit sheet.cell(row, col)
        throughout — never sheet.append() — so every header lands on exactly
        the right row with no drift.
        """
        sheet = self.workbook.create_sheet("Reconciliation", 0)
        r = 1   # single source of truth — equals actual sheet row

        converge_invoices = converge_current.get("invoices", {})
        settled_invoices  = converge_settled.get("invoice_level", {})
        asn_set           = set(asn_process_numbers)
        orders_dict       = classification["orders"]

        order_total_map = {}
        for item in (order_totals or []):
            pnum  = item.get("process_number")
            total = item.get("order_total")
            if pnum and total is not None:
                order_total_map[str(pnum).strip()] = total

        # ── Helpers ────────────────────────────────────────────────────────────
        def wr(row, values):
            for col, val in enumerate(values, start=1):
                sheet.cell(row=row, column=col).value = val

        def section_hdr(row, title):
            sheet.cell(row=row, column=1).value = title
            self._apply_header_style(sheet, row, 6)
            sheet.merge_cells(
                start_row=row, start_column=1,
                end_row=row,   end_column=6
            )
            return row + 1

        def sub_hdr(row, cols):
            wr(row, cols)
            self._apply_sub_header_style(sheet, row, len(cols))
            return row + 1

        def no_issue_row(row):
            sheet.cell(row=row, column=4).value = "No issue"
            return row + 1

        # ══════════════════════════════════════════════════════════════════════
        # SECTION 1 — Orders present in CXP but NOT in Converge
        # Only orders where order_state == "SUCCESS" but zero Converge presence.
        # WAITING_FOR_PAYMENT / PAYMENT_CANCELLED / ERROR are excluded.
        # Columns: Order # | Converge Status | CXP Status | Remarks
        # ══════════════════════════════════════════════════════════════════════
        r = section_hdr(r, "Orders present in CXP and not present in Converge")
        r = sub_hdr(r, ["Order #", "Converge Status", "CXP Status", "Remarks"])

        sec1_orders = [
            o for o in cxp_orders
            if o.get("order_state") == "SUCCESS"
            and o["process_number"] not in converge_invoices
            and o["process_number"] not in settled_invoices
        ]
        if sec1_orders:
            for order in sec1_orders:
                wr(r, [
                    order["process_number"],
                    "NA",
                    order.get("fulfillment_status", ""),
                    ""
                ])
                self._apply_row_fill(sheet, r, 4, "FFEB9C")
                r += 1
        else:
            r = no_issue_row(r)

        r += 2

        # ══════════════════════════════════════════════════════════════════════
        # SECTION 2 — Orders Shipped in CXP but amount different in Converge
        # Columns: Order # | Converge Amount | CXP Amount | Remarks
        # ══════════════════════════════════════════════════════════════════════
        r = section_hdr(r, "Orders Shipped in CXP but amount different in Converge")
        r = sub_hdr(r, ["Order #", "Converge Amount", "CXP Amount", "Remarks"])

        settled_amount_map = {}
        for invoice, data in settled_invoices.items():
            for raw_row in data.get("raw_rows", []):
                if raw_row.get("transaction_type") == "SALE":
                    settled_amount_map[invoice] = raw_row.get("amount")
                    break

        sec2_written = False
        for order in cxp_orders:
            oid = order["process_number"]
            if order.get("fulfillment_status") != "SHIPPED":
                continue
            cxp_amount      = order_total_map.get(oid)
            converge_amount = settled_amount_map.get(oid)
            if cxp_amount is not None and converge_amount is not None:
                try:
                    if abs(float(cxp_amount) - float(converge_amount)) > 0.01:
                        wr(r, [oid, converge_amount, cxp_amount, ""])
                        self._apply_row_fill(sheet, r, 4, "FFC7CE")
                        r += 1
                        sec2_written = True
                except (TypeError, ValueError):
                    pass
        if not sec2_written:
            r = no_issue_row(r)

        r += 2

        # ══════════════════════════════════════════════════════════════════════
        # SECTION 3 — Orders Shipped in CXP but NOT showing in ASN
        # Source of truth for "shipped": fulfillment_status == "SHIPPED"
        #   (from order_items DB query — query 2)
        # Source of truth for "ASN received": asn_process_numbers
        #   (from fetch_asn_process_numbers DB query — query 3)
        # SHIPPED ONLY — CLAIMED is intentionally excluded.
        # Columns: Order # | ASN Status | CXP Status | Remarks
        # ══════════════════════════════════════════════════════════════════════
        r = section_hdr(r, "Orders Claimed / Shipped in CXP but not showing in ASN")
        r = sub_hdr(r, ["Order #", "ASN Status", "CXP Status", "Remarks"])

        shipped_not_in_asn = [
            o for o in cxp_orders
            if o.get("fulfillment_status") == "SHIPPED"
            and o["process_number"] not in asn_set
        ]
        if shipped_not_in_asn:
            for order in shipped_not_in_asn:
                wr(r, [
                    order["process_number"],
                    "NA",
                    order.get("fulfillment_status", ""),
                    ""
                ])
                self._apply_row_fill(sheet, r, 4, "FFDCB2")
                r += 1
        else:
            r = no_issue_row(r)

        r += 2

        # ══════════════════════════════════════════════════════════════════════
        # SECTION 4 — Orders Shipped in CXP but Converge is NOT settled
        # Uses action_reason == "SHIPPED_NOT_SETTLED" from classifier.
        # Columns: Order # | CXP status | Converge Status | Remarks
        # ══════════════════════════════════════════════════════════════════════
        r = section_hdr(r, "Orders Shipped in CXP but converge is not settled")
        r = sub_hdr(r, ["Order #", "CXP status", "Converge Status", "Remarks"])

        sec4_written = False
        for order in cxp_orders:
            oid         = order["process_number"]
            order_class = orders_dict.get(oid, {})
            if order_class.get("action_reason") == "SHIPPED_NOT_SETTLED":
                wr(r, [
                    oid,
                    order_class.get("cxp_status", "NA"),
                    order_class.get("converge_status", "NA"),
                    ""
                ])
                self._apply_row_fill(sheet, r, 4, "FFC7CE")
                r += 1
                sec4_written = True
        if not sec4_written:
            r = no_issue_row(r)

        r += 3

        # ══════════════════════════════════════════════════════════════════════
        # SECTION 5 — Summary Statistics (5 lines)
        # ══════════════════════════════════════════════════════════════════════
        total_orders     = len(cxp_orders)
        successful_count = len(classification["successful_orders"])
        retry_count      = len(classification["retry_success_orders"])

        declined_count = sum(
            1 for oid, data in orders_dict.items()
            if data.get("state") == "FAILED"
            and "DECLINED" in data.get("converge_status", "")
        )
        cancelled_count = sum(
            1 for o in cxp_orders
            if o.get("order_state") == "PAYMENT_CANCELLED"
        )

        summary_data = [
            ("Total Number of orders submitted",                            total_orders),
            ("Number of orders placed successfully",                        successful_count),
            ("Number of orders declined due to credit card related issues", declined_count),
            ("Number of orders cancelled by users (user-initiated)",        cancelled_count),
            ("Number of orders placed successfully after multiple tries",   retry_count),
        ]

        for label, value in summary_data:
            sheet.cell(row=r, column=1).value = label
            sheet.cell(row=r, column=6).value = value
            sheet.cell(row=r, column=6).font  = Font(bold=True)
            r += 1

        r += 2

        # ══════════════════════════════════════════════════════════════════════
        # SECTION 6 — Failed Orders Detail
        # Columns: OrderNumber | Email | Phone Number | CXP Status |
        #          Converge Status | Reason for order failure
        # ══════════════════════════════════════════════════════════════════════
        r = section_hdr(r, "Order Details")
        r = sub_hdr(r, [
            "Order Number", "Email", "Phone Number",
            "CXP Status", "Converge Status", "Reason for order failure"
        ])

        failed_ids     = set(classification["failed_orders"])
        internal_users = {"amit.kumar@phasezeroventures.com"}

        for order in cxp_orders:
            oid   = order["process_number"]
            email = (order.get("notif_email") or "").strip().lower()
            if email in internal_users or oid not in failed_ids:
                continue

            oc        = orders_dict.get(oid, {})
            db_status = oc.get("cxp_db_status", "")
            conv_stat = oc.get("converge_status", "NA")

            if db_status == "PAYMENT_CANCELLED":
                reason = "user-initiated cancellation"
            elif "DECLINED" in conv_stat:
                reason = "card declined"
            elif conv_stat == "SUSPECTED FRAUD":
                reason = "suspected fraud"
            else:
                reason = ""

            wr(r, [
                oid,
                order.get("notif_email", ""),
                order.get("notify_mobile_no", ""),
                db_status,
                conv_stat,
                reason
            ])
            r += 1

        self._auto_fit_columns(sheet)

    # ================= SHEET 6: LOGS =================
    def create_logs_sheet(
        self,
        start_date: str,
        end_date: str,
        sales_orders: list,
        order_items: list,
        asn_process_numbers: list,
        order_totals: list,
        classification: dict,
        converge_current_result: dict,
        converge_settled_result: dict
    ):
        """
        All information that would go to the log file — run metadata,
        DB query counts, converge batch stats, full classification breakdown,
        action required order details, settlement mismatches, retry successes.
        """
        sheet = self.workbook.create_sheet("Logs")
        r     = 1

        orders_dict = classification.get("orders", {})
        stats       = classification.get("classification_stats", {})

        def wr(row, values):
            for col, val in enumerate(values, start=1):
                sheet.cell(row=row, column=col).value = val

        def hdr(row, title):
            sheet.cell(row=row, column=1).value = title
            fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
            font = Font(color="FFFFFF", bold=True)
            for col in range(1, 5):
                cell = sheet.cell(row=row, column=col)
                cell.fill = fill
                cell.font = font
            sheet.merge_cells(
                start_row=row, start_column=1,
                end_row=row,   end_column=4
            )
            sheet.cell(row=row, column=1).alignment = Alignment(horizontal="center")
            return row + 1

        def kv(row, key, value):
            sheet.cell(row=row, column=1).value = key
            sheet.cell(row=row, column=2).value = value
            sheet.cell(row=row, column=1).font  = Font(bold=True)
            return row + 1

        # ── A: Run Info ───────────────────────────────────────────────────────
        r = hdr(r, "Reconciliation Run Info")
        r = kv(r, "Start Date",      start_date)
        r = kv(r, "End Date",        end_date)
        r = kv(r, "Run Timestamp",   datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        r += 1

        # ── B: DB Query Counts ────────────────────────────────────────────────
        r = hdr(r, "Database Query Results")
        r = kv(r, "Sales Orders fetched",       len(sales_orders))
        r = kv(r, "Order Items fetched",         len(order_items))
        r = kv(r, "ASN Process Numbers fetched", len(asn_process_numbers))
        r = kv(r, "Order Totals fetched",        len(order_totals))

        orders_with_totals = sum(1 for i in order_totals if i.get("order_total") is not None)
        pct = f"{orders_with_totals / len(sales_orders) * 100:.1f}%" if sales_orders else "0%"
        r = kv(r, "Order Total Coverage", f"{orders_with_totals}/{len(sales_orders)} ({pct})")
        r += 1

        # ── C: Converge Batch Stats ───────────────────────────────────────────
        r = hdr(r, "Converge Batch Stats")
        cc = converge_current_result.get("stats", {})
        r = kv(r, "CURRENT — unique invoices",       cc.get("total_invoices", 0))
        r = kv(r, "CURRENT — data inconsistencies",  cc.get("data_inconsistencies", 0))
        cs = converge_settled_result.get("stats", {})
        r = kv(r, "SETTLED — unique invoices",       cs.get("total_invoices", 0))
        r = kv(r, "SETTLED — settled count",         cs.get("settled_count", 0))
        r = kv(r, "SETTLED — anomaly count",         cs.get("anomaly_count", 0))
        r = kv(r, "SETTLED — multiple SALE rows",    cs.get("multiple_sales_count", 0))
        r += 1

        # ── D: Classification Summary ─────────────────────────────────────────
        r = hdr(r, "Classification Summary")
        r = kv(r, "Total Orders",                          stats.get("total", 0))
        r = kv(r, "Success — Shipped + Settled",           stats.get("success_shipped_settled", 0))
        r = kv(r, "Success — Ordered/Claimed + Approved",  stats.get("success_ordered_approved", 0))
        r = kv(r, "Success — Data inconsistency (noted)",  stats.get("verification_needed", 0))
        r = kv(r, "Failed — Declined",                     stats.get("declined", 0))
        r = kv(r, "Failed — Fraud",                        stats.get("fraud", 0))
        r = kv(r, "Failed — Payment Cancelled",            stats.get("payment_cancelled", 0))
        r = kv(r, "Failed — Other",                        stats.get("failed_other", 0))
        r = kv(r, "Action Required — CXP Error State",     stats.get("cxp_error_state", 0))
        r = kv(r, "Action Required — ASN Not Settled",     stats.get("asn_not_settled", 0))
        r = kv(r, "Action Required — Shipped Not Settled", stats.get("shipped_not_settled", 0))
        r = kv(r, "Action Required — No Payment Data",     stats.get("no_payment_data", 0))
        r = kv(r, "Action Required — Payment Success Order Failed",
                  stats.get("payment_success_order_failed", 0))
        r = kv(r, "Action Required — Settlement Anomaly",  stats.get("settlement_anomaly", 0))
        r = kv(r, "Action Required — Amount Mismatch",     stats.get("settlement_amount_mismatch", 0))
        r = kv(r, "Retry Successes",                       len(classification.get("retry_success_orders", [])))
        r += 1

        # ── E: Action Required Detail ─────────────────────────────────────────
        action_ids = classification.get("action_required_orders", [])
        r = hdr(r, f"Action Required Orders ({len(action_ids)})")

        if action_ids:
            wr(r, ["Order ID", "Reason", "CXP DB Status", "CXP Status",
                   "Converge Status", "Settled", "ASN", "Order Total", "Settled Amount"])
            self._apply_sub_header_style(sheet, r, 9)
            r += 1

            reason_colours = {
                "CXP_ERROR_STATE":              "FFC7CE",
                "ASN_NOT_SETTLED":              "FFDCB2",
                "SHIPPED_NOT_SETTLED":          "FFDCB2",
                "NO_PAYMENT_DATA":              "FFEB9C",
                "PAYMENT_SUCCESS_ORDER_FAILED": "DAEEF3",
                "SETTLEMENT_ANOMALY":           "FFDCB2",
                "SETTLEMENT_AMOUNT_MISMATCH":   "FFC7CE",
            }

            for oid in action_ids:
                od = orders_dict.get(oid, {})
                wr(r, [
                    oid,
                    od.get("action_reason", ""),
                    od.get("cxp_db_status", ""),
                    od.get("cxp_status", ""),
                    od.get("converge_status", "NA"),
                    "Yes" if od.get("is_settled") else "No",
                    "Yes" if od.get("has_asn") else "No",
                    od.get("order_total", ""),
                    od.get("settled_amount", "")
                ])
                colour = reason_colours.get(od.get("action_reason", ""), "FFEB9C")
                self._apply_row_fill(sheet, r, 9, colour)
                r += 1
        else:
            sheet.cell(row=r, column=1).value = "✓ No orders require action"
            sheet.cell(row=r, column=1).font  = Font(bold=True, color="375623")
            r += 1

        r += 1

        # ── F: Settlement Mismatches ──────────────────────────────────────────
        mismatches = classification.get("settlement_amount_mismatches", [])
        r = hdr(r, f"Settlement Amount Mismatches ({len(mismatches)})")

        if mismatches:
            wr(r, ["Order ID", "CXP Order Total", "Converge Settled Amount", "Difference"])
            self._apply_sub_header_style(sheet, r, 4)
            r += 1
            for m in mismatches:
                diff_val = m["difference"]
                sign     = "+" if diff_val > 0 else ""
                wr(r, [
                    m["order_id"],
                    f"{float(m['order_total']):.2f}",
                    f"{float(m['settled_amount']):.2f}",
                    f"{sign}{diff_val:.2f}"
                ])
                self._apply_row_fill(sheet, r, 4, "FFC7CE")
                r += 1
        else:
            sheet.cell(row=r, column=1).value = "✓ No amount mismatches"
            sheet.cell(row=r, column=1).font  = Font(bold=True, color="375623")
            r += 1

        r += 1

        # ── G: Retry Successes ────────────────────────────────────────────────
        retry_ids = classification.get("retry_success_orders", [])
        r = hdr(r, f"Retry Successes ({len(retry_ids)})")

        if retry_ids:
            wr(r, ["Success Order ID", "Previous Failed Order ID", "Customer Email"])
            self._apply_sub_header_style(sheet, r, 3)
            r += 1
            for oid in retry_ids:
                od = orders_dict.get(oid, {})
                wr(r, [
                    oid,
                    od.get("previous_failed_attempt", ""),
                    od.get("email", "")
                ])
                self._apply_row_fill(sheet, r, 3, "C6EFCE")
                r += 1
        else:
            sheet.cell(row=r, column=1).value = "No retry successes"
            r += 1

        self._auto_fit_columns(sheet)

    # ================= HELPER METHODS =================
    def _apply_header_style(self, sheet, row, col_count):
        dark_blue_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
        white_font = Font(color="FFFFFF", bold=True)
        for col in range(1, col_count + 1):
            cell = sheet.cell(row=row, column=col)
            cell.fill = dark_blue_fill
            cell.font = white_font
            cell.alignment = Alignment(horizontal="center", vertical="center")

    def _apply_sub_header_style(self, sheet, row, col_count):
        light_blue_fill = PatternFill(start_color="B4C7E7", end_color="B4C7E7", fill_type="solid")
        bold_font = Font(bold=True)
        for col in range(1, col_count + 1):
            cell = sheet.cell(row=row, column=col)
            cell.fill = light_blue_fill
            cell.font = bold_font
            cell.alignment = Alignment(horizontal="center", vertical="center")

    def _apply_row_fill(self, sheet, row, col_count, color):
        fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
        for col in range(1, col_count + 1):
            sheet.cell(row=row, column=col).fill = fill

    def _add_section_header(self, sheet, row, title):
        """Legacy helper — not used inside reconciliation sheet."""
        sheet.cell(row=row, column=1).value = title
        self._apply_header_style(sheet, row, 6)
        sheet.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)
        return row + 1

    def _auto_fit_columns(self, sheet):
        for column in sheet.columns:
            max_length    = 0
            column_letter = None
            for cell in column:
                if isinstance(cell, MergedCell):
                    continue
                if column_letter is None:
                    column_letter = cell.column_letter
                try:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                except Exception:
                    pass
            if column_letter:
                sheet.column_dimensions[column_letter].width = min(max_length + 3, 55)

    # ================= SAVE =================
    def save(self, directory: str = "."):
        path = f"{directory}/{self.get_filename()}"
        self.workbook.save(path)
        return path

    def to_bytes(self) -> BytesIO:
        output = BytesIO()
        self.workbook.save(output)
        output.seek(0)
        return output