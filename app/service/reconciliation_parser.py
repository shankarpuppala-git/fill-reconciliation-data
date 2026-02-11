import logging
from collections import defaultdict
from typing import Dict, List, Any

logger = logging.getLogger("CSV parser")


# =========================================================
# Converge CURRENT parser - FIXED COLUMN NAMES
# =========================================================
def resolve_converge_current(converge_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Resolves Converge CURRENT batch rows into one final status per invoice.

    FIXED: Now uses correct CSV column names:
    - "Invoice Number" (not "invoice")
    - "Auth Message" (not "auth_message")
    - "Customer Full Name" (not "customer")
    """

    if not isinstance(converge_rows, list):
        raise ValueError("Converge rows must be a list of dictionaries")

    logger.info("Starting Converge CURRENT resolution")
    logger.info("Total raw Converge rows received: %s", len(converge_rows))

    invoice_map = defaultdict(list)
    summary_row = None

    for row in converge_rows:
        # ✅ FIXED: Use correct column name
        invoice = (row.get("Invoice Number") or "").strip()

        # Check if this is a summary row (has totals but no invoice)
        if not invoice:
            sales_count = row.get("Sales Count", "").strip()
            if sales_count:  # This is a summary row
                summary_row = {
                    "sales_count": _safe_int(sales_count),
                    "total_sales": _safe_float(row.get("Total Sales")),
                    "returns_count": _safe_int(row.get("Returns Count")),
                    "total_returns": _safe_float(row.get("Total Returns")),
                    "net_sales": _safe_float(row.get("Net Sales")),
                    "others_count": _safe_int(row.get("Others Count")),
                    "total_count": _safe_int(row.get("Total Count"))
                }
                logger.info("Captured Converge CURRENT summary: %s", summary_row)
            continue

        invoice_map[invoice].append(row)

    logger.info("Total unique invoices found in Converge: %s", len(invoice_map))

    resolved = {}

    for invoice, rows in invoice_map.items():
        auth_messages = set()
        auth_sequence = []

        for r in rows:
            # ✅ FIXED: Use correct column name
            auth = (r.get("Auth Message") or "").strip().upper()
            if auth:
                auth_messages.add(auth)
                auth_sequence.append(auth)
            else:
                auth_sequence.append("NA")

        is_data_issue = len(auth_messages) > 1

        if is_data_issue:
            logger.warning(
                "Converge data inconsistency | invoice=%s | auth_messages=%s",
                invoice,
                list(auth_messages)
            )

        # Determine final status
        if "APPROVAL" in auth_messages:
            final_status = "APPROVAL"
        elif "SUSPECTED FRAUD" in auth_messages:
            final_status = "SUSPECTED FRAUD"
        elif "DECLINED:NSF" in auth_messages:
            final_status="DECLINED:NSF"
        elif "WITHDRAWAL LIMIT" in auth_messages:
            final_status = "WITHDRAWAL LIMIT"
        elif "DECLINED:CLOSED" in auth_messages:
            final_status="DECLINED:CLOSED"
        elif any("DECLINED" in msg for msg in auth_messages):
            final_status = "DECLINED"
        else:
            final_status = "NA"

        resolved[invoice] = {
            "final_status": final_status,
            "is_data_issue": is_data_issue,
            "auth_messages": list(auth_messages),
            "auth_sequence": auth_sequence,
            "raw_rows": rows
        }

        logger.info(
            "Resolved Converge invoice | invoice=%s | final_status=%s | data_issue=%s",
            invoice,
            final_status,
            is_data_issue
        )

    logger.info("Completed Converge CURRENT resolution")

    return {
        "invoices": resolved,
        "summary": summary_row
    }


# =========================================================
# Converge SETTLED parser - FIXED
# =========================================================
def resolve_converge_settled(settled_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Parses Converge SETTLED CSV rows.

    FIXED: Captures summary totals properly.
    """

    if not isinstance(settled_rows, list):
        raise ValueError("Settled rows must be a list of dictionaries")

    logger.info("Starting Converge SETTLED batch parsing")
    logger.info("Total settled rows received: %s", len(settled_rows))

    invoice_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    summary_row = None

    for row in settled_rows:
        # ✅ Use correct column name
        invoice = (row.get("Invoice Number") or "").strip()

        # Check if this is a summary row
        if not invoice:
            sales_count = row.get("Sales Count", "").strip()
            if sales_count:
                summary_row = {
                    "sales_count": _safe_int(sales_count),
                    "total_sales": _safe_float(row.get("Total Sales")),
                    "returns_count": _safe_int(row.get("Returns Count")),
                    "total_returns": _safe_float(row.get("Total Returns")),
                    "net_sales": _safe_float(row.get("Net Sales")),
                    "others_count": _safe_int(row.get("Others Count")),
                    "total_count": _safe_int(row.get("Total Count"))
                }
                logger.info("Captured Converge SETTLED summary: %s", summary_row)
            continue

        # Normalize the row
        normalized_row = {
            "invoice": invoice,
            "transaction_type": (row.get("Original Transaction Type") or "").strip().upper(),
            "transaction_status": (row.get("Transaction Status") or "").strip().upper(),
            "amount": _safe_float(row.get("Original Amount")),
            "_raw": row
        }

        invoice_map[invoice].append(normalized_row)

    logger.info(
        "Invoice rows parsed=%s | Summary present=%s",
        len(invoice_map),
        summary_row is not None
    )

    # Resolve invoice-level settlement
    resolved_invoices: Dict[str, Dict[str, Any]] = {}

    for invoice, rows in invoice_map.items():
        sale_count = 0
        return_count = 0
        other_types = set()

        for r in rows:
            txn_type = r.get("transaction_type")

            if txn_type == "SALE":
                sale_count += 1
            elif txn_type == "RETURN":
                return_count += 1
            else:
                if txn_type:
                    other_types.add(txn_type)

        settled = sale_count > 0

        if sale_count > 1:
            logger.warning(
                "Multiple SALE rows found for invoice=%s | sale_count=%s",
                invoice,
                sale_count
            )

        if other_types:
            logger.info(
                "Non-standard transaction types for invoice=%s | types=%s",
                invoice,
                list(other_types)
            )

        resolved_invoices[invoice] = {
            "settled": settled,
            "sale_count": sale_count,
            "return_count": return_count,
            "raw_rows": rows
        }

        logger.info(
            "Resolved settled status | invoice=%s | settled=%s | sales=%s | returns=%s",
            invoice,
            settled,
            sale_count,
            return_count
        )

    logger.info("Completed Converge SETTLED batch parsing")

    return {
        "invoice_level": resolved_invoices,
        "summary": summary_row or {
            "sales_count": None,
            "total_sales": None,
            "returns_count": None,
            "total_returns": None,
            "net_sales": None,
            "total_count": None
        }
    }


# =========================================================
# Helpers
# =========================================================
def _safe_float(value):
    try:
        if value is None or value == "":
            return None
        # Remove $ and commas
        if isinstance(value, str):
            value = value.replace('$', '').replace(',', '').strip()
        return float(value)
    except Exception:
        return None


def _safe_int(value):
    try:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            value = value.strip()
        return int(value)
    except Exception:
        return None
