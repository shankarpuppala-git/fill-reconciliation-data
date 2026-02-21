"""
Improved reconciliation parser with proper validations and error handling.
"""
import logging
from collections import defaultdict
from typing import Dict, List, Any, Optional

logger = logging.getLogger("Parser")

# Constants
REQUIRED_CURRENT_COLUMNS = ["Invoice Number", "Auth Message"]
REQUIRED_SETTLED_COLUMNS = ["Invoice Number", "Original Transaction Type",
                            "Transaction Status", "Original Amount"]


class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass


def validate_csv_columns(rows: List[Dict[str, Any]],
                         required_columns: List[str],
                         batch_type: str) -> None:
    """Validate that required columns exist in CSV data."""
    if not rows:
        raise ValidationError(f"{batch_type} batch is empty")

    first_row = rows[0]
    missing_columns = [col for col in required_columns if col not in first_row]

    if missing_columns:
        available_columns = list(first_row.keys())
        raise ValidationError(
            f"{batch_type} batch missing required columns: {missing_columns}. "
            f"Available columns: {available_columns}"
        )
    logger.info(f"{batch_type} batch validation passed")


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float."""
    try:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            value = value.replace('$', '').replace(',', '').strip()
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Safely convert value to integer."""
    try:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            value = value.strip()
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_summary_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse summary row from Converge CSV."""
    sales_count = row.get("Sales Count", "").strip() if isinstance(row.get("Sales Count"), str) else row.get(
        "Sales Count")
    if not sales_count:
        return None

    return {
        "sales_count": _safe_int(sales_count),
        "total_sales": _safe_float(row.get("Total Sales")),
        "returns_count": _safe_int(row.get("Returns Count")),
        "total_returns": _safe_float(row.get("Total Returns")),
        "net_sales": _safe_float(row.get("Net Sales")),
        "others_count": _safe_int(row.get("Others Count")),
        "total_count": _safe_int(row.get("Total Count"))
    }


def resolve_converge_current(converge_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Resolves Converge CURRENT batch rows into one final status per invoice.

    Returns:
        {
            "invoices": {invoice_number: {...}},
            "summary": {...},
            "stats": {...}
        }
    """
    if not isinstance(converge_rows, list):
        raise ValidationError("Converge rows must be a list of dictionaries")

    logger.info(f"Starting Converge CURRENT resolution with {len(converge_rows)} rows")

    # Validate required columns
    validate_csv_columns(converge_rows, REQUIRED_CURRENT_COLUMNS, "Converge CURRENT")

    invoice_map = defaultdict(list)
    summary_row = None

    for row in converge_rows:
        invoice = (row.get("Invoice Number") or "").strip()

        # Check for summary row
        if not invoice:
            parsed_summary = _parse_summary_row(row)
            if parsed_summary:
                summary_row = parsed_summary
            continue

        invoice_map[invoice].append(row)

    logger.info(f"Found {len(invoice_map)} unique invoices")

    resolved = {}
    data_inconsistency_count = 0

    for invoice, rows in invoice_map.items():
        auth_messages = set()
        auth_sequence = []

        for r in rows:
            auth = (r.get("Auth Message") or "").strip().upper()
            if auth:
                auth_messages.add(auth)
                auth_sequence.append(auth)
            else:
                auth_sequence.append("NA")

        # Check for data inconsistencies
        is_data_issue = len(auth_messages) > 1
        if is_data_issue:
            data_inconsistency_count += 1

        # Determine final status with priority
        final_status = _determine_auth_status(auth_messages)

        resolved[invoice] = {
            "final_status": final_status,
            "is_data_issue": is_data_issue,
            "auth_messages": list(auth_messages),
            "auth_sequence": auth_sequence,
            "row_count": len(rows),
            "raw_rows": rows
        }

    if data_inconsistency_count > 0:
        logger.warning(f"{data_inconsistency_count} invoices have inconsistent auth messages")

    logger.info("Converge CURRENT resolution completed")

    return {
        "invoices": resolved,
        "summary": summary_row,
        "stats": {
            "total_invoices": len(resolved),
            "data_inconsistencies": data_inconsistency_count
        }
    }


def _determine_auth_status(auth_messages: set) -> str:
    """Determine final authorization status with priority."""
    if not auth_messages:
        return "NA"

    # Priority 1: Approval
    if "APPROVAL" in auth_messages:
        return "APPROVAL"

    # Priority 2: Specific decline reasons
    if "DECLINED:NSF" in auth_messages:
        return "DECLINED:NSF"
    if "DECLINED:CLOSED" in auth_messages:
        return "DECLINED:CLOSED"

    # Priority 3: Fraud
    if "SUSPECTED FRAUD" in auth_messages:
        return "SUSPECTED FRAUD"

    # Priority 4: Withdrawal limit
    if "WITHDRAWAL LIMIT" in auth_messages:
        return "WITHDRAWAL LIMIT"

    # Priority 5: Generic decline
    declined_msg = next((msg for msg in auth_messages if msg.startswith("DECLINED")), None)
    if declined_msg:
        return declined_msg

    return list(auth_messages)[0]


def resolve_converge_settled(settled_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Parses Converge SETTLED CSV rows with proper validation.

    Returns:
        {
            "invoice_level": {invoice_number: {...}},
            "summary": {...},
            "stats": {...}
        }
    """
    if not isinstance(settled_rows, list):
        raise ValidationError("Settled rows must be a list of dictionaries")

    logger.info(f"Starting Converge SETTLED resolution with {len(settled_rows)} rows")

    # Validate required columns
    validate_csv_columns(settled_rows, REQUIRED_SETTLED_COLUMNS, "Converge SETTLED")

    invoice_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    summary_row = None

    for row in settled_rows:
        invoice = (row.get("Invoice Number") or "").strip()

        # Check for summary row
        if not invoice:
            parsed_summary = _parse_summary_row(row)
            if parsed_summary:
                summary_row = parsed_summary
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

    logger.info(f"Found {len(invoice_map)} unique invoices")

    # Resolve invoice-level settlement with validation
    resolved_invoices: Dict[str, Dict[str, Any]] = {}
    anomaly_count = 0
    multiple_sales_count = 0

    for invoice, rows in invoice_map.items():
        sale_rows = [r for r in rows if r.get("transaction_type") == "SALE"]
        return_rows = [r for r in rows if r.get("transaction_type") == "RETURN"]
        other_rows = [r for r in rows if r.get("transaction_type") not in ("SALE", "RETURN", "")]

        sale_count = len(sale_rows)
        return_count = len(return_rows)
        settled = sale_count > 0

        # Detect anomalies
        has_anomaly = False
        anomaly_reasons = []

        if sale_count > 1:
            multiple_sales_count += 1
            has_anomaly = True
            anomaly_reasons.append(f"MULTIPLE_SALES:{sale_count}")

        if sale_count == 0 and return_count > 0:
            has_anomaly = True
            anomaly_reasons.append("RETURN_WITHOUT_SALE")

        if other_rows:
            has_anomaly = True
            other_types = {r.get("transaction_type") for r in other_rows}
            anomaly_reasons.append(f"NON_STANDARD_TYPES:{other_types}")

        if has_anomaly:
            anomaly_count += 1

        # Calculate amounts
        sale_amount = sum(r.get("amount") or 0 for r in sale_rows)
        return_amount = sum(r.get("amount") or 0 for r in return_rows)
        net_amount = sale_amount - return_amount

        resolved_invoices[invoice] = {
            "settled": settled,
            "sale_count": sale_count,
            "return_count": return_count,
            "other_count": len(other_rows),
            "sale_amount": sale_amount,
            "return_amount": return_amount,
            "net_amount": net_amount,
            "has_anomaly": has_anomaly,
            "anomaly_reasons": anomaly_reasons,
            "raw_rows": rows
        }

    if multiple_sales_count > 0:
        logger.warning(f"{multiple_sales_count} invoices have multiple SALE transactions")
    if anomaly_count > 0:
        logger.warning(f"{anomaly_count} invoices have anomalies requiring investigation")

    logger.info("Converge SETTLED resolution completed")

    return {
        "invoice_level": resolved_invoices,
        "summary": summary_row,
        "stats": {
            "total_invoices": len(resolved_invoices),
            "settled_count": sum(1 for inv in resolved_invoices.values() if inv["settled"]),
            "anomaly_count": anomaly_count,
            "multiple_sales_count": multiple_sales_count
        }
    }