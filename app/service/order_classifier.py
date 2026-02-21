import logging
from collections import defaultdict
from datetime import datetime
from typing import Optional

logger = logging.getLogger("classifier.orders")


def classify_orders(
        cxp_orders: list,
        converge_current: dict,
        converge_settled: dict,
        order_totals: Optional[dict] = None  # ✅ NEW: For settlement amount validation
) -> dict:
    """
    Classifies orders into:
    - SUCCESS
    - FAILED
    - RISKY
    - VERIFICATION_NEEDED

    Adds risk_reason for RISKY orders.
    Detects retry success cases.

    PRIORITY ORDER:
    1. ERROR state (TOP PRIORITY)
    2. No payment data available
    3. PAYMENT_CANCELLED
    4. Shipped but not settled
    5. Bank/fraud issues
    6. Payment success but order failed
    7. Data inconsistencies
    8. Settlement amount mismatch (NEW)
    9. Success cases
    10. Everything else = FAILED
    """

    logger.info("Starting order classification for %s orders", len(cxp_orders))

    orders_result = {}
    successful_orders = []
    failed_orders = set()
    action_required_orders = []
    retry_success_orders = []
    converge_data_inconsistencies = []
    settlement_amount_mismatches = []  # ✅ NEW

    customer_history = defaultdict(list)

    # Get invoice-level data
    converge_invoices = converge_current.get("invoices", {})
    settled_invoices = converge_settled.get("invoice_level", {})

    # Build order totals map
    order_totals_map = {}
    if order_totals:
        if isinstance(order_totals, list):
            order_totals_map = {
                item['process_number']: item.get('order_total')
                for item in order_totals
                if item.get('order_total') is not None
            }
        elif isinstance(order_totals, dict):
            order_totals_map = {k: v for k, v in order_totals.items() if v is not None}

    # Statistics tracking
    classification_stats = {
        "total": len(cxp_orders),
        "cxp_error_state": 0,
        "no_payment_data": 0,
        "payment_cancelled": 0,
        "shipped_not_settled": 0,
        "declined": 0,
        "fraud": 0,
        "payment_success_order_failed": 0,
        "data_inconsistencies": 0,
        "settlement_amount_mismatch": 0,
        "success_shipped_settled": 0,
        "success_ordered_approved": 0,
        "failed_other": 0
    }

    # ------------------------------------
    # First pass: classify each order
    # ------------------------------------
    for order in cxp_orders:
        order_id = order["process_number"]
        email = order.get("notif_email")
        phone = order.get("notify_mobile_no")
        order_date = order.get("order_date")

        cxp_status = order.get("fulfillment_status")
        cxp_db_status = order.get("order_state")

        # Get Converge data
        converge_info = converge_invoices.get(order_id, {})
        settled_info = settled_invoices.get(order_id, {})

        converge_status = converge_info.get("final_status", "NA")
        is_data_issue = converge_info.get("is_data_issue", False)
        is_settled = settled_info.get("settled", False)
        settled_amount = settled_info.get("net_amount")  # ✅ NEW
        has_settlement_anomaly = settled_info.get("has_anomaly", False)  # ✅ NEW

        # Get order total
        order_total = order_totals_map.get(order_id)  # ✅ NEW

        state = None
        risk_reason = None

        # ===== PRIORITY 1: ERROR STATE =====
        if cxp_db_status == "ERROR":
            state = "RISKY"
            risk_reason = "CXP_ERROR_STATE"
            classification_stats["cxp_error_state"] += 1

        # ===== PRIORITY 2: No payment data =====
        elif cxp_db_status == "SUCCESS" and converge_status == "NA" and not is_settled and cxp_status in ("ORDERED", "CLAIMED"):
            state = "RISKY"
            risk_reason = "NO_PAYMENT_DATA"
            classification_stats["no_payment_data"] += 1

        # ===== PRIORITY 3: PAYMENT CANCELLED =====
        elif cxp_db_status == "PAYMENT_CANCELLED":
            state = "FAILED"
            classification_stats["payment_cancelled"] += 1

        # ===== PRIORITY 4: Shipped but not settled =====
        elif cxp_status == "SHIPPED" and not is_settled:
            state = "RISKY"
            risk_reason = "SHIPPED_NOT_SETTLED"
            classification_stats["shipped_not_settled"] += 1

        # ===== PRIORITY 5: Bank declined / fraud =====
        elif converge_status in ("DECLINED", "SUSPECTED FRAUD") or "DECLINED" in converge_status:
            state = "FAILED"
            if "FRAUD" in converge_status:
                classification_stats["fraud"] += 1
            else:
                classification_stats["declined"] += 1

        # ===== PRIORITY 6: Payment success but order failed =====
        elif not cxp_status and converge_status == "APPROVAL":
            state = "RISKY"
            risk_reason = "PAYMENT_SUCCESS_ORDER_FAILED"
            classification_stats["payment_success_order_failed"] += 1

        # ===== PRIORITY 7: Converge data inconsistency =====
        elif is_data_issue:
            state = "VERIFICATION_NEEDED"
            risk_reason = "CONVERGE_DATA_INCONSISTENCY"
            classification_stats["data_inconsistencies"] += 1
            converge_data_inconsistencies.append(order_id)

        # ✅ PRIORITY 8: Settlement anomaly
        elif has_settlement_anomaly:
            state = "RISKY"
            risk_reason = f"SETTLEMENT_ANOMALY"
            classification_stats["settlement_amount_mismatch"] += 1

        # ===== PRIORITY 9: SUCCESS – Shipped + Settled (WITH AMOUNT VALIDATION) =====
        elif cxp_status == "SHIPPED" and is_settled:
            # ✅ VALIDATE SETTLEMENT AMOUNT
            if order_total is not None and settled_amount is not None:
                try:
                    amount_diff = abs(float(settled_amount) - float(order_total))
                    if amount_diff > 0.01:  # More than 1 cent difference
                        state = "RISKY"
                        risk_reason = "SETTLEMENT_AMOUNT_MISMATCH"
                        classification_stats["settlement_amount_mismatch"] += 1
                        settlement_amount_mismatches.append({
                            "order_id": order_id,
                            "order_total": float(order_total),
                            "settled_amount": float(settled_amount),
                            "difference": float(settled_amount) - float(order_total)
                        })
                        logger.warning(
                            "Settlement mismatch: %s | Order=$%.2f | Settled=$%.2f | Diff=$%.2f",
                            order_id, float(order_total), float(settled_amount), amount_diff
                        )
                    else:
                        state = "SUCCESS"
                        classification_stats["success_shipped_settled"] += 1
                except (ValueError, TypeError) as e:
                    logger.debug("Amount validation failed for %s: %s", order_id, e)
                    state = "SUCCESS"
                    classification_stats["success_shipped_settled"] += 1
            else:
                state = "SUCCESS"
                classification_stats["success_shipped_settled"] += 1

        # ===== PRIORITY 10: SUCCESS – Ordered/Claimed + Approval (WITH AMOUNT VALIDATION) =====
        elif cxp_status in ("ORDERED", "CLAIMED") and converge_status == "APPROVAL":
            # ✅ CHECK AMOUNT IF SETTLED
            if is_settled and order_total is not None and settled_amount is not None:
                try:
                    amount_diff = abs(float(settled_amount) - float(order_total))
                    if amount_diff > 0.01:
                        state = "RISKY"
                        risk_reason = "SETTLEMENT_AMOUNT_MISMATCH"
                        classification_stats["settlement_amount_mismatch"] += 1
                        settlement_amount_mismatches.append({
                            "order_id": order_id,
                            "order_total": float(order_total),
                            "settled_amount": float(settled_amount),
                            "difference": float(settled_amount) - float(order_total)
                        })
                        logger.warning(
                            "Settlement mismatch: %s | Order=$%.2f | Settled=$%.2f | Diff=$%.2f",
                            order_id, float(order_total), float(settled_amount), amount_diff
                        )
                    else:
                        state = "SUCCESS"
                        classification_stats["success_ordered_approved"] += 1
                except (ValueError, TypeError) as e:
                    logger.debug("Amount validation failed for %s: %s", order_id, e)
                    state = "SUCCESS"
                    classification_stats["success_ordered_approved"] += 1
            else:
                state = "SUCCESS"
                classification_stats["success_ordered_approved"] += 1

        # ===== DEFAULT: Everything else = FAILED =====
        else:
            state = "FAILED"
            classification_stats["failed_other"] += 1

        # Store classification result
        orders_result[order_id] = {
            "state": state,
            "risk_reason": risk_reason,
            "cxp_status": cxp_status,
            "cxp_db_status": cxp_db_status,
            "converge_status": converge_status,
            "is_settled": is_settled,
            "settled_amount": settled_amount,  # ✅ NEW
            "order_total": order_total,  # ✅ NEW
            "is_data_issue": is_data_issue,
            "is_retry_success": False,
            "order_date": order_date
        }

        # Categorize
        if state == "SUCCESS":
            successful_orders.append(order_id)
        elif state == "FAILED":
            failed_orders.add(order_id)
        elif state == "RISKY":
            action_required_orders.append(order_id)
        elif state == "VERIFICATION_NEEDED":
            successful_orders.append(order_id)

        # Track for retry detection
        customer_key = email or phone
        if customer_key and order_date:
            customer_history[customer_key].append({
                "order_id": order_id,
                "state": state,
                "order_date": order_date,
                "converge_status": converge_status
            })

    # ------------------------------------
    # Second pass: ✅ IMPROVED retry detection
    # ------------------------------------
    for customer, attempts in customer_history.items():
        if len(attempts) < 2:
            continue

        # ✅ Sort by order date
        try:
            sorted_attempts = sorted(attempts, key=lambda x: x.get("order_date") or "")
        except (TypeError, KeyError):
            logger.debug("Could not sort attempts for customer %s", customer[:20])
            continue

        # ✅ Look for FAILED followed by SUCCESS pattern
        for i in range(len(sorted_attempts) - 1):
            current = sorted_attempts[i]
            next_attempt = sorted_attempts[i + 1]

            # Check if current failed and next succeeded
            if current["state"] == "FAILED" and next_attempt["state"] == "SUCCESS":
                # Verify it's within reasonable time window
                try:
                    current_date = current.get("order_date")
                    next_date = next_attempt.get("order_date")

                    if current_date and next_date:
                        # Parse dates if they're strings
                        if isinstance(current_date, str):
                            current_date = datetime.fromisoformat(current_date.replace('Z', '+00:00'))
                        if isinstance(next_date, str):
                            next_date = datetime.fromisoformat(next_date.replace('Z', '+00:00'))

                        # Check time difference (within 7 days)
                        if isinstance(current_date, datetime) and isinstance(next_date, datetime):
                            time_diff = (next_date - current_date).days

                            if 0 <= time_diff <= 7:
                                order_id = next_attempt["order_id"]
                                orders_result[order_id]["is_retry_success"] = True
                                orders_result[order_id]["previous_failed_attempt"] = current["order_id"]
                                retry_success_orders.append(order_id)

                                logger.info(
                                    "Retry success: Customer=%s | Failed=%s | Success=%s | Days=%s",
                                    customer[:30], current["order_id"], order_id, time_diff
                                )
                        else:
                            # If dates aren't datetime objects, still mark as retry but log warning
                            order_id = next_attempt["order_id"]
                            orders_result[order_id]["is_retry_success"] = True
                            retry_success_orders.append(order_id)
                            logger.debug("Retry detected but couldn't verify time window for %s", order_id)
                except Exception as e:
                    logger.debug("Error processing retry for customer %s: %s", customer[:20], e)
                    continue

    # ------------------------------------
    # ✅ SUMMARY LOGGING (instead of every order)
    # ------------------------------------
    logger.info("=" * 60)
    logger.info("CLASSIFICATION COMPLETED")
    logger.info("=" * 60)
    logger.info("Total Orders:          %s", classification_stats["total"])
    logger.info("Success:               %s", len(successful_orders))
    logger.info("  - Shipped+Settled:   %s", classification_stats["success_shipped_settled"])
    logger.info("  - Ordered+Approved:  %s", classification_stats["success_ordered_approved"])
    logger.info("Failed:                %s", len(failed_orders))
    logger.info("  - Declined:          %s", classification_stats["declined"])
    logger.info("  - Fraud:             %s", classification_stats["fraud"])
    logger.info("  - Payment Cancelled: %s", classification_stats["payment_cancelled"])
    logger.info("  - Other:             %s", classification_stats["failed_other"])
    logger.info("Risky (Action Req):    %s", len(action_required_orders))
    logger.info("  - CXP Error:         %s", classification_stats["cxp_error_state"])
    logger.info("  - No Payment Data:   %s", classification_stats["no_payment_data"])
    logger.info("  - Shipped Not Settled: %s", classification_stats["shipped_not_settled"])
    logger.info("  - Payment Success Order Failed: %s", classification_stats["payment_success_order_failed"])
    logger.info("  - Settlement Mismatch: %s", classification_stats["settlement_amount_mismatch"])
    logger.info("Verification Needed:   %s", len(converge_data_inconsistencies))
    logger.info("Retry Successes:       %s", len(retry_success_orders))
    logger.info("=" * 60)

    # ✅ Log action required items with grouped reasons
    # ✅ Log action required items with grouped reasons + order IDs
    if action_required_orders:
        risk_reason_summary = {}
        risk_reason_orders = {}

        for order_id in action_required_orders:
            reason = orders_result[order_id].get("risk_reason", "UNKNOWN")

            # count per reason
            risk_reason_summary[reason] = risk_reason_summary.get(reason, 0) + 1

            # collect order ids per reason
            risk_reason_orders.setdefault(reason, []).append(order_id)

        logger.warning("⚠️ %s orders require attention:", len(action_required_orders))

        for reason, count in sorted(
                risk_reason_summary.items(),
                key=lambda x: x[1],
                reverse=True
        ):
            order_ids = ", ".join(map(str, risk_reason_orders.get(reason, [])))
            logger.warning(
                "  - %s: %s orders | order_ids=[%s]",
                reason,
                count,
                order_ids
            )

    # ✅ Log settlement mismatches details
    if settlement_amount_mismatches:
        logger.warning("⚠️ %s SETTLEMENT AMOUNT MISMATCHES:", len(settlement_amount_mismatches))
        for mismatch in settlement_amount_mismatches[:5]:  # Show first 5
            logger.warning(
                "  - %s: Order=$%.2f vs Settled=$%.2f (Diff=$%.2f)",
                mismatch['order_id'],
                mismatch['order_total'],
                mismatch['settled_amount'],
                abs(mismatch['difference'])
            )
        if len(settlement_amount_mismatches) > 5:
            logger.warning("  ... and %s more", len(settlement_amount_mismatches) - 5)

    return {
        "orders": orders_result,
        "successful_orders": successful_orders,
        "failed_orders": list(failed_orders),
        "action_required_orders": action_required_orders,
        "retry_success_orders": retry_success_orders,
        "converge_data_inconsistencies": converge_data_inconsistencies,
        "settlement_amount_mismatches": settlement_amount_mismatches,  # ✅ NEW
        "classification_stats": classification_stats  # ✅ NEW
    }