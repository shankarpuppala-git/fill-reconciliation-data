import logging
from collections import defaultdict

logger = logging.getLogger("classifier.orders")


def classify_orders(
        cxp_orders: list,
        converge_current: dict,
        converge_settled: dict
) -> dict:
    """
    Classifies orders into:
    - SUCCESS
    - FAILED
    - RISKY

    Adds risk_reason for RISKY orders.
    Detects retry success cases.

    PRIORITY ORDER:
    1. ERROR state (TOP PRIORITY)
    2. PAYMENT_CANCELLED
    3. Bank/fraud issues
    4. Shipped but not settled
    5. Payment without order
    6. Data inconsistencies
    7. Success cases
    8. Everything else = FAILED
    """

    logger.info("Starting order classification")

    orders_result = {}
    successful_orders = []
    failed_orders = set()
    action_required_orders = []
    retry_success_orders = []
    converge_data_inconsistencies = []

    customer_history = defaultdict(list)

    # Get invoice-level data
    converge_invoices = converge_current.get("invoices", {})
    settled_invoices = converge_settled.get("invoice_level", {})

    # ------------------------------------
    # First pass: classify each order
    # ------------------------------------
    for order in cxp_orders:
        order_id = order["process_number"]

        email = order.get("notif_email")
        phone = order.get("notify_mobile_no")

        cxp_status = order.get("fulfillment_status")
        cxp_db_status = order.get("order_state")

        # Get Converge data
        converge_info = converge_invoices.get(order_id, {})
        settled_info = settled_invoices.get(order_id, {})

        converge_status = converge_info.get("final_status", "NA")
        is_data_issue = converge_info.get("is_data_issue", False)
        is_settled = settled_info.get("settled", False)

        state = None
        risk_reason = None

        # ===== TOP PRIORITY: ERROR STATE =====
        if cxp_db_status == "ERROR":
            state = "RISKY"
            risk_reason = "CXP_ERROR_STATE"

        elif cxp_db_status == "SUCCESS" and converge_status == "NA" and not is_settled and cxp_status in  ("ORDERED","CLAIMED"):
            state = "RISKY"
            risk_reason="Order Placed into CXP but no Payment data available from converge"

        # ===== PAYMENT CANCELLED =====
        elif cxp_db_status == "PAYMENT_CANCELLED":
            state = "FAILED"

        # ===== RISKY – shipped but not settled =====
        elif cxp_status == "SHIPPED" and not is_settled:
            state = "RISKY"
            risk_reason = "SHIPPED_NOT_SETTLED"

        # ===== FAILED – bank / fraud =====
        elif converge_status in ("DECLINED", "SUSPECTED FRAUD") or "DECLINED" in converge_status:
            state = "FAILED"

        # ===== RISKY – payment success but order missing =====
        elif not cxp_status and converge_status == "APPROVAL":
            state = "RISKY"
            risk_reason = "PAYMENT_SUCCESS_PLACE_ORDER_FAILED"

        # ===== RISKY – converge inconsistency =====
        elif is_data_issue:
            state = "VERIFICATION_NEEDED"
            risk_reason = "CONVERGE_DATA_INCONSISTENCY"
            successful_orders.append(order_id)

        # ===== SUCCESS – shipped + settled =====
        elif cxp_status == "SHIPPED" and is_settled:
            state = "SUCCESS"

        # ===== SUCCESS – ordered / claimed + approval =====
        elif cxp_status in ("ORDERED", "CLAIMED") and converge_status == "APPROVAL":
            state = "SUCCESS"

        # ===== FAILED – everything else =====
        else:
            state = "FAILED"

        orders_result[order_id] = {
            "state": state,
            "risk_reason": risk_reason,
            "cxp_status": cxp_status,
            "cxp_db_status": cxp_db_status,
            "converge_status": converge_status,
            "is_settled": is_settled,
            "is_data_issue": is_data_issue,
            "is_retry_success": False
        }

        if state == "SUCCESS":
            successful_orders.append(order_id)
        elif state == "FAILED":
            failed_orders.add(order_id)
        elif state == "RISKY":
            action_required_orders.append(order_id)
        elif state == "VERIFICATION_NEEDED":
            converge_data_inconsistencies.append(order_id)

        customer_key = email or phone
        if customer_key:
            customer_history[customer_key].append((order_id, state))

        logger.info(
            "Order classified | order=%s | state=%s | classification_reason=%s | cxp_status=%s | cxp_db_status=%s | converge=%s | settled=%s",
            order_id,
            state,
            risk_reason,
            cxp_status,
            cxp_db_status,
            converge_status,
            is_settled
        )

    # ------------------------------------
    # Second pass: detect retry success
    # ------------------------------------
    for customer, attempts in customer_history.items():
        had_failure = any(state == "FAILED" for _, state in attempts)
        for order_id, state in attempts:
            if had_failure and state == "SUCCESS":
                orders_result[order_id]["is_retry_success"] = True
                retry_success_orders.append(order_id)

                logger.info(
                    "Retry success detected | customer=%s | order=%s",
                    customer,
                    order_id
                )
    if len(action_required_orders) > 0:
        for order_id in action_required_orders:
            order_info = orders_result.get(order_id)

            if not order_info:
                logger.warning(f"order {order_id} not found in orders_result")
                continue

            risk_reason = order_info.get("risk_reason", "Unknown reason")

            logger.warning(
                f"ATTENTION REQUIRED | order_id={order_id} | reason={risk_reason}"
            )
    else:
        logger.info("No  orders require your attention")

    logger.info("Order classification completed")


    return {
        "orders": orders_result,
        "successful_orders": successful_orders,
        "failed_orders": list(failed_orders),
        "action_required_orders": action_required_orders,
        "retry_success_orders": retry_success_orders,
        "converge_data_inconsistencies": converge_data_inconsistencies
    }
