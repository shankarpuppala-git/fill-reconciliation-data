import logging
from collections import defaultdict
from datetime import datetime, date
from typing import Optional, List

logger = logging.getLogger("classifier.orders")


def classify_orders(
        cxp_orders: list,
        converge_current: dict,
        converge_settled: dict,
        order_totals: Optional[dict] = None,
        asn_process_numbers: Optional[List[str]] = None
) -> dict:
    """
    Classifies orders into:
    - SUCCESS         : order completed correctly (includes VERIFICATION_NEEDED / data inconsistency)
    - FAILED          : order definitively failed (cancelled, declined, fraud)
    - ACTION_REQUIRED : requires manual intervention
        Reasons:
          CXP_ERROR_STATE            – order_state = ERROR in CXP
          NO_PAYMENT_DATA            – CXP success but no converge record at all
          ASN_NOT_SETTLED            – ASN received (physically shipped) but payment not settled
          SHIPPED_NOT_SETTLED        – CXP status SHIPPED but not settled (no ASN)
          SHIPPED_NOT_SETTLED        – CXP status SHIPPED but payment not yet settled
          PAYMENT_SUCCESS_ORDER_FAILED – converge approved but no CXP fulfillment
          SETTLEMENT_ANOMALY         – duplicate SALE rows or RETURN without SALE in settled batch
          SETTLEMENT_AMOUNT_MISMATCH – settled amount differs from CXP order total by > $0.01

    PRIORITY ORDER:
      1. CXP ERROR state
      2. No payment data
      3. Payment cancelled  → FAILED
      4. ASN received but not settled
      5. Shipped in CXP but Converge DECLINED/FRAUD
      6. Shipped in CXP but not settled (no ASN)
      7. Payment approved but order has no fulfillment status
      8. Settlement anomaly (duplicate SALE rows etc.)
      9. SUCCESS – Shipped + Settled  (with amount validation)
      10. SUCCESS – Ordered/Claimed + Approved (with amount validation)
      11. DECLINED / FRAUD  → FAILED
      12. Default → FAILED
    """

    logger.info("Starting order classification for %s orders", len(cxp_orders))

    asn_set = set(asn_process_numbers) if asn_process_numbers else set()

    orders_result = {}
    successful_orders = []
    rejected_orders = []
    failed_orders = set()
    action_required_orders = []
    retry_success_orders = []
    converge_data_inconsistencies = []
    settlement_amount_mismatches = []
    settled_no_asn_orders = []          # Settled in Converge but no ASN record — info only

    customer_history = defaultdict(list)

    # Get invoice-level data
    converge_invoices = converge_current.get("invoices", {})
    settled_invoices = converge_settled.get("invoice_level", {})

    # Normalise order_totals to dict {process_number: order_total}
    order_totals_map = {}
    if order_totals:
        if isinstance(order_totals, list):
            order_totals_map = {
                item["process_number"]: item.get("order_total")
                for item in order_totals
                if item.get("order_total") is not None
            }
        elif isinstance(order_totals, dict):
            order_totals_map = {k: v for k, v in order_totals.items() if v is not None}

    # Statistics tracking
    classification_stats = {
        "total": len(cxp_orders),
        "cxp_error_state": 0,
        "no_payment_data": 0,
        "payment_cancelled": 0,
        "asn_not_settled": 0,
        "shipped_not_settled": 0,
        "declined": 0,
        "fraud": 0,
        "payment_success_order_failed": 0,
        "settlement_anomaly": 0,
        "settlement_amount_mismatch": 0,
        "settled_no_asn": 0,
        "success_shipped_settled": 0,
        "success_ordered_approved": 0,
        "verification_needed": 0,
        "failed_other": 0,
        "order_rejected":0
    }

    # ──────────────────────────────────────────
    # First pass: classify each order
    # ──────────────────────────────────────────
    for order in cxp_orders:
        order_id       = order["process_number"]
        email          = order.get("notif_email")
        phone          = order.get("notify_mobile_no")
        order_date     = order.get("order_date")
        cxp_status     = order.get("fulfillment_status")   # SHIPPED / ORDERED / CLAIMED / None
        cxp_db_status  = order.get("order_state")          # SUCCESS / ERROR / PAYMENT_CANCELLED …

        # Converge data
        converge_info         = converge_invoices.get(order_id, {})
        settled_info          = settled_invoices.get(order_id, {})

        converge_status       = converge_info.get("final_status", "NA")
        is_data_issue         = converge_info.get("is_data_issue", False)
        is_settled            = settled_info.get("settled", False)
        settled_amount        = settled_info.get("net_amount")        # sale - returns
        has_settlement_anomaly = settled_info.get("has_anomaly", False)

        # ASN & order total
        has_asn     = order_id in asn_set
        order_total = order_totals_map.get(order_id)

        state       = None
        action_reason = None

        # ── PRIORITY 1: CXP ERROR STATE ──────────────────────────────
        if cxp_db_status == "ERROR":
            state = "ACTION_REQUIRED"
            action_reason = "CXP_ERROR_STATE"
            classification_stats["cxp_error_state"] += 1

        # ── PRIORITY 2: No payment data ───────────────────────────────
        elif (cxp_db_status == "SUCCESS"
              and converge_status == "NA"
              and not is_settled
              and cxp_status in ("ORDERED", "CLAIMED")):
            state = "ACTION_REQUIRED"
            action_reason = "NO_PAYMENT_DATA"
            classification_stats["no_payment_data"] += 1

        # ── PRIORITY 3: Payment cancelled → FAILED ────────────────────
        elif cxp_db_status == "PAYMENT_CANCELLED":
            state = "FAILED"
            classification_stats["payment_cancelled"] += 1

        # ── PRIORITY 4: ASN received but NOT settled ──────────────────
        #    Physical shipment went out — money MUST be settled
        elif has_asn and not is_settled:
            state = "ACTION_REQUIRED"
            action_reason = "ASN_NOT_SETTLED"
            classification_stats["asn_not_settled"] += 1

        # ── PRIORITY 5: CXP=SHIPPED but not settled ───────────────────
        # Note: we trust the settled flag over the auth message.
        # If is_settled=True the order will reach Priority 9 (SUCCESS).
        # DECLINED/FRAUD in the auth batch is a Converge data inconsistency
        # that is irrelevant once settlement is confirmed.
        elif cxp_status == "SHIPPED" and not is_settled:
            state = "ACTION_REQUIRED"
            action_reason = "SHIPPED_NOT_SETTLED"
            classification_stats["shipped_not_settled"] += 1

        elif cxp_status == "REJECTED" and converge_status == "APPROVAL":
            state="ORDER REJECTED"
            classification_stats["order_rejected"] += 1

        # ── PRIORITY 7: Payment approved but no CXP fulfillment status
        elif not cxp_status and converge_status == "APPROVAL":
            state = "ACTION_REQUIRED"
            action_reason = "PAYMENT_SUCCESS_ORDER_FAILED"
            classification_stats["payment_success_order_failed"] += 1

        # ── PRIORITY 8: Settlement anomaly (dup SALEs / RETURN w/o SALE)
        elif has_settlement_anomaly:
            state = "ACTION_REQUIRED"
            action_reason = "SETTLEMENT_ANOMALY"
            classification_stats["settlement_anomaly"] += 1

        # ── PRIORITY 9: SUCCESS – Shipped + Settled ───────────────────
        # Condition: CXP says SHIPPED *or* order is in ASN (authoritative
        # for physical shipment) AND payment is settled in Converge.
        # Using `has_asn` catches orders whose pzv_sales_order_item record
        # was not yet updated to SHIPPED but the warehouse already sent ASN.
        elif (cxp_status == "SHIPPED" or has_asn) and is_settled:
            state, action_reason = _validate_amounts(
                order_id, order_total, settled_amount,
                "success_shipped_settled", "success_shipped_settled",
                classification_stats, settlement_amount_mismatches, logger
            )

        # ── PRIORITY 10: SUCCESS – Ordered/Claimed + Approved ─────────
        elif cxp_status in ("ORDERED", "CLAIMED") and converge_status == "APPROVAL":
            if is_data_issue:
                # Converge data inconsistency — still SUCCESS but flag it
                state = "SUCCESS"
                action_reason = "CONVERGE_DATA_INCONSISTENCY"
                classification_stats["verification_needed"] += 1
                converge_data_inconsistencies.append(order_id)
            else:
                state, action_reason = _validate_amounts(
                    order_id, order_total, settled_amount,
                    "success_ordered_approved", "success_ordered_approved",
                    classification_stats, settlement_amount_mismatches, logger
                )

        # ── PRIORITY 11: DECLINED / FRAUD  → FAILED ──────────────────
        elif "DECLINED" in converge_status or converge_status == "SUSPECTED FRAUD":
            state = "FAILED"
            if "FRAUD" in converge_status:
                classification_stats["fraud"] += 1
            else:
                classification_stats["declined"] += 1

        # ── DEFAULT: everything else FAILED ───────────────────────────
        else:
            state = "FAILED"
            classification_stats["failed_other"] += 1

        # Detect vice-versa: settled in Converge but no ASN record.
        # If Converge collected payment AND cxp_status is SHIPPED but the
        # warehouse sent no ASN — we cannot confirm the order was physically
        # shipped. This is ACTION_REQUIRED with reason SETTLED_NO_ASN.
        # Note: this block runs AFTER the main priority chain, so it only
        # overrides if the order already reached SUCCESS (P9/P10).
        # If it was already ACTION_REQUIRED for another reason, keep that reason.
        settled_no_asn = (is_settled and not has_asn and cxp_status == "SHIPPED")
        if settled_no_asn and state == "SUCCESS":
            state = "ACTION_REQUIRED"
            action_reason = "SETTLED_NO_ASN"
            classification_stats["settled_no_asn"] = classification_stats.get("settled_no_asn", 0) + 1

        # Store result
        orders_result[order_id] = {
            "state": state,
            "action_reason": action_reason,
            "cxp_status": cxp_status,
            "cxp_db_status": cxp_db_status,
            "converge_status": converge_status,
            "is_settled": is_settled,
            "settled_amount": settled_amount,
            "order_total": order_total,
            "is_data_issue": is_data_issue,
            "has_asn": has_asn,
            "settled_no_asn": settled_no_asn,
            "is_retry_success": False,
            "order_date": order_date,
            "email": email,
            "phone": phone
        }

        if state == "SUCCESS":
            successful_orders.append(order_id)
        elif state == "FAILED":
            failed_orders.add(order_id)
        elif state == "ACTION_REQUIRED":
            action_required_orders.append(order_id)
        elif state == "REJECTES":
            rejected_orders.append(order_id)


        if settled_no_asn:
            settled_no_asn_orders.append(order_id)
            logger.warning(
                "⚠ Settled but NO ASN: %s — Converge collected payment but no "
                "warehouse ASN record. Verify the order was physically shipped.",
                order_id
            )
        # VERIFICATION_NEEDED is no longer a separate state —
        # converge data inconsistency goes to SUCCESS with action_reason tag

        # Track for retry detection
        customer_key = email or phone
        if customer_key and order_date:
            customer_history[customer_key].append({
                "order_id": order_id,
                "state": state,
                "order_date": order_date,
                "converge_status": converge_status
            })

    # ──────────────────────────────────────────
    # Second pass: retry detection
    # A "retry success" is: same customer, earlier FAILED or ACTION_REQUIRED
    # attempt followed by a SUCCESS attempt within 7 days.
    # ──────────────────────────────────────────
    for customer, attempts in customer_history.items():
        if len(attempts) < 2:
            continue

        try:
            sorted_attempts = sorted(
                attempts,
                key=lambda x: _to_datetime(x.get("order_date")) or datetime.min
            )
        except Exception:
            continue

        for i in range(len(sorted_attempts) - 1):
            current      = sorted_attempts[i]
            next_attempt = sorted_attempts[i + 1]

            # Previous must be FAILED or ACTION_REQUIRED; next must be SUCCESS
            if (current["state"] in ("FAILED", "ACTION_REQUIRED")
                    and next_attempt["state"] == "SUCCESS"):
                try:
                    cur_dt  = _to_datetime(current["order_date"])
                    next_dt = _to_datetime(next_attempt["order_date"])

                    if cur_dt and next_dt:
                        diff_days = (next_dt - cur_dt).days
                        if 0 <= diff_days <= 7:
                            oid = next_attempt["order_id"]
                            orders_result[oid]["is_retry_success"] = True
                            orders_result[oid]["previous_failed_attempt"] = current["order_id"]
                            retry_success_orders.append(oid)
                            logger.info(
                                "Retry success: Customer=%s | Failed=%s | Success=%s | Days=%s",
                                customer[:30], current["order_id"], oid, diff_days
                            )
                except Exception as e:
                    logger.debug("Error processing retry for customer %s: %s", customer[:20], e)

    # ──────────────────────────────────────────
    # Summary logging
    # ──────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("CLASSIFICATION COMPLETED")
    logger.info("=" * 60)
    logger.info("Total Orders:                  %s", classification_stats["total"])
    logger.info("Success:                       %s", len(successful_orders))
    logger.info("  - Shipped + Settled:         %s", classification_stats["success_shipped_settled"])
    logger.info("  - Ordered + Approved:        %s", classification_stats["success_ordered_approved"])
    logger.info("  - Verification (data issue): %s", classification_stats["verification_needed"])
    logger.info("Rejected orders:               %s",classification_stats["order_rejected"])
    logger.info("Failed:                        %s", len(failed_orders))
    logger.info("  - Declined:                  %s", classification_stats["declined"])
    logger.info("  - Fraud:                     %s", classification_stats["fraud"])
    logger.info("  - Payment Cancelled:         %s", classification_stats["payment_cancelled"])
    logger.info("  - Other:                     %s", classification_stats["failed_other"])
    logger.info("Action Required:               %s", len(action_required_orders))
    logger.info("  - CXP Error:                 %s", classification_stats["cxp_error_state"])
    logger.info("  - No Payment Data:           %s", classification_stats["no_payment_data"])
    logger.info("  - ASN Not Settled:           %s", classification_stats["asn_not_settled"])
    logger.info("  - Shipped Not Settled:       %s", classification_stats["shipped_not_settled"])
    logger.info("  - Payment Success Order Failed: %s", classification_stats["payment_success_order_failed"])
    logger.info("  - Settlement Anomaly:        %s", classification_stats["settlement_anomaly"])
    logger.info("  - Settlement Amount Mismatch:%s", classification_stats["settlement_amount_mismatch"])
    logger.info("Retry Successes:               %s", len(retry_success_orders))
    logger.info("=" * 60)

    if action_required_orders:
        reason_summary = defaultdict(list)
        for oid in action_required_orders:
            reason = orders_result[oid].get("action_reason", "UNKNOWN")
            reason_summary[reason].append(oid)

        logger.warning("⚠️  %s orders require attention:", len(action_required_orders))
        for reason, oids in sorted(reason_summary.items(), key=lambda x: -len(x[1])):
            logger.warning("  - %s: %s orders | [%s]", reason, len(oids), ", ".join(oids))

    if settlement_amount_mismatches:
        logger.warning("⚠️  %s SETTLEMENT AMOUNT MISMATCHES:", len(settlement_amount_mismatches))
        for m in settlement_amount_mismatches[:5]:
            logger.warning(
                "  - %s: Order=%.2f | Settled=%.2f | Diff=%.2f",
                m["order_id"], m["order_total"], m["settled_amount"], abs(m["difference"])
            )

    if settled_no_asn_orders:
        logger.warning(
            "⚠ %s orders settled in Converge but have NO ASN record — "
            "verify all were physically shipped: %s",
            len(settled_no_asn_orders),
            ", ".join(settled_no_asn_orders[:10])
        )

    return {
        "orders": orders_result,
        "successful_orders": successful_orders,
        "failed_orders": list(failed_orders),
        "action_required_orders": action_required_orders,
        "retry_success_orders": retry_success_orders,
        "converge_data_inconsistencies": converge_data_inconsistencies,
        "settlement_amount_mismatches": settlement_amount_mismatches,
        "settled_no_asn_orders": settled_no_asn_orders,
        "classification_stats": classification_stats,
        "rejected_orders": rejected_orders
    }


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _to_datetime(value) -> Optional[datetime]:
    """Safely coerce order_date to datetime regardless of input type."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value.split("+")[0].rstrip("Z"), fmt.rstrip("%z"))
            except ValueError:
                continue
    return None


def _validate_amounts(order_id, order_total, settled_amount,
                      success_stat_key, _unused,
                      classification_stats, settlement_amount_mismatches, log) -> tuple:
    """
    Compare settled_amount vs order_total.
    Returns (state, action_reason).
    """
    if order_total is not None and settled_amount is not None:
        try:
            diff = abs(float(settled_amount) - float(order_total))
            if diff > 0.01:
                classification_stats["settlement_amount_mismatch"] += 1
                settlement_amount_mismatches.append({
                    "order_id": order_id,
                    "order_total": float(order_total),
                    "settled_amount": float(settled_amount),
                    "difference": float(settled_amount) - float(order_total)
                })
                log.warning(
                    "Settlement mismatch: %s | Order=%.2f | Settled=%.2f | Diff=%.2f",
                    order_id, float(order_total), float(settled_amount), diff
                )
                return "ACTION_REQUIRED", "SETTLEMENT_AMOUNT_MISMATCH"
            else:
                classification_stats[success_stat_key] += 1
                return "SUCCESS", None
        except (ValueError, TypeError) as e:
            log.debug("Amount validation failed for %s: %s", order_id, e)
            classification_stats[success_stat_key] += 1
            return "SUCCESS", None
    else:
        classification_stats[success_stat_key] += 1
        return "SUCCESS", None