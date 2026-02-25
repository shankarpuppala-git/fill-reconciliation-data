import logging
from typing import List, Optional

from app.service.reconciliation_parser import (
    resolve_converge_current,
    resolve_converge_settled
)
from app.service.order_classifier import classify_orders

logger = logging.getLogger("service.reconciliation")


class ReconciliationService:
    """
    Orchestrates the full reconciliation flow.

    Flow:
      1. Resolve Converge CURRENT (authorisation batch)
      2. Resolve Converge SETTLED (financial/settlement batch)
      3. Classify every CXP order → SUCCESS / FAILED / ACTION_REQUIRED
    """

    @staticmethod
    def run_reconciliation(
            cxp_orders: list,
            converge_current_rows: list,
            converge_settled_rows: list,
            order_totals: Optional[dict] = None,
            asn_process_numbers: Optional[List[str]] = None
    ) -> dict:
        """
        Main entry point for reconciliation.

        Returns:
        {
            "classification": {
                "orders": {...},
                "successful_orders": [...],
                "failed_orders": [...],
                "action_required_orders": [...],
                "retry_success_orders": [...],
                "converge_data_inconsistencies": [...],
                "settlement_amount_mismatches": [...],
                "classification_stats": {...}
            },
            "converge_current_result": {
                "invoices": {...},
                "summary": {...},
                "stats": {...}
            },
            "converge_settled_result": {
                "invoice_level": {...},
                "summary": {...},
                "stats": {...}
            }
        }
        """

        logger.info("===== Reconciliation process started =====")

        # ─────────────────────────────────────────────────────────────
        # STEP 1: Resolve Converge CURRENT (Authorisation)
        # ─────────────────────────────────────────────────────────────
        logger.info("Step 1: Resolving Converge CURRENT batch")

        converge_current_result = resolve_converge_current(converge_current_rows)

        converge_current   = converge_current_result.get("invoices", {})
        converge_current_summary = converge_current_result.get("summary")

        logger.info(
            "Converge CURRENT resolved | invoices=%s | summary_present=%s",
            len(converge_current),
            converge_current_summary is not None
        )

        # ─────────────────────────────────────────────────────────────
        # STEP 2: Resolve Converge SETTLED (Financial)
        # ─────────────────────────────────────────────────────────────
        logger.info("Step 2: Resolving Converge SETTLED batch")

        converge_settled_result = resolve_converge_settled(converge_settled_rows)

        invoice_level_settled = converge_settled_result.get("invoice_level", {})
        settled_summary       = converge_settled_result.get("summary", {})

        logger.info(
            "Converge SETTLED resolved | invoices=%s | summary_present=%s",
            len(invoice_level_settled),
            settled_summary is not None and settled_summary.get("sales_count") is not None
        )

        # ─────────────────────────────────────────────────────────────
        # STEP 3: Classify orders
        # ─────────────────────────────────────────────────────────────
        logger.info("Step 3: Classifying orders (ASN count=%s)", len(asn_process_numbers or []))

        classification_result = classify_orders(
            cxp_orders=cxp_orders,
            converge_current={"invoices": converge_current},
            converge_settled=converge_settled_result,
            order_totals=order_totals,
            asn_process_numbers=asn_process_numbers
        )

        logger.info(
            "Classification completed | total=%s | success=%s | failed=%s"
            " | action_required=%s | retry_success=%s",
            len(cxp_orders),
            len(classification_result["successful_orders"]),
            len(classification_result["failed_orders"]),
            len(classification_result["action_required_orders"]),
            len(classification_result["retry_success_orders"])
        )

        if classification_result["converge_data_inconsistencies"]:
            logger.info(
                "Converge data inconsistencies (treated as success): %s",
                len(classification_result["converge_data_inconsistencies"])
            )

        logger.info("===== Reconciliation process completed =====")

        return {
            "classification": classification_result,
            "converge_current_result": converge_current_result,
            "converge_settled_result": converge_settled_result
        }