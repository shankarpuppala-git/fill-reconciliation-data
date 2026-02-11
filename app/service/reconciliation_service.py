import logging

from app.service.reconciliation_parser import (
    resolve_converge_current,
    resolve_converge_settled
)
from app.service.order_classifier import classify_orders

logger = logging.getLogger("service.reconciliation")


class ReconciliationService:
    """
    Orchestrates the full reconciliation flow.

    Returns complete converge results for Excel generation.
    """

    @staticmethod
    def run_reconciliation(
            cxp_orders: list,
            converge_current_rows: list,
            converge_settled_rows: list
    ) -> dict:
        """
        Main entry point for reconciliation.

        Flow:
        1. Resolve Converge CURRENT
        2. Resolve Converge SETTLED
        3. Classify orders (SUCCESS / FAILED / RISKY)

        Returns:
        {
            "classification": {
                "orders": {...},
                "successful_orders": [...],
                "failed_orders": [...],
                "risky_orders": [...],
                "retry_success_orders": [...]
            },
            "converge_current_result": {
                "invoices": {...},
                "summary": {...}
            },
            "converge_settled_result": {
                "invoice_level": {...},
                "summary": {...}
            }
        }
        """

        logger.info("===== Reconciliation process started =====")

        # -------------------------------------------------
        # STEP 1: Resolve Converge CURRENT (Authorization)
        # -------------------------------------------------
        logger.info("Step 1: Resolving Converge CURRENT batch")

        converge_current_result = resolve_converge_current(converge_current_rows)

        converge_current = converge_current_result.get("invoices", {})
        converge_current_summary = converge_current_result.get("summary")

        logger.info(
            "Converge CURRENT resolved | invoices=%s | summary_present=%s",
            len(converge_current),
            converge_current_summary is not None
        )

        # -------------------------------------------------
        # STEP 2: Resolve Converge SETTLED (Financial)
        # -------------------------------------------------
        logger.info("Step 2: Resolving Converge SETTLED batch")

        converge_settled_result = resolve_converge_settled(converge_settled_rows)

        invoice_level_settled = converge_settled_result.get("invoice_level", {})
        settled_summary = converge_settled_result.get("summary", {})

        logger.info(
            "Converge SETTLED resolved | invoices=%s | summary_present=%s",
            len(invoice_level_settled),
            settled_summary.get("sales_count") is not None
        )

        # -------------------------------------------------
        # STEP 3: Classify Orders (Business Truth)
        # -------------------------------------------------
        logger.info("Step 3: Classifying orders")

        classification_result = classify_orders(
            cxp_orders=cxp_orders,
            converge_current={"invoices": converge_current},
            converge_settled=converge_settled_result
        )

        logger.info(
            "Classification completed | success=%s | failed=%s | risky=%s | retry_success=%s",
            len(classification_result["successful_orders"]),
            len(classification_result["failed_orders"]),
            len(classification_result["risky_orders"]),
            len(classification_result["retry_success_orders"])
        )

        logger.info("===== Reconciliation process completed =====")

        # Return complete results for Excel generation
        return {
            "classification": classification_result,
            "converge_current_result": converge_current_result,  # Complete structure
            "converge_settled_result": converge_settled_result  # Complete structure
        }
