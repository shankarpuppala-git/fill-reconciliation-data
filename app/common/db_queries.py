"""
Database query module with better error handling and logging.
"""
from typing import Optional, List, Dict
from app.db.db_client import get_db_connection
import logging

logger = logging.getLogger(__name__)


def fetch_all_dicts(sql: str, params: Optional[tuple] = None) -> List[Dict]:
    """
    Execute SQL query and return results as list of dictionaries.

    Args:
        sql: SQL query string
        params: Query parameters tuple

    Returns:
        List of dictionaries with query results

    Raises:
        Exception: If query execution fails
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        raise


def fetch_sales_orders(start_date: str, end_date: str) -> List[Dict]:
    """Fetch sales orders for date range."""
    sql = """
          SELECT process_number, \
                 notif_email, \
                 order_date, \
                 order_state, \
                 notify_mobile_no, \
                 payment_reference_no
          FROM pzv_aftermarket.pzv_sales_order pso
          WHERE created_on >= DATE %s + INTERVAL '6 hours'
            AND created_on \
              < DATE %s + INTERVAL '6 hours'
            AND process_number ILIKE 'CXCL%%'
          ORDER BY order_date DESC \
          """
    results = fetch_all_dicts(sql, (start_date, end_date))
    logger.info(f"Fetched {len(results)} sales orders for {start_date} to {end_date}")
    return results


def fetch_order_items(start_date: str, end_date: str) -> List[Dict]:
    """Fetch order items for date range."""
    sql = """
          SELECT order_process_number, \
                 order_status
          FROM pzv_aftermarket.pzv_sales_order_item
          WHERE order_process_number IN (SELECT process_number \
                                         FROM pzv_aftermarket.pzv_sales_order pso \
                                         WHERE created_on >= DATE %s + \
              INTERVAL '6 hours'
            AND created_on \
              < DATE %s + INTERVAL '6 hours'
            AND process_number ILIKE 'CXCL%%'
              ) \
          """
    results = fetch_all_dicts(sql, (start_date, end_date))
    logger.info(f"Fetched {len(results)} order items")
    return results


def fetch_asn_process_numbers(start_date: str, end_date: str) -> List[Dict]:
    """Fetch ASN process numbers for date range."""
    sql = """
          SELECT DISTINCT process_number
          FROM pzv_aftermarket.asn_request_log arl
          WHERE arl.created_on >= DATE %s + INTERVAL '6 hours'
            AND arl.created_on \
              < DATE %s + INTERVAL '6 hours' \
          """
    results = fetch_all_dicts(sql, (start_date, end_date))
    logger.info(f"Fetched {len(results)} ASN process numbers")
    return results


def fetch_order_totals(process_numbers: List[str]) -> List[Dict]:
    """Fetch order totals for specified process numbers."""
    if not process_numbers:
        logger.warning("No process numbers provided")
        return []

    placeholders = ",".join(["%s"] * len(process_numbers))
    sql = f"""
        SELECT
            process_number,
            order_total
        FROM pzv_aftermarket.pzv_sales_order pso
        WHERE process_number IN ({placeholders})
    """
    results = fetch_all_dicts(sql, tuple(process_numbers))
    logger.info(f"Fetched order totals for {len(results)}/{len(process_numbers)} orders")
    return results