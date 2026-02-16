from app.db.db_client import get_db_connection


def fetch_all_dicts(sql: str, params: tuple | None = None) -> list[dict]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params or ())
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def fetch_sales_orders(start_date, end_date):

    sql = """
          SELECT process_number, \
                 notif_email, \
                 order_date, \
                 order_state, \
                 pso.notify_mobile_no, \
                 pso.payment_reference_no
          FROM pzv_aftermarket.pzv_sales_order pso
          WHERE created_on >= DATE %s + INTERVAL '6 hours'
            AND created_on \
              < DATE %s + INTERVAL '6 hours'
            AND process_number ILIKE 'CXCL%%'
          ORDER BY order_date DESC \
          """
    return fetch_all_dicts(sql, (start_date, end_date))


def fetch_order_items(start_date, end_date):
    """
    Fetch order items for the date range
    """
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
    return fetch_all_dicts(sql, (start_date, end_date))


def fetch_asn_process_numbers(start_date, end_date):
    """
    Fetch ASN process numbers for the date range
    """
    sql = """
          SELECT DISTINCT process_number
          FROM pzv_aftermarket.asn_request_log arl
          WHERE arl.created_on >= DATE %s + INTERVAL '6 hours'
            AND arl.created_on \
              < DATE %s + INTERVAL '6 hours' \
          """
    return fetch_all_dicts(sql, (start_date, end_date))


def fetch_order_totals(process_numbers):
    """
    Fetch order totals for given process numbers
    (No date filtering needed here)
    """
    if not process_numbers:
        return []

    placeholders = ",".join(["%s"] * len(process_numbers))
    sql = f"""
        SELECT
            process_number,
            order_total
        FROM pzv_aftermarket.pzv_sales_order pso
        WHERE process_number IN ({placeholders})
    """
    return fetch_all_dicts(sql, tuple(process_numbers))
