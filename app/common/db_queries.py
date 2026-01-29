from app.db.db_client import get_connection


# ---------------- QUERY 1 ----------------
def fetch_sales_orders(business_date):
    sql = """
        SELECT
            process_number,
            notif_email,
            order_date,
            order_state,
            pso.notify_mobile_no,
            pso.payment_reference_no
        FROM pzv_aftermarket.pzv_sales_order pso
        WHERE created_on > DATE %s
          AND process_number ILIKE 'CXCL%%'
        ORDER BY order_date DESC
    """

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (business_date,))
            return cursor.fetchall()
    finally:
        conn.close()


# ---------------- QUERY 2 ----------------
def fetch_order_items(business_date):
    sql = """
        SELECT
            order_process_number,
            order_status
        FROM pzv_aftermarket.pzv_sales_order_item
        WHERE order_process_number IN (
            SELECT process_number
            FROM pzv_aftermarket.pzv_sales_order pso
            WHERE created_on > DATE %s
              AND process_number ILIKE 'CXCL%%'
        )
    """

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (business_date,))
            return cursor.fetchall()
    finally:
        conn.close()


# ---------------- QUERY 3 ----------------
def fetch_asn_process_numbers(business_date):
    sql = """
        SELECT DISTINCT
            process_number
        FROM pzv_aftermarket.asn_request_log arl
        WHERE arl.created_on > DATE %s
    """

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (business_date,))
            return cursor.fetchall()
    finally:
        conn.close()


# ---------------- QUERY 4 ----------------
def fetch_order_totals(process_numbers):
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

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, tuple(process_numbers))
            return cursor.fetchall()
    finally:
        conn.close()
