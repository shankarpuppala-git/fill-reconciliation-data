from contextlib import contextmanager

from psycopg2.pool import SimpleConnectionPool
import os

_pool: SimpleConnectionPool | None = None

def init_pool():
    global _pool

    if _pool is not None:
        return

    required_envs = [
        "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"
    ]

    missing = [e for e in required_envs if not os.getenv(e)]
    if missing:
        raise RuntimeError(f"Missing DB env vars: {missing}")

    _pool = SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        connect_timeout=5,
    )


@contextmanager
def get_db_connection():
    if _pool is None:
        raise RuntimeError("DB pool not initialized")

    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)

def close_pool():
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None