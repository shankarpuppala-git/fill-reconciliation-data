from contextlib import asynccontextmanager
from fastapi import FastAPI
from dotenv import load_dotenv

from app.common.logger import setup_logging
from app.db.db_client import init_pool, close_pool
from app.controller.reconciliation_controller import router

# Load env + logging
load_dotenv()
setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP
    init_pool()
    yield
    # SHUTDOWN
    close_pool()


app = FastAPI(
    title="Reconciliation Service",
    version="1.0.0",
    description="Service to reconcile DB orders with Converge batch files",
    lifespan=lifespan
)

# Register routes
app.include_router(router)


@app.get("/health")
def health_check():
    return {
        "status": "UP",
        "service": "reconciliation"
    }
