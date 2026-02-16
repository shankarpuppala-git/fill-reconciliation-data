from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI
from dotenv import load_dotenv

from app.common.logger import setup_logging
from app.db.db_client import init_pool, close_pool
from app.controller import reconciliation_controller


# Load env + logging FIRST
load_dotenv()
setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP
    init_pool()
    yield
    # SHUTDOWN
    close_pool()


# CREATE APP ONCE
app = FastAPI(
    title="Reconciliation Service",
    version="1.0.0",
    description="Service to reconcile DB orders with Converge batch files",
    lifespan=lifespan
)

# REGISTER ROUTERS AFTER APP CREATION
app.include_router(
    reconciliation_controller.router,
    prefix="/reconciliation",
    tags=["reconciliation"]
)



@app.get("/health")
def health_check():
    return {
        "status": "UP",
        "service": "reconciliation"
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)