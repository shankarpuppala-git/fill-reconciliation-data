from fastapi import FastAPI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from app.controller.reconciliation_controller import router

app = FastAPI(
    title="Reconciliation Service",
    version="1.0.0",
    description="Service to reconcile DB orders with Converge batch files"
)

# Register routes
app.include_router(router)


# Optional: health check endpoint (very useful in UAT/Prod)
@app.get("/health")
def health_check():
    return {
        "status": "UP",
        "service": "reconciliation"
    }
