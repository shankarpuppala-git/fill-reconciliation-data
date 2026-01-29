from fastapi import FastAPI
from app.controller.reconciliation_controller import router

app = FastAPI(title="Reconciliation Service")

app.include_router(router)
