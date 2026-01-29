from fastapi import FastAPI
from app.controller.reconciliation_controller import router
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Reconciliation Service")

app.include_router(router)
