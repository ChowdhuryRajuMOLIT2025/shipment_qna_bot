# src/shipment_qna_bot/api/main.py
#################
# call sign for running the app
# uv run uvicorn shipment_qna_bot.api.main:app --reload --host=127.0.0.1 --port=8000
# https://shipmentqnabot-dgh3cjgzdzbyc3f0.eastus2-01.azurewebsites.net/
#################
import os
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware

from shipment_qna_bot.api.routes_chat import \
    router as chat_router  # type: ignore
from shipment_qna_bot.logging.middleware_log import RequestLoggingMiddleware

app = FastAPI(title="MCS Shipment Chat Bot")
_APP_INSTANCE_ID = str(uuid.uuid4())
_APP_STARTED_AT = datetime.now(timezone.utc).isoformat()

# loging middleware (trace_id, timing, basic request logs)
app.add_middleware(RequestLoggingMiddleware)

# Session middleware for backend-driven persistence
from starlette.middleware.sessions import SessionMiddleware

app.add_middleware(SessionMiddleware, secret_key=_APP_INSTANCE_ID)

# Gzip compression reduces transfer time for large table/text responses without
# changing the JSON schema or response types. Clients transparently decompress.
if os.getenv("ENABLE_GZIP", "true").lower() == "true":
    app.add_middleware(
        GZipMiddleware,
        minimum_size=int(os.getenv("GZIP_MIN_SIZE", "1000")),
    )

# Mount static files
static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def read_root():
    # Serve index.html if it exists
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Shipment Q&A Bot API is running. Documentation at /docs"}


@app.get("/api/health")
async def health_check():
    return {
        "status": "ok",
        "instance_id": _APP_INSTANCE_ID,
        "started_at": _APP_STARTED_AT,
    }


# routers as chat_router as rote via user intention hook
app.include_router(chat_router)
