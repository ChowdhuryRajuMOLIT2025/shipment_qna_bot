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

from shipment_qna_bot.api.routes_chat import \
    router as chat_router  # type: ignore
from shipment_qna_bot.logging.middleware_log import RequestLoggingMiddleware

app = FastAPI(title="MCS Shipment Chat Bot")

# Persistent session secret key from environment or fallback (Warning: fallback is insecure)
# In production, SHIPMENT_QNA_BOT_SESSION_SECRET must be set.
_SESSION_SECRET = os.getenv("SHIPMENT_QNA_BOT_SESSION_SECRET", str(uuid.uuid4()))
_APP_INSTANCE_ID = str(uuid.uuid4())
_APP_STARTED_AT = datetime.now(timezone.utc).isoformat()

# logging middleware (trace_id, timing, basic request logs)
app.add_middleware(RequestLoggingMiddleware)

# Security Headers Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src 'self' https://fonts.gstatic.com; "
            "img-src 'self' data:; "
            "media-src 'self' data:;"
        )
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains"
        )
        return response


app.add_middleware(SecurityHeadersMiddleware)

# CORS Middleware (Restrict as needed)
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten this in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# Session middleware for backend-driven persistence
from starlette.middleware.sessions import SessionMiddleware

app.add_middleware(SessionMiddleware, secret_key=_SESSION_SECRET)

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
