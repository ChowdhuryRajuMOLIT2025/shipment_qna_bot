# src/shipment_qna_bot/api/main.py

from fastapi import FastAPI

from shipment_qna_bot.api.routes_chat import \
    router as chat_router  # type: ignore
from shipment_qna_bot.logging.middleware_log import RequestLoggingMiddleware

app = FastAPI(title="MCS Shipment Chat Bot")

# loging middleware (trace_id, timing, basic request logs)
app.add_middleware(RequestLoggingMiddleware)

# routers as chat_router as rote via user intention hook
app.include_router(chat_router, prefix="/api")  # type: ignore
